"""
Модуль диспетчеризации и оркестрации параллельных расчётов рисков (backend/core/orchestrator.py).

Реализует центральный класс `Orchestrator`, который координирует жизненный цикл
всех пользовательских и регламентных запусков под единым `asyncio.Lock` (NFR-01, NFR-03):
1. Предварительная проверка конфликтов без побочных эффектов (`dry_run`, FR-15).
2. Дедупликация идентичных заявок по паре `(process_id, target_date)` с подпиской
   инициатора в массив `subscribers` без создания дублирующего воркера (FR-05).
3. Шлюз приоритетов «Люди vs Машина»: регламентные задачи (`MACHINE`) уступают
   пользовательским (`USER`) и ожидают в статусе `WAITING_TIMER` (FR-01..FR-03).
4. Проверка совместимости композитных ключей (`resource_id`, `date_from..date_to`, `access_mode`)
   через `lock_manager` и постановка конфликтующих задач в FIFO-очередь `QUEUED`
   без удержания открытых транзакций БД (FR-06..FR-09).
5. Фоновый таймер регламентных задач (`_machine_timer_loop`) и Watchdog контроля
   просроченных TTL-блокировок (`_watchdog_loop`, NFR-02).
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional
from uuid import UUID

import asyncpg

from backend.core import lock_manager
from backend.core.registry import ProcessRegistry, ResourceLockSpec
from backend.db import database
from backend.db.models import (
    ConfigUpdateRequest,
    DryRunResponse,
    LockInfo,
    RunCreateRequest,
    RunInfo,
    SystemStateSnapshot,
)

logger = logging.getLogger(__name__)


class Orchestrator:
    """Главный диспетчер процессов оценки рисков."""

    def __init__(
        self,
        worker_callback: Optional[Callable[[UUID], Awaitable[None]]] = None,
        ws_manager: Any = None,
        config_path: str = "config/processes.json",
    ) -> None:
        """
        Инициализирует диспетчер, реестр процессов и параметры из конфигурации.

        :param worker_callback: Асинхронная корутина воркера, запускаемая при переходе задачи в RUNNING.
        :param ws_manager: Менеджер WebSocket-подключений для мгновенной рассылки обновлений UI.
        :param config_path: Путь к JSON-файлу конфигурации бизнес-процессов.
        """
        self._worker_callback = worker_callback
        self._ws_manager = ws_manager
        self._config_path = Path(config_path)

        # Единая асинхронная блокировка гарантирует отсутствие гонок при принятии решений (NFR-01, NFR-03)
        self._lock = asyncio.Lock()

        # Реестр процессов для расчёта скользящих окон и карт доступа к ресурсам
        self._registry = ProcessRegistry(str(self._config_path))

        # Загружаем глобальные тайминги из конфигурационного файла
        self._config: dict[str, Any] = self._load_config()
        self._machine_timer_interval_sec: int = int(
            self._config.get("machine_timer_interval_sec", 15)
        )
        self._lock_ttl_sec: int = int(self._config.get("lock_ttl_sec", 25))
        self._heartbeat_interval_sec: int = int(
            self._config.get("heartbeat_interval_sec", 3)
        )

        # Список фоновых корутин (таймер машины и Watchdog)
        self._bg_tasks: list[asyncio.Task[Any]] = []

    def _load_config(self) -> dict[str, Any]:
        """Считывает базовые настройки оркестратора из config/processes.json."""
        if not self._config_path.exists():
            return {
                "machine_timer_interval_sec": 15,
                "lock_ttl_sec": 25,
                "heartbeat_interval_sec": 3,
                "processes": {},
            }
        with self._config_path.open("r", encoding="utf-8") as file:
            return json.load(file)

    def _get_process_meta(self, process_id: str) -> dict[str, Any]:
        """Возвращает метаданные процесса из конфига либо выбрасывает ValueError."""
        processes = self._config.get("processes", {})
        if process_id not in processes:
            raise ValueError(f"Неизвестный идентификатор процесса: {process_id}")
        return processes[process_id]

    @staticmethod
    def _row_to_run_info(row: asyncpg.Record | dict[str, Any]) -> RunInfo:
        """Преобразует строку из таблицы `runs` в Pydantic-модель `RunInfo`."""
        data = dict(row)
        raw_subs = data.get("subscribers")
        if isinstance(raw_subs, str):
            data["subscribers"] = json.loads(raw_subs)
        elif raw_subs is None:
            data["subscribers"] = []
        return RunInfo(**data)

    async def _broadcast_state_safe(self) -> None:
        """Безопасно отправляет актуальный снимок состояния всем подключённым WebSocket-клиентам."""
        if self._ws_manager is None:
            return
        try:
            snapshot = await self.get_state_snapshot()
            await self._ws_manager.broadcast_state(snapshot.model_dump(mode="json"))
        except Exception as exc:
            logger.warning("Ошибка при рассылке состояния по WebSocket: %s", exc)

    # -------------------------------------------------------------------------
    # 1. Предпроверка конфликтов без создания задачи (Dry-Run, FR-15)
    # -------------------------------------------------------------------------

    async def dry_run(self, request: RunCreateRequest) -> DryRunResponse:
        """
        Выполняет симуляцию диспетчеризации без записи в БД и захвата блокировок.

        Позволяет аналитику заранее увидеть, запустится ли процесс сразу,
        подключится к дублю, встанет в очередь из-за конфликта или уступит пользователям.
        """
        self._get_process_meta(request.process_id)
        lock_specs: list[ResourceLockSpec] = self._registry.get_required_locks(
            request.process_id, request.target_date
        )
        date_from = lock_specs[0].date_from if lock_specs else request.target_date
        date_to = lock_specs[0].date_to if lock_specs else request.target_date

        async with self._lock:
            pool = await database.get_pool()
            async with pool.acquire() as conn:
                # Шаг 1: Проверка на дубликат среди активных и ждущих задач (FR-05)
                dup_row = await conn.fetchrow(
                    """
                    SELECT run_id, status, user_name
                    FROM runs
                    WHERE process_id = $1
                      AND target_date = $2
                      AND status IN ('WAITING_TIMER', 'QUEUED', 'RUNNING')
                    ORDER BY created_at ASC
                    LIMIT 1
                    """,
                    request.process_id,
                    request.target_date,
                )
                if dup_row:
                    short_id = str(dup_row["run_id"])[:8]
                    return DryRunResponse(
                        prediction="DEDUPLICATE",
                        date_from=date_from,
                        date_to=date_to,
                        reason=(
                            f"Расчёт уже активен или ожидает (#{short_id}, статус {dup_row['status']}). "
                            f"Запрос будет присоединён к существующему запуску без дублирования нагрузки."
                        ),
                    )

                # Шаг 2: Шлюз приоритета «Люди vs Машина» (FR-03)
                if request.initiator == "MACHINE":
                    user_count = await conn.fetchval(
                        """
                        SELECT COUNT(*)
                        FROM runs
                        WHERE initiator = 'USER'
                          AND status IN ('RUNNING', 'QUEUED')
                        """
                    )
                    if user_count and user_count > 0:
                        return DryRunResponse(
                            prediction="WAIT_TIMER",
                            date_from=date_from,
                            date_to=date_to,
                            reason=(
                                f"В системе активны или ожидают {user_count} пользовательских расчётов. "
                                f"Регламентный запуск будет отложен по таймеру."
                            ),
                        )

                # Шаг 3: Проверка пересечений по композитному ключу (Resource + Date_Range + Mode)
                conflicts = await lock_manager.check_conflicts(conn, lock_specs)
                if conflicts:
                    first = conflicts[0]
                    owner_short = str(first["run_id"])[:8]
                    return DryRunResponse(
                        prediction="QUEUE",
                        date_from=date_from,
                        date_to=date_to,
                        reason=(
                            f"Конфликт {first['req_mode']}/{first['act_mode']} по ресурсу "
                            f"{first['resource_id']} ({first['date_from']}..{first['date_to']}) "
                            f"с запуском #{owner_short}"
                        ),
                    )

                # Шаг 4: Конфликтов не обнаружено — расчёт стартует мгновенно
                return DryRunResponse(
                    prediction="RUN_IMMEDIATELY",
                    date_from=date_from,
                    date_to=date_to,
                    reason="Все целевые ресурсы и диапазоны дат свободны. Запуск начнётся немедленно.",
                )

    # -------------------------------------------------------------------------
    # 2. Приём и маршрутизация заявки (submit_run, FR-01..FR-09)
    # -------------------------------------------------------------------------

    async def submit_run(self, request: RunCreateRequest) -> dict[str, Any]:
        """
        Регистрирует новую заявку на расчёт или присоединяет пользователя к существующему дублю.

        Все проверки и захват блокировок выполняются атомарно под `self._lock`.
        """
        proc_meta = self._get_process_meta(request.process_id)
        process_name: str = proc_meta.get("name", request.process_id)
        lock_specs: list[ResourceLockSpec] = self._registry.get_required_locks(
            request.process_id, request.target_date
        )
        date_from = lock_specs[0].date_from if lock_specs else request.target_date
        date_to = lock_specs[0].date_to if lock_specs else request.target_date

        run_to_start: Optional[UUID] = None
        result_payload: dict[str, Any]

        async with self._lock:
            pool = await database.get_pool()
            async with pool.acquire() as conn:
                # 1. Дедупликация: ищем идентичный процесс на ту же дату в активных/ждущих статусах
                dup_row = await conn.fetchrow(
                    """
                    SELECT *
                    FROM runs
                    WHERE process_id = $1
                      AND target_date = $2
                      AND status IN ('WAITING_TIMER', 'QUEUED', 'RUNNING')
                    ORDER BY created_at ASC
                    LIMIT 1
                    """,
                    request.process_id,
                    request.target_date,
                )

                if dup_row:
                    existing_id: UUID = dup_row["run_id"]
                    subscribers: list[str] = (
                        json.loads(dup_row["subscribers"])
                        if isinstance(dup_row["subscribers"], str)
                        else list(dup_row["subscribers"] or [])
                    )
                    # Добавляем инициатора в список подписчиков, если его там ещё нет
                    if request.user_name not in subscribers:
                        subscribers.append(request.user_name)

                    updated_row = await conn.fetchrow(
                        """
                        UPDATE runs
                        SET subscribers = $1::jsonb
                        WHERE run_id = $2
                        RETURNING *
                        """,
                        json.dumps(subscribers, ensure_ascii=False),
                        existing_id,
                    )
                    await database.log_audit_event(
                        run_id=existing_id,
                        event_type="DEDUPLICATED",
                        message=(
                            f"Заявка от {request.user_name} ({request.initiator}) присоединена к "
                            f"активному запуску #{str(existing_id)[:8]}"
                        ),
                        details={
                            "subscriber": request.user_name,
                            "initiator": request.initiator,
                            "process_id": request.process_id,
                            "target_date": str(request.target_date),
                        },
                    )
                    result_payload = {
                        "action": "ATTACHED",
                        "run": self._row_to_run_info(updated_row),
                    }
                else:
                    # 2. Создаём новый экземпляр запуска
                    run_id = uuid.uuid4()
                    status = "RUNNING"
                    blocked_reason: Optional[str] = None
                    lock_token: Optional[UUID] = None
                    started_at: Optional[datetime] = None

                    # Проверяем приоритет пользователей перед регламентной машиной (FR-03)
                    if request.initiator == "MACHINE":
                        user_active_count = await conn.fetchval(
                            """
                            SELECT COUNT(*)
                            FROM runs
                            WHERE initiator = 'USER'
                              AND status IN ('RUNNING', 'QUEUED')
                            """
                        )
                        if user_active_count and user_active_count > 0:
                            status = "WAITING_TIMER"
                            blocked_reason = f"Ожидание завершения пользовательских задач ({user_active_count} в работе)"

                    # Если задача не отложена по таймеру машины — проверяем конфликты ресурсов
                    if status != "WAITING_TIMER":
                        conflicts = await lock_manager.check_conflicts(conn, lock_specs)
                        if conflicts:
                            # По FR-08 ставим в очередь в памяти/БД без открытия рабочих транзакций
                            status = "QUEUED"
                            first = conflicts[0]
                            owner_short = str(first["run_id"])[:8]
                            blocked_reason = (
                                f"Конфликт {first['req_mode']}/{first['act_mode']} по {first['resource_id']} "
                                f"({first['date_from']}..{first['date_to']}) с #{owner_short}"
                            )
                        else:
                            status = "RUNNING"
                            lock_token = uuid.uuid4()
                            started_at = datetime.now(timezone.utc)

                    # Вставляем запись о запуске в таблицу `runs`
                    inserted_row = await conn.fetchrow(
                        """
                        INSERT INTO runs (
                            run_id, process_id, process_name, target_date,
                            date_from, date_to, initiator, user_name,
                            status, stage, progress, subscribers,
                            blocked_reason, lock_token, started_at
                        )
                        VALUES (
                            $1, $2, $3, $4,
                            $5, $6, $7, $8,
                            $9, NULL, 0, $10::jsonb,
                            $11, $12, $13
                        )
                        RETURNING *
                        """,
                        run_id,
                        request.process_id,
                        process_name,
                        request.target_date,
                        date_from,
                        date_to,
                        request.initiator,
                        request.user_name,
                        status,
                        json.dumps([request.user_name], ensure_ascii=False),
                        blocked_reason,
                        lock_token,
                        started_at,
                    )

                    await database.log_audit_event(
                        run_id=run_id,
                        event_type="CREATED",
                        message=f"Создана заявка {process_name} на {request.target_date} ({request.initiator})",
                        details={"status": status, "user_name": request.user_name},

                    )

                    # Если конфликтов нет — атомарно захватываем все ключи и готовим старт воркера
                    if status == "RUNNING":
                        await lock_manager.acquire_locks(

                            run_id=run_id,
                            required_locks=lock_specs,
                            ttl_sec=self._lock_ttl_sec,
                        )
                        await database.log_audit_event(
                            run_id=run_id,
                            event_type="STARTED",
                            message=f"Ресурсы зарезервированы, запуск #{str(run_id)[:8]} переведён в RUNNING",
                            details={"locks_count": len(lock_specs)},

                        )
                        run_to_start = run_id
                    elif status == "WAITING_TIMER":
                        await database.log_audit_event(
                            run_id=run_id,
                            event_type="TIMER_WAIT",
                            message=blocked_reason
                            or "Переведён в ожидание таймера машины",

                        )
                    elif status == "QUEUED":
                        await database.log_audit_event(
                            run_id=run_id,
                            event_type="QUEUED",
                            message=blocked_reason
                            or "Поставлен в очередь ожидания ресурсов",

                        )

                    result_payload = {
                        "action": "CREATED",
                        "run": self._row_to_run_info(inserted_row),
                    }

        # Запускаем воркер уже после освобождения self._lock
        if run_to_start is not None and self._worker_callback is not None:
            asyncio.create_task(self._worker_callback(run_to_start))

        await self._broadcast_state_safe()
        return result_payload

    # -------------------------------------------------------------------------
    # 3. Отмена задачи из очереди или таймера (cancel_run, FR-16)
    # -------------------------------------------------------------------------

    async def cancel_run(self, run_id: UUID) -> dict[str, Any]:
        """
        Отменяет задачу, находящуюся в статусе `QUEUED` или `WAITING_TIMER`.

        Если задача уже выполняется (`RUNNING`) или завершена, выбрасывает `ValueError`
        (API вернёт HTTP 409 Conflict).
        """
        async with self._lock:
            pool = await database.get_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT status, process_name FROM runs WHERE run_id = $1",
                    run_id,
                )
                if not row:
                    raise KeyError(f"Запуск {run_id} не найден")

                current_status = row["status"]
                if current_status not in ("QUEUED", "WAITING_TIMER"):
                    raise ValueError(
                        f"Нельзя отменить задачу в статусе {current_status} (допустимо только QUEUED или WAITING_TIMER)"
                    )

                await conn.execute(
                    """
                    UPDATE runs
                    SET status = 'CANCELLED',
                        blocked_reason = 'Отменено пользователем',
                        finished_at = NOW()
                    WHERE run_id = $1
                    """,
                    run_id,
                )
                await database.log_audit_event(
                    run_id=run_id,
                    event_type="CANCELLED",
                    message=f"Задача #{str(run_id)[:8]} ({row['process_name']}) отменена из {current_status}",

                )

        await self._broadcast_state_safe()
        return {"status": "CANCELLED", "run_id": str(run_id)}

    # -------------------------------------------------------------------------
    # 4. Завершение задачи и пробуждение очереди (complete_run / process_queue, FR-10)
    # -------------------------------------------------------------------------

    async def complete_run(
        self,
        run_id: UUID,
        status: str = "COMPLETED",
        error: Optional[str] = None,
    ) -> None:
        """
        Фиксирует завершение воркера (`COMPLETED` или `FAILED`), гарантированно
        освобождает его блокировки и немедленно запускает пересмотр очереди.
        """
        async with self._lock:
            pool = await database.get_pool()
            async with pool.acquire() as conn:
                progress_val = 100 if status == "COMPLETED" else 0
                await conn.execute(
                    """
                    UPDATE runs
                    SET status = $1,
                        progress = CASE WHEN $1 = 'COMPLETED' THEN 100 ELSE progress END,
                        blocked_reason = $2,
                        finished_at = NOW()
                    WHERE run_id = $3
                      AND status = 'RUNNING'
                    """,
                    status,
                    error,
                    run_id,
                )
                # Гарантированно удаляем все ключи блокировок данного запуска
                await lock_manager.release_locks(conn, run_id)

                await database.log_audit_event(
                    run_id=run_id,
                    event_type=status,
                    message=(
                        f"Запуск #{str(run_id)[:8]} завершён со статусом {status}"
                        + (f": {error}" if error else "")
                    ),
                    details={"progress": progress_val},

                )

        # Пробуждаем ожидающие задачи и рассылаем обновление интерфейса
        await self.process_queue()
        await self._broadcast_state_safe()

    async def process_queue(self) -> None:
        """
        Пересматривает очередь ожидания и таймер машины (FR-10).

        1. Если в системе не осталось активных/ждущих пользовательских задач (`USER`),
           переводит регламентные задачи из `WAITING_TIMER` в `QUEUED`.
        2. Сортирует задачи в `QUEUED` по приоритету (`USER` выше `MACHINE`) и времени
           создания (`created_at ASC` — FIFO) и запускает все задачи, чьи ресурсы свободны.
        """
        runs_to_launch: list[UUID] = []

        async with self._lock:
            pool = await database.get_pool()
            async with pool.acquire() as conn:
                # Проверяем наличие пользовательских задач в работе или очереди
                user_active_count = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM runs
                    WHERE initiator = 'USER'
                      AND status IN ('RUNNING', 'QUEUED')
                    """
                )

                # Если пользовательских задач 0 — освобождаем регламентные задачи из таймера
                if not user_active_count or user_active_count == 0:
                    promoted = await conn.fetch(
                        """
                        UPDATE runs
                        SET status = 'QUEUED',
                            blocked_reason = 'Переведено из таймера в общую очередь'
                        WHERE status = 'WAITING_TIMER'
                        RETURNING run_id
                        """
                    )
                    for rec in promoted:
                        await database.log_audit_event(
                            run_id=rec["run_id"],
                            event_type="QUEUED",
                            message="Пользовательских задач нет: регламентный запуск переведён из WAITING_TIMER в QUEUED",

                        )

                # Загружаем все задачи, ожидающие в очереди QUEUED
                queued_rows = await conn.fetch(
                    """
                    SELECT run_id, process_id, process_name, target_date, initiator, created_at
                    FROM runs
                    WHERE status = 'QUEUED'
                    """
                )

                # Сортируем по приоритету инициатора (USER=0, MACHINE=1) и времени постановки (FIFO)
                sorted_queue = sorted(
                    queued_rows,
                    key=lambda r: (
                        0 if r["initiator"] == "USER" else 1,
                        r["created_at"],
                    ),
                )

                for row in sorted_queue:
                    # Повторно страхуем приоритет: если в очереди уже есть USER-задачи,
                    # не пускаем MACHINE-задачи в обход них
                    if row["initiator"] == "MACHINE":
                        current_users = await conn.fetchval(
                            """
                            SELECT COUNT(*)
                            FROM runs
                            WHERE initiator = 'USER'
                              AND status IN ('RUNNING', 'QUEUED')
                            """
                        )
                        if current_users and current_users > 0:
                            continue

                    lock_specs = self._registry.get_required_locks(
                        row["process_id"], row["target_date"]
                    )
                    conflicts = await lock_manager.check_conflicts(conn, lock_specs)
                    if conflicts:
                        # Ресурсы всё ещё заняты — обновляем причину ожидания и идём к следующей задаче
                        first = conflicts[0]
                        owner_short = str(first["run_id"])[:8]
                        reason = (
                            f"Ожидание освобождения {first['resource_id']} "
                            f"({first['date_from']}..{first['date_to']}, владелец #{owner_short})"
                        )
                        await conn.execute(
                            "UPDATE runs SET blocked_reason = $1 WHERE run_id = $2",
                            reason,
                            row["run_id"],
                        )
                        continue

                    # Конфликтов нет: атомарно захватываем блокировки и переводим в RUNNING
                    run_id: UUID = row["run_id"]
                    lock_token = uuid.uuid4()
                    await lock_manager.acquire_locks(

                        run_id=run_id,
                        required_locks=lock_specs,
                        ttl_sec=self._lock_ttl_sec,
                    )
                    await conn.execute(
                        """
                        UPDATE runs
                        SET status = 'RUNNING',
                            lock_token = $1,
                            blocked_reason = NULL,
                            started_at = NOW()
                        WHERE run_id = $2
                        """,
                        lock_token,
                        run_id,
                    )
                    await database.log_audit_event(
                        run_id=run_id,
                        event_type="STARTED",
                        message=f"Задача #{str(run_id)[:8]} ({row['process_name']}) запущена из очереди",

                    )
                    runs_to_launch.append(run_id)

        # Запускаем корутины воркеров вне глобальной блокировки
        if self._worker_callback is not None:
            for r_id in runs_to_launch:
                asyncio.create_task(self._worker_callback(r_id))

        if runs_to_launch:
            await self._broadcast_state_safe()

    # -------------------------------------------------------------------------
    # 5. Фоновые циклы: Таймер машины и Watchdog TTL (NFR-02)
    # -------------------------------------------------------------------------

    async def _machine_timer_loop(self) -> None:
        """
        Фоновый цикл таймера регламентных задач (`WAITING_TIMER`).

        Каждые `machine_timer_interval_sec` секунд инициирует проверку очереди,
        чтобы разбудить машинные задачи, как только пользовательская нагрузка спадёт.
        """
        while True:
            await asyncio.sleep(self._machine_timer_interval_sec)
            try:
                await self.process_queue()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Ошибка в фоновом цикле таймера машины: %s", exc)

    async def _watchdog_loop(self) -> None:
        """
        Фоновый сторожевой таймер (Watchdog, NFR-02).

        Каждые 2 секунды сканирует таблицу `active_locks` на предмет ключей с истёкшим
        сроком действия (`expires_at < NOW()`). Если воркер аварийно упал и перестал
        слать Heartbeat, Watchdog переводит задачу в `FAILED`, принудительно снимает
        блокировки и пробуждает очередь.
        """
        while True:
            await asyncio.sleep(2)
            try:
                expired_found = False
                async with self._lock:
                    pool = await database.get_pool()
                    async with pool.acquire() as conn:
                        expired_rows = await conn.fetch(
                            """
                            SELECT DISTINCT run_id
                            FROM active_locks
                            WHERE expires_at < NOW()
                            """
                        )
                        for row in expired_rows:
                            expired_found = True
                            stale_run_id: UUID = row["run_id"]
                            await conn.execute(
                                """
                                UPDATE runs
                                SET status = 'FAILED',
                                    blocked_reason = 'Превышен TTL блокировки (аварийное завершение воркера / Watchdog)',
                                    finished_at = NOW()
                                WHERE run_id = $1
                                  AND status = 'RUNNING'
                                """,
                                stale_run_id,
                            )
                            await lock_manager.release_locks(conn, stale_run_id)
                            await database.log_audit_event(
                                run_id=stale_run_id,
                                event_type="TTL_EXPIRED",
                                message=(
                                    f"Watchdog принудительно снял блокировки зависшего запуска "
                                    f"#{str(stale_run_id)[:8]} по истечении TTL ({self._lock_ttl_sec}с)"
                                ),

                            )

                if expired_found:
                    await self.process_queue()
                    await self._broadcast_state_safe()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Ошибка в фоновом цикле Watchdog: %s", exc)

    def start_background_tasks(self) -> None:
        """Запускает фоновые корутины таймера машины и Watchdog при старте сервера."""
        if self._bg_tasks:
            return
        self._bg_tasks.append(asyncio.create_task(self._machine_timer_loop()))
        self._bg_tasks.append(asyncio.create_task(self._watchdog_loop()))
        logger.info(
            "Фоновые задачи оркестратора запущены (timer=%ss, ttl=%ss)",
            self._machine_timer_interval_sec,
            self._lock_ttl_sec,
        )

    async def stop_background_tasks(self) -> None:
        """Корректно останавливает все фоновые задачи при завершении работы сервера."""
        for task in self._bg_tasks:
            task.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()

    # -------------------------------------------------------------------------
    # 6. Снимок состояния для UI и динамическое обновление настроек (FR-14)
    # -------------------------------------------------------------------------

    async def get_state_snapshot(self) -> SystemStateSnapshot:
        """
        Формирует полный согласованный снимок состояния системы для REST и WebSocket.
        """
        pool = await database.get_pool()
        async with pool.acquire() as conn:
            active_rows = await conn.fetch(
                "SELECT * FROM runs WHERE status = 'RUNNING' ORDER BY started_at ASC"
            )
            queue_rows = await conn.fetch(
                """
                SELECT * FROM runs
                WHERE status = 'QUEUED'
                ORDER BY (CASE WHEN initiator = 'USER' THEN 0 ELSE 1 END), created_at ASC
                """
            )
            timer_rows = await conn.fetch(
                "SELECT * FROM runs WHERE status = 'WAITING_TIMER' ORDER BY created_at ASC"
            )
            lock_rows = await conn.fetch(
                """
                SELECT lock_id, run_id, resource_id, date_from, date_to, access_mode, expires_at
                FROM active_locks
                ORDER BY lock_id ASC
                """
            )
            history_rows = await conn.fetch(
                """
                SELECT * FROM runs
                WHERE status IN ('COMPLETED', 'FAILED', 'CANCELLED')
                ORDER BY finished_at DESC NULLS LAST
                LIMIT 20
                """
            )
            audit_rows = await conn.fetch(
                """
                SELECT id, run_id, event_type, message, details, created_at
                FROM audit_logs
                ORDER BY id DESC
                LIMIT 30
                """
            )

        active_runs = [self._row_to_run_info(r) for r in active_rows]
        queue = [self._row_to_run_info(r) for r in queue_rows]
        timer_waiting = [self._row_to_run_info(r) for r in timer_rows]
        history = [self._row_to_run_info(r) for r in history_rows]
        active_locks = [LockInfo(**dict(r)) for r in lock_rows]

        audit_logs: list[dict[str, Any]] = []
        for r in audit_rows:
            item = dict(r)
            if isinstance(item.get("details"), str):
                item["details"] = json.loads(item["details"])
            if item.get("run_id") is not None:
                item["run_id"] = str(item["run_id"])
            if isinstance(item.get("created_at"), datetime):
                item["created_at"] = item["created_at"].isoformat()
            audit_logs.append(item)

        return SystemStateSnapshot(
            active_runs=active_runs,
            queue=queue,
            timer_waiting=timer_waiting,
            active_locks=active_locks,
            history=history,
            audit_logs=audit_logs,
            config={
                "machine_timer_interval_sec": self._machine_timer_interval_sec,
                "lock_ttl_sec": self._lock_ttl_sec,
                "heartbeat_interval_sec": self._heartbeat_interval_sec,
            },
        )

    async def update_config(self, cfg: ConfigUpdateRequest) -> dict[str, int]:
        """Динамически обновляет интервалы таймера машины, TTL и Heartbeat без перезапуска."""
        if getattr(cfg, "machine_timer_interval_sec", None) is not None:
            self._machine_timer_interval_sec = int(cfg.machine_timer_interval_sec)
        if getattr(cfg, "lock_ttl_sec", None) is not None:
            self._lock_ttl_sec = int(cfg.lock_ttl_sec)
        if getattr(cfg, "heartbeat_interval_sec", None) is not None:
            self._heartbeat_interval_sec = int(cfg.heartbeat_interval_sec)

        self._config.update(
            {
                "machine_timer_interval_sec": self._machine_timer_interval_sec,
                "lock_ttl_sec": self._lock_ttl_sec,
                "heartbeat_interval_sec": self._heartbeat_interval_sec,
            }
        )
        await self._broadcast_state_safe()
        return {
            "machine_timer_interval_sec": self._machine_timer_interval_sec,
            "lock_ttl_sec": self._lock_ttl_sec,
            "heartbeat_interval_sec": self._heartbeat_interval_sec,
        }
