"""Главный диспетчер и оркестратор параллельных процессов оценки рисков."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import date, datetime, timedelta, timezone
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

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "processes.json"


class Orchestrator:
    """Координирует жизненный цикл расчётов под единым asyncio.Lock."""

    def __init__(
        self,
        worker_callback: Optional[Callable[[UUID], Awaitable[None]]] = None,
        ws_manager: Any = None,
        config_path: str | Path = DEFAULT_CONFIG_PATH,
    ) -> None:
        self._worker_callback = worker_callback
        self._ws_manager = ws_manager
        self._config_path = Path(config_path)

        self._active_lock: Optional[asyncio.Lock] = None
        self._active_loop: Optional[asyncio.AbstractEventLoop] = None
        self._registry = ProcessRegistry(self._config_path)

        self._config: dict[str, Any] = self._load_config()
        self._machine_timer_interval_sec: int = int(
            os.getenv(
                "MACHINE_TIMER_INTERVAL_SEC",
                self._config.get("machine_timer_interval_sec", 15),
            )
        )
        self._lock_ttl_sec: int = int(
            os.getenv("LOCK_TTL_SEC", self._config.get("lock_ttl_sec", 25))
        )
        self._heartbeat_interval_sec: int = int(
            self._config.get("heartbeat_interval_sec", 3)
        )
        self._bg_tasks: list[asyncio.Task[Any]] = []
        self._worker_tasks: set[asyncio.Task[Any]] = set()
        self._persist_tasks: set[asyncio.Task[Any]] = set()
        self._mem_runs: dict[UUID, dict[str, Any]] = {}
        self._mem_locks: list[dict[str, Any]] = []
        self._mem_initialized: bool = False

    async def init_memory_cache(self) -> None:
        """Синхронизирует оперативный кэш с базой данных при старте."""
        async with self._lock:
            self._mem_runs.clear()
            self._mem_locks.clear()
            pool = await database.get_pool()
            async with pool.acquire() as conn:
                runs = await conn.fetch("SELECT * FROM runs WHERE status IN ('WAITING_TIMER', 'QUEUED', 'RUNNING')")
                for r in runs:
                    d = dict(r)
                    subs = d.get("subscribers")
                    if isinstance(subs, str):
                        d["subscribers"] = json.loads(subs)
                    self._mem_runs[d["run_id"]] = d
                locks = await conn.fetch("SELECT * FROM active_locks WHERE expires_at > NOW()")
                for l in locks:
                    self._mem_locks.append(
                        {
                            "run_id": l["run_id"],
                            "resource_id": l["resource_id"],
                            "date_from": lock_manager._to_date(l["date_from"]),
                            "date_to": lock_manager._to_date(l["date_to"]),
                            "access_mode": l["access_mode"].upper(),
                            "expires_at": l["expires_at"],
                        }
                    )
            self._mem_initialized = True

    def reset_memory_state(self) -> None:
        """Очищает структуры оперативной памяти для изолированных тестов."""
        self._mem_runs.clear()
        self._mem_locks.clear()
        self._mem_initialized = True

    @property
    def _lock(self) -> asyncio.Lock:
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None
        if getattr(self, "_active_loop", None) is not current_loop or getattr(self, "_active_lock", None) is None:
            self._active_lock = asyncio.Lock()
            self._active_loop = current_loop
        return self._active_lock

    def _load_config(self) -> dict[str, Any]:
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
        processes = self._config.get("processes", {})
        if process_id not in processes:
            raise ValueError(f"Неизвестный идентификатор процесса: {process_id}")
        return processes[process_id]

    @staticmethod
    def _row_to_run_info(
        row: asyncpg.Record | dict[str, Any],
        position: Optional[int] = None,
    ) -> RunInfo:
        data = dict(row)
        raw_subs = data.get("subscribers")
        if isinstance(raw_subs, str):
            data["subscribers"] = json.loads(raw_subs)
        elif raw_subs is None:
            data["subscribers"] = []
        if position is not None:
            data["position"] = position
        return RunInfo(**data)

    async def _broadcast_state_safe(self) -> None:
        if self._ws_manager is None:
            return
        if hasattr(self._ws_manager, "active_connections") and not self._ws_manager.active_connections:
            return
        try:
            snapshot = await self.get_state_snapshot()
            await self._ws_manager.broadcast_state(snapshot.model_dump(mode="json"))
        except Exception as exc:
            logger.warning("Ошибка при рассылке состояния по WebSocket: %s", exc)

    async def dry_run(self, request: RunCreateRequest) -> DryRunResponse:
        """Симулирует диспетчеризацию без записи в БД и захвата блокировок."""
        self._get_process_meta(request.process_id)
        lock_specs: list[ResourceLockSpec] = self._registry.get_required_locks(
            request.process_id, request.target_date
        )
        date_from = lock_specs[0].date_from if lock_specs else request.target_date
        date_to = lock_specs[0].date_to if lock_specs else request.target_date
        required_lock_models = [
            LockInfo(
                resource_id=s.resource_id,
                date_from=s.date_from,
                date_to=s.date_to,
                access_mode=s.access_mode,
            )
            for s in lock_specs
        ]

        async with self._lock:
            # 1. Проверяем в оперативной памяти
            for r in self._mem_runs.values():
                if (
                    r["process_id"] == request.process_id
                    and r["target_date"] == request.target_date
                    and r["status"] in ("WAITING_TIMER", "QUEUED", "RUNNING")
                ):
                    short_id = str(r["run_id"])[:8]
                    return DryRunResponse(
                        prediction="DEDUPLICATE",
                        date_from=date_from,
                        date_to=date_to,
                        reason=(
                            f"Расчёт уже активен или ожидает (#{short_id}, статус {r['status']}). "
                            f"Запрос будет присоединён к существующему запуску."
                        ),
                        required_locks=required_lock_models,
                    )

            if request.initiator == "MACHINE":
                user_count = sum(
                    1
                    for r in self._mem_runs.values()
                    if r["initiator"] == "USER" and r["status"] in ("RUNNING", "QUEUED")
                )
                if user_count > 0:
                    return DryRunResponse(
                        prediction="WAIT_TIMER",
                        date_from=date_from,
                        date_to=date_to,
                        reason=(
                            f"В системе активны или ожидают {user_count} пользовательских задач. "
                            f"Регламентный запуск будет отложен по таймеру."
                        ),
                        required_locks=required_lock_models,
                    )

            mem_conflicts = []
            for req in lock_specs:
                req_f = lock_manager._to_date(req.date_from)
                req_t = lock_manager._to_date(req.date_to)
                req_m = req.access_mode.upper()
                for act in self._mem_locks:
                    if req.resource_id != act["resource_id"]:
                        continue
                    if not lock_manager._intersects(req_f, req_t, act["date_from"], act["date_to"]):
                        continue
                    if req_m == "READ" and act["access_mode"] == "READ":
                        continue
                    mem_conflicts.append(
                        {
                            "run_id": act["run_id"],
                            "resource_id": act["resource_id"],
                            "date_from": act["date_from"],
                            "date_to": act["date_to"],
                            "req_mode": req_m,
                            "act_mode": act["access_mode"],
                        }
                    )

            if mem_conflicts:
                first = mem_conflicts[0]
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
                    required_locks=required_lock_models,
                )

            # Если в памяти нет данных и кэш не инициализирован, проверяем в БД (холодный старт)
            if not self._mem_initialized and not self._mem_runs and not self._mem_locks:
                pool = await database.get_pool()
                async with pool.acquire() as conn:
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
                                f"Запрос будет присоединён к существующему запуску."
                            ),
                            required_locks=required_lock_models,
                        )

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
                                    f"В системе активны или ожидают {user_count} пользовательских задач. "
                                    f"Регламентный запуск будет отложен по таймеру."
                                ),
                                required_locks=required_lock_models,
                            )

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
                            required_locks=required_lock_models,
                        )

            return DryRunResponse(
                prediction="RUN_IMMEDIATELY",
                date_from=date_from,
                date_to=date_to,
                reason="Все целевые ресурсы и диапазоны дат свободны. Запуск начнётся немедленно.",
                required_locks=required_lock_models,
            )

    async def submit_run(self, request: RunCreateRequest) -> dict[str, Any]:
        """Регистрирует заявку, присоединяет к дублю или ставит в очередь/таймер."""
        proc_meta = self._get_process_meta(request.process_id)
        process_name: str = proc_meta.get("name", request.process_id)
        lock_specs: list[ResourceLockSpec] = self._registry.get_required_locks(
            request.process_id, request.target_date
        )
        date_from = lock_specs[0].date_from if lock_specs else request.target_date
        date_to = lock_specs[0].date_to if lock_specs else request.target_date

        run_to_start: Optional[UUID] = None
        audit_item: Optional[tuple[Any, ...]] = None
        is_attached = False

        async with self._lock:
            dup_run: Optional[dict[str, Any]] = None
            for r in self._mem_runs.values():
                if (
                    r["process_id"] == request.process_id
                    and r["target_date"] == request.target_date
                    and r["status"] in ("WAITING_TIMER", "QUEUED", "RUNNING")
                ):
                    dup_run = r
                    break

            if dup_run is None and not self._mem_initialized and not self._mem_runs:
                pool = await database.get_pool()
                async with pool.acquire() as conn:
                    row = await conn.fetchrow(
                        """
                        SELECT * FROM runs
                        WHERE process_id = $1 AND target_date = $2
                          AND status IN ('WAITING_TIMER', 'QUEUED', 'RUNNING')
                        ORDER BY created_at ASC LIMIT 1
                        """,
                        request.process_id,
                        request.target_date,
                    )
                    if row:
                        dup_run = dict(row)
                        subs = dup_run.get("subscribers")
                        if isinstance(subs, str):
                            dup_run["subscribers"] = json.loads(subs)
                        self._mem_runs[dup_run["run_id"]] = dup_run

            if dup_run is not None:
                is_attached = True
                existing_id: UUID = dup_run["run_id"]
                subscribers: list[str] = dup_run.get("subscribers") or []
                if isinstance(subscribers, str):
                    subscribers = json.loads(subscribers)
                    dup_run["subscribers"] = subscribers
                if request.user_name not in subscribers:
                    subscribers.append(request.user_name)

                audit_item = (
                    existing_id,
                    "DEDUPLICATED",
                    f"Заявка от {request.user_name} ({request.initiator}) присоединена к запуску #{str(existing_id)[:8]}",
                    {
                        "subscriber": request.user_name,
                        "initiator": request.initiator,
                        "process_id": request.process_id,
                        "target_date": str(request.target_date),
                    },
                )
                run_dict_for_response = dict(dup_run)
            else:
                run_id = uuid.uuid4()
                status = "RUNNING"
                blocked_reason: Optional[str] = None
                lock_token: Optional[UUID] = None
                started_at: Optional[datetime] = None

                if request.initiator == "MACHINE":
                    user_active = any(
                        r["initiator"] == "USER" and r["status"] in ("RUNNING", "QUEUED")
                        for r in self._mem_runs.values()
                    )
                    if not user_active and not self._mem_initialized and not self._mem_runs:
                        pool = await database.get_pool()
                        async with pool.acquire() as conn:
                            cnt = await conn.fetchval(
                                "SELECT COUNT(*) FROM runs WHERE initiator = 'USER' AND status IN ('RUNNING', 'QUEUED')"
                            )
                            if cnt and cnt > 0:
                                user_active = True

                    if user_active:
                        status = "WAITING_TIMER"
                        blocked_reason = "Ожидание завершения пользовательских задач"

                if status != "WAITING_TIMER":
                    conflicts = []
                    for req in lock_specs:
                        req_f = lock_manager._to_date(req.date_from)
                        req_t = lock_manager._to_date(req.date_to)
                        req_m = req.access_mode.upper()
                        for act in self._mem_locks:
                            if req.resource_id != act["resource_id"]:
                                continue
                            if not lock_manager._intersects(req_f, req_t, act["date_from"], act["date_to"]):
                                continue
                            if req_m == "READ" and act["access_mode"] == "READ":
                                continue
                            conflicts.append(
                                {
                                    "run_id": act["run_id"],
                                    "resource_id": act["resource_id"],
                                    "date_from": act["date_from"],
                                    "date_to": act["date_to"],
                                    "req_mode": req_m,
                                    "act_mode": act["access_mode"],
                                }
                            )

                    if not conflicts and not self._mem_initialized and not self._mem_locks:
                        pool = await database.get_pool()
                        async with pool.acquire() as conn:
                            db_confs = await lock_manager.check_conflicts(conn, lock_specs)
                            if db_confs:
                                conflicts = db_confs

                    if conflicts:
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
                        expires_at = started_at + timedelta(seconds=self._lock_ttl_sec)
                        for s in lock_specs:
                            self._mem_locks.append(
                                {
                                    "run_id": run_id,
                                    "resource_id": s.resource_id,
                                    "date_from": lock_manager._to_date(s.date_from),
                                    "date_to": lock_manager._to_date(s.date_to),
                                    "access_mode": s.access_mode.upper(),
                                    "expires_at": expires_at,
                                }
                            )

                run_dict_for_response = {
                    "run_id": run_id,
                    "process_id": request.process_id,
                    "process_name": process_name,
                    "target_date": request.target_date,
                    "date_from": date_from,
                    "date_to": date_to,
                    "initiator": request.initiator,
                    "user_name": request.user_name,
                    "status": status,
                    "stage": None,
                    "progress": 0,
                    "subscribers": [request.user_name],
                    "blocked_reason": blocked_reason,
                    "lock_token": lock_token,
                    "created_at": datetime.now(timezone.utc),
                    "started_at": started_at,
                    "finished_at": None,
                }
                self._mem_runs[run_id] = run_dict_for_response

                if status == "RUNNING":
                    audit_item = (
                        run_id,
                        "STARTED",
                        f"Создана заявка {process_name} на {request.target_date} ({request.initiator}), статус RUNNING",
                        {"status": status, "user_name": request.user_name, "locks_count": len(lock_specs)},
                    )
                    run_to_start = run_id
                elif status == "WAITING_TIMER":
                    audit_item = (
                        run_id,
                        "TIMER_WAIT",
                        blocked_reason or "Переведён в ожидание таймера машины",
                        {"status": status, "user_name": request.user_name},
                    )
                elif status == "QUEUED":
                    audit_item = (
                        run_id,
                        "QUEUED",
                        blocked_reason or "Поставлен в очередь ожидания ресурсов",
                        {"status": status, "user_name": request.user_name},
                    )

        action = "ATTACHED" if is_attached else "CREATED"
        p_task = asyncio.create_task(
            self._persist_run_in_db(is_attached, run_dict_for_response, lock_specs, audit_item)
        )
        self._persist_tasks.add(p_task)
        p_task.add_done_callback(self._persist_tasks.discard)

        if run_to_start is not None and self._worker_callback is not None:
            cb_res = self._worker_callback(run_to_start)
            if asyncio.iscoroutine(cb_res):
                w_task = asyncio.create_task(cb_res)
                self._worker_tasks.add(w_task)
                w_task.add_done_callback(self._worker_tasks.discard)

        asyncio.create_task(self._broadcast_state_safe())
        return {
            "action": action,
            "run": RunInfo(**run_dict_for_response).model_dump(mode="json"),
        }

    async def wait_pending_persists(self) -> None:
        """Ожидает завершения всех фоновых операций персистентности."""
        tasks = list(self._persist_tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._persist_tasks.clear()

    async def _persist_run_in_db(
        self,
        is_attached: bool,
        run_data: dict[str, Any],
        lock_specs: list[ResourceLockSpec],
        audit_item: Optional[tuple[Any, ...]],
    ) -> None:
        try:
            pool = await database.get_pool()
            async with pool.acquire() as conn:
                async with conn.transaction():
                    if is_attached:
                        await conn.execute(
                            "UPDATE runs SET subscribers = $1::jsonb WHERE run_id = $2",
                            json.dumps(run_data["subscribers"], ensure_ascii=False),
                            run_data["run_id"],
                        )
                    else:
                        await conn.execute(
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
                            ON CONFLICT (run_id) DO NOTHING
                            """,
                            run_data["run_id"],
                            run_data["process_id"],
                            run_data["process_name"],
                            run_data["target_date"],
                            run_data["date_from"],
                            run_data["date_to"],
                            run_data["initiator"],
                            run_data["user_name"],
                            run_data["status"],
                            json.dumps(run_data["subscribers"], ensure_ascii=False),
                            run_data["blocked_reason"],
                            run_data["lock_token"],
                            run_data["started_at"],
                        )
                        if run_data["status"] == "RUNNING" and lock_specs:
                            await lock_manager.acquire_locks(
                                conn=conn,
                                run_id=run_data["run_id"],
                                required_locks=lock_specs,
                                ttl_sec=self._lock_ttl_sec,
                            )
                    if audit_item is not None:
                        await database.log_audit_event(*audit_item, conn=conn)
        except Exception as exc:
            logger.exception("Ошибка персистентности задачи %s в БД: %s", run_data.get("run_id"), exc)

    async def cancel_run(self, run_id: UUID) -> dict[str, Any]:
        """Отменяет задачу в статусе QUEUED или WAITING_TIMER."""
        run_uuid = UUID(str(run_id))
        async with self._lock:
            if run_uuid in self._mem_runs:
                self._mem_runs[run_uuid]["status"] = "CANCELLED"
            self._mem_locks = [l for l in self._mem_locks if l["run_id"] != run_uuid]
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
                    conn=conn,
                )

        await self.process_queue()
        await self._broadcast_state_safe()
        return {"status": "CANCELLED", "run_id": str(run_id)}

    async def complete_run(
        self,
        run_id: UUID,
        status: str = "COMPLETED",
        error: Optional[str] = None,
    ) -> None:
        """Фиксирует завершение воркера, освобождает блокировки и пробуждает очередь."""
        run_uuid = UUID(str(run_id))
        async with self._lock:
            self._mem_locks = [l for l in self._mem_locks if l["run_id"] != run_uuid]
            if run_uuid in self._mem_runs:
                self._mem_runs[run_uuid]["status"] = status
                self._mem_runs[run_uuid]["blocked_reason"] = error
                self._mem_runs[run_uuid]["finished_at"] = datetime.now(timezone.utc)
                if status == "COMPLETED":
                    self._mem_runs[run_uuid]["progress"] = 100

            pool = await database.get_pool()
            async with pool.acquire() as conn:
                progress_val = 100 if status == "COMPLETED" else 0
                await conn.execute(
                    """
                    UPDATE runs
                    SET status = $1::text,
                        progress = CASE WHEN $1::text = 'COMPLETED' THEN 100 ELSE progress END,
                        blocked_reason = $2,
                        finished_at = NOW()
                    WHERE run_id = $3
                      AND status = 'RUNNING'
                    """,
                    status,
                    error,
                    run_id,
                )
                await lock_manager.release_locks(conn, run_id)
                await database.log_audit_event(
                    run_id=run_id,
                    event_type=status,
                    message=(
                        f"Запуск #{str(run_id)[:8]} завершён со статусом {status}"
                        + (f": {error}" if error else "")
                    ),
                    details={"progress": progress_val},
                    conn=conn,
                )

        await self.process_queue()
        await self._broadcast_state_safe()

    async def process_queue(self) -> None:
        """Переводит задачи из таймера при отсутствии USER-задач и запускает свободные из QUEUED."""
        runs_to_launch: list[UUID] = []
        state_changed = False

        async with self._lock:
            pool = await database.get_pool()
            async with pool.acquire() as conn:
                user_active_count = await conn.fetchval(
                    """
                    SELECT COUNT(*)
                    FROM runs
                    WHERE initiator = 'USER'
                      AND status IN ('RUNNING', 'QUEUED')
                    """
                )

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
                    if promoted:
                        state_changed = True
                    for rec in promoted:
                        await database.log_audit_event(
                            run_id=rec["run_id"],
                            event_type="QUEUED",
                            message="Пользовательских задач нет: регламентный запуск переведён из WAITING_TIMER в QUEUED",
                            conn=conn,
                        )

                queued_rows = await conn.fetch(
                    """
                    SELECT run_id, process_id, process_name, target_date, initiator, created_at
                    FROM runs
                    WHERE status = 'QUEUED'
                    """
                )

                sorted_queue = sorted(
                    queued_rows,
                    key=lambda r: (0 if str(r["initiator"]).upper() == "USER" else 1, r["created_at"]),
                )

                for row in sorted_queue:
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

                    run_id: UUID = row["run_id"]
                    lock_token = uuid.uuid4()
                    await lock_manager.acquire_locks(
                        conn=conn,
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
                        conn=conn,
                    )
                    runs_to_launch.append(run_id)
                    state_changed = True

        if self._worker_callback is not None:
            for r_id in runs_to_launch:
                cb_res = self._worker_callback(r_id)
                if asyncio.iscoroutine(cb_res):
                    w_task = asyncio.create_task(cb_res)
                    self._worker_tasks.add(w_task)
                    w_task.add_done_callback(self._worker_tasks.discard)

        if state_changed:
            await self._broadcast_state_safe()

    async def _machine_timer_loop(self) -> None:
        elapsed = 0
        while True:
            await asyncio.sleep(1)
            elapsed += 1
            if elapsed < self._machine_timer_interval_sec:
                continue
            elapsed = 0
            try:
                await self.process_queue()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Ошибка в фоновом цикле таймера машины: %s", exc)

    async def _watchdog_loop(self) -> None:
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
                            self._mem_locks = [l for l in self._mem_locks if l["run_id"] != stale_run_id]
                            if stale_run_id in self._mem_runs:
                                self._mem_runs[stale_run_id]["status"] = "FAILED"
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
                                    f"Watchdog снял блокировки зависшего запуска "
                                    f"#{str(stale_run_id)[:8]} по истечении TTL ({self._lock_ttl_sec}с)"
                                ),
                                conn=conn,
                            )

                if expired_found:
                    await self.process_queue()
                    await self._broadcast_state_safe()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Ошибка в фоновом цикле Watchdog: %s", exc)

    def start_background_tasks(self) -> None:
        self._bg_tasks = [t for t in self._bg_tasks if not t.done()]
        if self._bg_tasks:
            return
        self._bg_tasks.append(asyncio.create_task(self._machine_timer_loop()))
        self._bg_tasks.append(asyncio.create_task(self._watchdog_loop()))
        logger.info(
            "Фоновые задачи оркестратора запущены (timer=%ss, ttl=%ss)",
            self._machine_timer_interval_sec,
            self._lock_ttl_sec,
        )

    async def cancel_all_workers(self) -> None:
        """Отменяет все активные задачи воркеров."""
        tasks = list(self._worker_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._worker_tasks.clear()

    async def stop_background_tasks(self) -> None:
        for task in self._bg_tasks:
            task.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()
        await self.cancel_all_workers()
        await self.wait_pending_persists()

    async def get_state_snapshot(self) -> SystemStateSnapshot:
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
        queue = [self._row_to_run_info(r, position=idx) for idx, r in enumerate(queue_rows, start=1)]
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
        if cfg.machine_timer_interval_sec is not None:
            self._machine_timer_interval_sec = int(cfg.machine_timer_interval_sec)
        if cfg.lock_ttl_sec is not None:
            self._lock_ttl_sec = int(cfg.lock_ttl_sec)
        if cfg.heartbeat_interval_sec is not None:
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