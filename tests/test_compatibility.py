"""Автотесты функциональных сценариев матрицы совместимости (раздел 8.1 ARCHITECTURE.md).

Покрывает:
- TC-01: Дедупликация (FR-05)
- TC-02: Изоляция буферов (FR-12)
- TC-03: Совместное чтение READ+READ (FR-07)
- TC-04: Скрытый конфликт скользящего окна (Инсайт ИИ №2)
- TC-05: Приоритет «Люди vs Машина» (FR-03)
- TC-06: Отказоустойчивость TTL Watchdog (NFR-02)
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

try:
    import pytest
except ImportError:
    class _MockMark:
        @staticmethod
        def asyncio(f):
            return f

    class _MockPytest:
        mark = _MockMark()

        @staticmethod
        def skip(reason=""):
            print(f"[SKIP] {reason}")

    pytest = _MockPytest()

from httpx import AsyncClient

from backend.core.lock_manager import _intersects, _to_date
from backend.core.registry import ProcessRegistry, ResourceLockSpec
from tests.conftest import is_db_available


# ---------------------------------------------------------------------------
# Модульные проверки алгоритмов ядра (работают автономно)
# ---------------------------------------------------------------------------

def test_registry_sliding_windows():
    """Проверка корректности скользящих окон (раздел 1.3 и 5.1)."""
    reg = ProcessRegistry()
    locks_credit = reg.get_required_locks("credit_risk", date(2026, 11, 20))
    # 7 дней скользящего окна: 2026-11-20 - (7 - 1) = 2026-11-14 .. 2026-11-20
    src_lock = next(l for l in locks_credit if l.resource_id == "src_transactions")
    assert src_lock.date_from == date(2026, 11, 14)
    assert src_lock.date_to == date(2026, 11, 20)
    assert src_lock.access_mode == "READ"

    # Выходная витрина mart_credit_risk (WRITE): точечный интервал target_date..target_date
    mart_lock = next(l for l in locks_credit if l.resource_id == "mart_credit_risk")
    assert mart_lock.date_from == date(2026, 11, 20)
    assert mart_lock.date_to == date(2026, 11, 20)
    assert mart_lock.access_mode == "WRITE"

    locks_etl = reg.get_required_locks("etl_transactions", date(2026, 11, 19))
    # 1 день скользящего окна: 2026-11-19 - (1 - 1) = 2026-11-19 .. 2026-11-19
    etl_lock = locks_etl[0]
    assert etl_lock.date_from == date(2026, 11, 19)
    assert etl_lock.date_to == date(2026, 11, 19)
    assert etl_lock.access_mode == "WRITE"


def test_lock_manager_intersection_logic():
    """Проверка формального правила пересечения интервалов (раздел 6.1)."""
    # Интервалы [14.11..20.11] и [19.11..19.11] пересекаются
    assert _intersects(date(2026, 11, 14), date(2026, 11, 20), date(2026, 11, 19), date(2026, 11, 19)) is True

    # Соседние даты витрин [19.11..19.11] и [20.11..20.11] НЕ пересекаются
    assert _intersects(date(2026, 11, 19), date(2026, 11, 19), date(2026, 11, 20), date(2026, 11, 20)) is False

    # Интервалы [01.11..05.11] и [06.11..10.11] НЕ пересекаются
    assert _intersects(date(2026, 11, 1), date(2026, 11, 5), date(2026, 11, 6), date(2026, 11, 10)) is False

    # Граничное касание в 1 день [01.11..05.11] и [05.11..10.11] пересекается
    assert _intersects(date(2026, 11, 1), date(2026, 11, 5), date(2026, 11, 5), date(2026, 11, 10)) is True


# ---------------------------------------------------------------------------
# Интеграционные сценарии (TC-01 .. TC-06)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_tc01_deduplication(client: AsyncClient):
    """TC-01: Дедупликация (FR-05)

    Запустить credit_risk на 2026-11-20 от «Аналитик 1», затем сразу от «Аналитик 2».
    Ожидаемый результат: Создан только 1 воркер; второй запрос получил action="ATTACHED"
    и попал в subscribers.
    """
    if not await is_db_available():
        pytest.skip("PostgreSQL недоступен. Запустите docker-compose up для интеграционных тестов.")

    # 1. Первый запуск от Аналитик 1
    resp1 = await client.post(
        "/api/runs",
        json={
            "process_id": "credit_risk",
            "target_date": "2026-11-20",
            "user_name": "Аналитик 1",
            "initiator": "USER",
        },
    )
    assert resp1.status_code == 200
    data1 = resp1.json()
    assert data1["action"] == "CREATED"
    run_id = data1["run"]["run_id"]
    assert "Аналитик 1" in data1["run"]["subscribers"]

    # 2. Второй идентичный запуск от Аналитик 2
    resp2 = await client.post(
        "/api/runs",
        json={
            "process_id": "credit_risk",
            "target_date": "2026-11-20",
            "user_name": "Аналитик 2",
            "initiator": "USER",
        },
    )
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["action"] == "ATTACHED"
    assert data2["run"]["run_id"] == run_id
    assert "Аналитик 2" in data2["run"]["subscribers"]


@pytest.mark.asyncio
async def test_tc02_buffer_isolation(client: AsyncClient):
    """TC-02: Изоляция буферов (FR-12)

    Запустить credit_risk на 2026-11-10 и credit_risk на 2026-11-20 одновременно.
    Ожидаемый результат: Оба выполняются параллельно (RUNNING), не конфликтуют по датам.
    """
    if not await is_db_available():
        pytest.skip("PostgreSQL недоступен. Запустите docker-compose up для интеграционных тестов.")

    r1, r2 = await asyncio.gather(
        client.post(
            "/api/runs",
            json={
                "process_id": "credit_risk",
                "target_date": "2026-11-10",
                "user_name": "Аналитик 1",
                "initiator": "USER",
            },
        ),
        client.post(
            "/api/runs",
            json={
                "process_id": "credit_risk",
                "target_date": "2026-11-20",
                "user_name": "Аналитик 2",
                "initiator": "USER",
            },
        ),
    )
    assert r1.status_code == 200 and r2.status_code == 200
    d1, d2 = r1.json(), r2.json()

    assert d1["action"] == "CREATED"
    assert d2["action"] == "CREATED"
    assert d1["run"]["status"] == "RUNNING"
    assert d2["run"]["status"] == "RUNNING"
    assert d1["run"]["run_id"] != d2["run"]["run_id"]


@pytest.mark.asyncio
async def test_parallel_credit_risk_adjacent_dates(client: AsyncClient):
    """Параллельный запуск credit_risk на соседние даты (2026-11-19 и 2026-11-20).

    Благодаря точечному интервалу для mart_credit_risk (WRITE) и совместимости READ+READ
    по src_transactions и ref_clients, оба процесса должны сразу перейти в RUNNING.
    """
    if not await is_db_available():
        pytest.skip("PostgreSQL недоступен. Запустите docker-compose up для интеграционных тестов.")

    r1, r2 = await asyncio.gather(
        client.post(
            "/api/runs",
            json={
                "process_id": "credit_risk",
                "target_date": "2026-11-19",
                "user_name": "Аналитик 1",
                "initiator": "USER",
            },
        ),
        client.post(
            "/api/runs",
            json={
                "process_id": "credit_risk",
                "target_date": "2026-11-20",
                "user_name": "Аналитик 2",
                "initiator": "USER",
            },
        ),
    )
    assert r1.status_code == 200 and r2.status_code == 200
    d1, d2 = r1.json(), r2.json()

    assert d1["action"] == "CREATED"
    assert d2["action"] == "CREATED"
    assert d1["run"]["status"] == "RUNNING"
    assert d2["run"]["status"] == "RUNNING"
    assert d1["run"]["run_id"] != d2["run"]["run_id"]


@pytest.mark.asyncio
async def test_tc03_concurrent_read_read(client: AsyncClient):
    """TC-03: Совместное чтение READ+READ (FR-07)

    Запустить credit_risk и counterparty_risk на 2026-11-20 одновременно.
    Ожидаемый результат: Оба выполняются параллельно без взаимной блокировки общих источников.
    """
    if not await is_db_available():
        pytest.skip("PostgreSQL недоступен. Запустите docker-compose up для интеграционных тестов.")

    r1, r2 = await asyncio.gather(
        client.post(
            "/api/runs",
            json={
                "process_id": "credit_risk",
                "target_date": "2026-11-20",
                "user_name": "Аналитик 1",
                "initiator": "USER",
            },
        ),
        client.post(
            "/api/runs",
            json={
                "process_id": "counterparty_risk",
                "target_date": "2026-11-20",
                "user_name": "Аналитик 2",
                "initiator": "USER",
            },
        ),
    )
    assert r1.status_code == 200 and r2.status_code == 200
    d1, d2 = r1.json(), r2.json()

    assert d1["run"]["status"] == "RUNNING"
    assert d2["run"]["status"] == "RUNNING"


@pytest.mark.asyncio
async def test_tc04_sliding_window_conflict(client: AsyncClient):
    """TC-04: Скрытый конфликт скользящего окна (Инсайт ИИ №2)

    Запустить credit_risk на 2026-11-20 (окно 14.11–20.11, READ src_transactions),
    затем etl_transactions на 2026-11-19 (окно 19.11–19.11, WRITE src_transactions).
    Ожидаемый результат: etl_transactions встаёт в QUEUED из-за пересечения интервалов.
    """
    if not await is_db_available():
        pytest.skip("PostgreSQL недоступен. Запустите docker-compose up для интеграционных тестов.")

    # 1. Запуск credit_risk (читает src_transactions с 14.11 по 20.11)
    r1 = await client.post(
        "/api/runs",
        json={
            "process_id": "credit_risk",
            "target_date": "2026-11-20",
            "user_name": "Аналитик 1",
            "initiator": "USER",
        },
    )
    assert r1.status_code == 200
    assert r1.json()["run"]["status"] == "RUNNING"

    # 2. Предпроверка dry-run должна прогнозировать QUEUE
    dry_resp = await client.post(
        "/api/runs/dry-run",
        json={
            "process_id": "etl_transactions",
            "target_date": "2026-11-19",
            "user_name": "Аналитик 2",
            "initiator": "USER",
        },
    )
    assert dry_resp.status_code == 200
    dry_data = dry_resp.json()
    assert dry_data["prediction"] == "QUEUE"
    assert "src_transactions" in dry_data["reason"]

    # 3. Реальный запуск etl_transactions (пишет в src_transactions 19.11–19.11)
    r2 = await client.post(
        "/api/runs",
        json={
            "process_id": "etl_transactions",
            "target_date": "2026-11-19",
            "user_name": "Аналитик 2",
            "initiator": "USER",
        },
    )
    assert r2.status_code == 200
    d2 = r2.json()
    assert d2["run"]["status"] == "QUEUED"
    assert "src_transactions" in (d2["run"]["blocked_reason"] or "")


@pytest.mark.asyncio
async def test_tc05_user_priority_over_machine(client: AsyncClient):
    """TC-05: Приоритет «Люди vs Машина» (FR-03)

    При активном USER-расчёте вызвать POST /api/demo/trigger-cron (MACHINE).
    Ожидаемый результат: Задача машины получает статус WAITING_TIMER.
    """
    if not await is_db_available():
        pytest.skip("PostgreSQL недоступен. Запустите docker-compose up для интеграционных тестов.")

    # 1. Запуск пользовательской задачи
    r_user = await client.post(
        "/api/runs",
        json={
            "process_id": "credit_risk",
            "target_date": "2026-11-20",
            "user_name": "Аналитик 1",
            "initiator": "USER",
        },
    )
    assert r_user.status_code == 200
    assert r_user.json()["run"]["status"] == "RUNNING"

    # 2. Запуск регламента (MACHINE)
    r_cron = await client.post(
        "/api/demo/trigger-cron",
        json={
            "process_id": "counterparty_risk",
            "target_date": "2026-11-20",
        },
    )
    assert r_cron.status_code == 200
    d_cron = r_cron.json()
    assert d_cron["run"]["status"] == "WAITING_TIMER"
    assert "пользовательских задач" in (d_cron["run"]["blocked_reason"] or "")

    # 3. Дедупликация: пользователь запрашивает ту же задачу, спящую в WAITING_TIMER
    r_prom = await client.post(
        "/api/runs",
        json={
            "process_id": "counterparty_risk",
            "target_date": "2026-11-20",
            "user_name": "Аналитик 2",
            "initiator": "USER",
        },
    )
    assert r_prom.status_code == 200
    d_prom = r_prom.json()
    assert d_prom["action"] == "ATTACHED"
    assert d_prom["run"]["initiator"] == "USER"
    assert d_prom["run"]["status"] == "RUNNING"
    assert "Аналитик 2" in d_prom["run"]["subscribers"]


@pytest.mark.asyncio
async def test_tc06_watchdog_ttl(client: AsyncClient):
    """TC-06: Отказоустойчивость TTL Watchdog (NFR-02)

    Запустить задачу и симулировать сбой воркера (Heartbeat остановлен).
    Ожидаемый результат: Watchdog переводит зависшую задачу в FAILED, освобождает блокировки
    и запускает следующую задачу из очереди.
    """
    if not await is_db_available():
        pytest.skip("PostgreSQL недоступен. Запустите docker-compose up для интеграционных тестов.")

    # Устанавливаем минимальный TTL (5 сек) для быстрого теста
    await client.put("/api/config", json={"lock_ttl_sec": 5})

    # 1. Запускаем задачу, которая захватит ресурсы
    r1 = await client.post(
        "/api/runs",
        json={
            "process_id": "credit_risk",
            "target_date": "2026-11-20",
            "user_name": "Аналитик 1",
            "initiator": "USER",
        },
    )
    assert r1.status_code == 200
    run_id_1 = r1.json()["run"]["run_id"]

    # 2. Ставим конфликтующую задачу в очередь
    r2 = await client.post(
        "/api/runs",
        json={
            "process_id": "etl_transactions",
            "target_date": "2026-11-19",
            "user_name": "Аналитик 2",
            "initiator": "USER",
        },
    )
    assert r2.status_code == 200
    run_id_2 = r2.json()["run"]["run_id"]
    assert r2.json()["run"]["status"] == "QUEUED"

    # 3. Имитируем сбой воркера 1
    r_kill = await client.post(f"/api/demo/kill-worker/{run_id_1}")
    assert r_kill.status_code == 200
    assert r_kill.json()["status"] == "HEARTBEAT_STOPPED"

    # 4. Ожидаем срабатывания Watchdog (в пределах 8 секунд)
    promoted = False
    for _ in range(16):
        await asyncio.sleep(0.5)
        st_resp = await client.get("/api/state")
        snap = st_resp.json()
        failed_run = next((r for r in snap["history"] if r["run_id"] == run_id_1 and r["status"] == "FAILED"), None)
        active_promoted = next((r for r in snap["active_runs"] if r["run_id"] == run_id_2), None)
        if failed_run and active_promoted:
            promoted = True
            break

    assert promoted, "Watchdog должен перевести зависшую задачу в FAILED и запустить ожидающую из очереди"
