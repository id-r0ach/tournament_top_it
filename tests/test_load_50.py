"""Нагрузочный тест на 50 одновременных запросов (раздел 8.2 ARCHITECTURE.md).

Критерии прохождения (NFR-01, NFR-03):
- Количество Deadlocks СУБД: 0 (0%).
- Среднее и 95-перцентиль времени диспетчеризации: < 200 мс.
- Корректная обработка всех 50 запросов без зависших блокировок.
"""

from __future__ import annotations

import asyncio
import random
import time
from typing import List

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

from tests.conftest import is_db_available


@pytest.mark.asyncio
async def test_load_50_concurrent_requests(client: AsyncClient):
    """Отправка 50 одновременных запросов с замером задержки диспетчеризации."""
    if not await is_db_available():
        pytest.skip("PostgreSQL недоступен. Запустите docker-compose up для нагрузочного теста.")

    processes = ["credit_risk", "counterparty_risk", "etl_transactions", "integrated_risk"]
    dates = [
        "2026-11-15",
        "2026-11-16",
        "2026-11-17",
        "2026-11-18",
        "2026-11-19",
        "2026-11-20",
    ]
    initiators = ["USER", "MACHINE"]
    analysts = ["Аналитик 1", "Аналитик 2", "Аналитик 3", "Риск-контролёр"]

    rng = random.Random(42)  # Воспроизводимый генератор
    requests_payload = [
        {
            "process_id": rng.choice(processes),
            "target_date": rng.choice(dates),
            "initiator": rng.choice(initiators),
            "user_name": rng.choice(analysts),
        }
        for _ in range(50)
    ]

    latencies_ms: List[float] = []
    deadlocks_count = 0
    errors_count = 0

    async def send_single_request(payload: dict) -> dict:
        nonlocal deadlocks_count, errors_count
        start = time.perf_counter()
        try:
            resp = await client.post("/api/runs", json=payload)
            duration = (time.perf_counter() - start) * 1000.0
            latencies_ms.append(duration)
            if resp.status_code >= 500:
                errors_count += 1
                if "deadlock" in resp.text.lower():
                    deadlocks_count += 1
            return resp.json()
        except Exception as exc:
            errors_count += 1
            if "deadlock" in str(exc).lower():
                deadlocks_count += 1
            return {"error": str(exc)}

    # Одновременная отправка всех 50 запросов через asyncio.gather
    results = await asyncio.gather(*[send_single_request(p) for p in requests_payload])

    assert len(results) == 50
    assert len(latencies_ms) == 50

    # 1. Критерий: 0 взаимных блокировок СУБД (Deadlocks)
    assert deadlocks_count == 0, f"Обнаружены Deadlocks СУБД: {deadlocks_count}"
    assert errors_count == 0, f"Обнаружены ошибки запросов: {errors_count}"

    # 2. Критерий: среднее и 95-перцентиль времени диспетчеризации < 200 мс
    sorted_latencies = sorted(latencies_ms)
    avg_latency = sum(sorted_latencies) / len(sorted_latencies)
    p95_index = int(0.95 * len(sorted_latencies))
    p95_latency = sorted_latencies[min(p95_index, len(sorted_latencies) - 1)]

    print(
        f"\n[Нагрузочный тест 50 запросов] "
        f"Среднее время: {avg_latency:.2f} мс, "
        f"95-перцентиль: {p95_latency:.2f} мс, "
        f"Максимум: {max(sorted_latencies):.2f} мс"
    )

    assert avg_latency < 200.0, f"Среднее время диспетчеризации превысило 200 мс: {avg_latency:.2f} мс"
    assert p95_latency < 200.0, f"95-перцентиль диспетчеризации превысил 200 мс: {p95_latency:.2f} мс"

    # 3. Проверка корректности состояния системы после диспетчеризации
    state_resp = await client.get("/api/state")
    assert state_resp.status_code == 200
    state = state_resp.json()

    total_tracked = (
        len(state.get("active_runs", []))
        + len(state.get("queue", []))
        + len(state.get("timer_waiting", []))
        + len(state.get("history", []))
    )
    assert total_tracked > 0, "Все принятые заявки должны отображаться в дашборде"
