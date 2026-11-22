"""Worker module – исполняет расчётные задачи.

Контролирует Heartbeat, обновляет статус и делает SQL‑операции.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any
from uuid import UUID

from backend.core import lock_manager
from backend.db import database

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Chaos‑тест – имитация краха воркера.
# ---------------------------------------------------------------------------
CRASHED_WORKERS: set[UUID] = set()


def kill_worker(run_id: UUID) -> None:
    """Добавляет run_id в набор CRASHED_WORKERS."""
    CRASHED_WORKERS.add(run_id)


async def _heartbeat_loop(run_id: UUID, interval_sec: int, ttl_sec: int) -> None:
    """Периодически продлевает TTL блокировок.
    Если run_id попал в CRASHED_WORKERS – завершаем цикл."""
    pool = await database.get_pool()
    async with pool.acquire() as conn:
        while run_id not in CRASHED_WORKERS:
            # Проверяем до и после сна – имитация проверки флага.
            if run_id in CRASHED_WORKERS:
                break
            await asyncio.sleep(interval_sec)
            if run_id in CRASHED_WORKERS:
                break
            try:
                await lock_manager.extend_ttl(conn, str(run_id), ttl_sec)
            except Exception as exc:
                logger.warning("Heartbeat error for %s: %s", run_id, exc)


async def update_stage(run_id: UUID, stage: str, progress: int, orchestrator: Any = None) -> None:
    """Обновляет stage и progress в runs и рассылает состояние."""
    pool = await database.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE runs SET stage=$1, progress=$2 WHERE run_id=$3 AND status='RUNNING'",
            stage,
            progress,
            str(run_id),
        )
    if orchestrator is not None:
        await orchestrator._broadcast_state_safe()


async def execute_risk_job(run_id: UUID, orchestrator: Any, ws_manager: Any = None) -> None:
    """Выполняет расчёт по заданному run_id.
    Читает конфигурацию, запускает Heartbeat и поочерёдно выполняет 4 этапа.
    """
    # -----------------------------------------------------------------------
    # 1. Загружаем метаданные запуска и конфигурацию процесса.
    # -----------------------------------------------------------------------
    pool = await database.get_pool()
    async with pool.acquire() as conn:
        run_row = await conn.fetchrow(
            "SELECT process_id, target_date, date_from, date_to FROM runs WHERE run_id=$1",
            str(run_id),
        )
        if run_row is None:
            logger.error("Run %s not found", run_id)
            return
        process_id = run_row["process_id"]
        target_date = run_row["target_date"]
        date_from = run_row["date_from"]
        date_to = run_row["date_to"]
        # Конфиг процесса
        config_path: Path = orchestrator._config_path
        with config_path.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
        process_cfg = cfg.get("processes", {}).get(process_id)
        if not process_cfg:
            logger.error("Process config not found for %s", process_id)
            return
        duration_sec = float(process_cfg.get("duration_sec", 8))
        step_delay = max(0.2, duration_sec / 4.0)
        # Запускаем Heartbeat
        asyncio.create_task(
            _heartbeat_loop(
                run_id,
                orchestrator._heartbeat_interval_sec,
                orchestrator._lock_ttl_sec,
            )
        )
        # -------------------------------------------------------------------
        # 2. 4 этапа расчёта
        # -------------------------------------------------------------------
        # 2.1 EXTRACT – 20%
        try:
            async with conn.transaction(isolation="repeatable_read"):
                rows = await conn.fetch(
                    "SELECT t.tx_date, t.amount, c.pd_base "
                    "FROM src_transactions t JOIN ref_clients c ON t.client_id = c.client_id "
                    "WHERE t.tx_date BETWEEN $1 AND $2",
                    date_from,
                    date_to,
                )
            if run_id in CRASHED_WORKERS:
                await asyncio.sleep(60)
                return
            await update_stage(run_id, "EXTRACT", 20, orchestrator)
            await asyncio.sleep(step_delay)
            # ----------------------------------------------------------------
            # 2.2 STAGING – 50%
            temp_table = f"temp_risk_{run_id.hex}"
            await conn.execute(
                f"CREATE TABLE IF NOT EXISTS {temp_table} (amount NUMERIC(14,2), weighted NUMERIC(16,4))"
            )
            # Пересчитываем weighted = amount * pd_base
            data = [(r["amount"], float(r["amount"]) * float(r["pd_base"])) for r in rows]
            await conn.executemany(
                f"INSERT INTO {temp_table} (amount, weighted) VALUES ($1, $2)",
                data,
            )
            if run_id in CRASHED_WORKERS:
                await asyncio.sleep(60)
                return
            await update_stage(run_id, "STAGING", 50, orchestrator)
            await asyncio.sleep(step_delay)
            # ----------------------------------------------------------------
            # 2.3 COMPUTE – 80%
            compute_row = await conn.fetchrow(
                f"SELECT COUNT(*)::INT AS rows_processed, COALESCE(SUM(amount),0) AS total_exposure, COALESCE(SUM(weighted),0) AS weighted_risk FROM {temp_table}"
            )
            if run_id in CRASHED_WORKERS:
                await asyncio.sleep(60)
                return
            await update_stage(run_id, "COMPUTE", 80, orchestrator)
            await asyncio.sleep(step_delay)
            # ----------------------------------------------------------------
            # 2.4 LOAD – 95%
            await conn.execute(
                "INSERT INTO mart_risk_results (process_id, target_date, total_exposure, weighted_risk, rows_processed, last_run_id) "
                "VALUES ($1, $2, $3, $4, $5, $6) "
                "ON CONFLICT (process_id, target_date) DO UPDATE "
                "SET total_exposure=EXCLUDED.total_exposure, "
                "weighted_risk=EXCLUDED.weighted_risk, "
                "rows_processed=EXCLUDED.rows_processed, "
                "last_run_id=EXCLUDED.last_run_id, "
                "updated_at=NOW()",
                process_id,
                target_date,
                compute_row["total_exposure"],
                compute_row["weighted_risk"],
                compute_row["rows_processed"],
                str(run_id),
            )
            if run_id in CRASHED_WORKERS:
                await asyncio.sleep(60)
                return
            await update_stage(run_id, "LOAD", 95, orchestrator)
            await asyncio.sleep(step_delay)
        except Exception as exc:
            logger.exception("Error during risk job %s: %s", run_id, exc)
            await orchestrator.complete_run(run_id, status="FAILED", error=str(exc))
            return
        # Завершили успешно
        await orchestrator.complete_run(run_id, status="COMPLETED")
