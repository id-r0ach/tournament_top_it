"""Вычислительный воркер: 4 этапа расчёта в PostgreSQL, Heartbeat и имитация сбоя."""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Union
from uuid import UUID

from backend.core import lock_manager
from backend.db import database

logger = logging.getLogger(__name__)

CRASHED_WORKERS: set[Union[UUID, str]] = set()


def kill_worker(run_id: Union[UUID, str]) -> None:
    """Помечает воркер как упавший: останавливает Heartbeat и имитирует зависание (NFR-02)."""
    CRASHED_WORKERS.add(run_id)
    if isinstance(run_id, str):
        try:
            CRASHED_WORKERS.add(UUID(run_id))
        except (ValueError, TypeError):
            pass


def _is_crashed(run_id: Union[UUID, str]) -> bool:
    if run_id in CRASHED_WORKERS:
        return True
    try:
        run_uuid = UUID(str(run_id))
        return run_uuid in CRASHED_WORKERS or str(run_uuid) in CRASHED_WORKERS
    except (ValueError, TypeError):
        return False


async def _sleep_checking_crash(run_id: Union[UUID, str], seconds: float) -> bool:
    elapsed = 0.0
    step = 0.25
    while elapsed < seconds:
        if _is_crashed(run_id):
            return True
        chunk = min(step, seconds - elapsed)
        await asyncio.sleep(chunk)
        elapsed += chunk
    return _is_crashed(run_id)


async def _heartbeat_loop(run_id: Union[UUID, str], orchestrator: Any) -> None:
    try:
        while not _is_crashed(run_id):
            interval = max(1, int(orchestrator._heartbeat_interval_sec))
            if await _sleep_checking_crash(run_id, float(interval)):
                logger.info("Heartbeat остановлен для упавшего воркера %s", run_id)
                break

            ttl = max(5, int(orchestrator._lock_ttl_sec))
            try:
                pool = await database.get_pool()
                async with pool.acquire() as conn:
                    await lock_manager.extend_ttl(conn, run_id, ttl)
            except Exception as exc:
                logger.warning("Ошибка Heartbeat для %s: %s", run_id, exc)
    except asyncio.CancelledError:
        return


async def update_stage(
    run_id: UUID,
    stage: str,
    progress: int,
    orchestrator: Any = None,
) -> None:
    pool = await database.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE runs SET stage = $1, progress = $2 WHERE run_id = $3 AND status = 'RUNNING'",
            stage,
            progress,
            run_id,
        )
    if orchestrator is not None:
        await orchestrator._broadcast_state_safe()


async def execute_risk_job(run_id: UUID, orchestrator: Any, ws_manager: Any = None) -> None:
    """Выполняет изолированный расчёт: EXTRACT -> STAGING -> COMPUTE -> LOAD."""
    await asyncio.sleep(0.05)
    pool = await database.get_pool()
    async with pool.acquire() as conn:
        run_row = await conn.fetchrow(
            "SELECT process_id, target_date, date_from, date_to FROM runs WHERE run_id = $1",
            run_id,
        )

    if run_row is None:
        logger.error("Запуск %s не найден в БД", run_id)
        return

    process_id = run_row["process_id"]
    target_date = run_row["target_date"]
    date_from = run_row["date_from"]
    date_to = run_row["date_to"]

    run_uuid = UUID(str(run_id)) if isinstance(run_id, str) else run_id

    cfg = getattr(orchestrator, "_config", None)
    if not cfg:
        config_path = getattr(orchestrator, "_config_path", None)
        if config_path and Path(config_path).is_file():
            with Path(config_path).open("r", encoding="utf-8") as f:
                cfg = json.load(f)
        else:
            cfg = {}

    process_cfg = cfg.get("processes", {}).get(process_id)
    if not process_cfg:
        logger.error("Конфигурация процесса %s не найдена", process_id)
        await orchestrator.complete_run(
            run_id, status="FAILED", error=f"Не найдена конфигурация для {process_id}"
        )
        return

    duration_sec = float(process_cfg.get("duration_sec", 8))
    step_delay = max(0.2, duration_sec / 4.0)
    temp_table = f"temp_risk_{run_uuid.hex}"

    heartbeat_task = asyncio.create_task(_heartbeat_loop(run_id, orchestrator))
    crashed = False

    try:
        if _is_crashed(run_id):
            crashed = True
            return
        async with pool.acquire() as conn:
            async with conn.transaction(isolation="repeatable_read"):
                rows = await conn.fetch(
                    """
                    SELECT t.tx_date, t.amount, c.pd_base
                    FROM src_transactions t
                    JOIN ref_clients c ON t.client_id = c.client_id
                    WHERE t.tx_date BETWEEN $1 AND $2
                    """,
                    date_from,
                    date_to,
                )
        await update_stage(run_id, "EXTRACT", 20, orchestrator)
        if await _sleep_checking_crash(run_id, step_delay):
            crashed = True
            return

        data = [(r["amount"], r["amount"] * r["pd_base"]) for r in rows]
        async with pool.acquire() as conn:
            await conn.execute(
                f"CREATE TABLE IF NOT EXISTS {temp_table} "
                f"(amount NUMERIC(14, 2), weighted NUMERIC(18, 4))"
            )
            if data:
                await conn.executemany(
                    f"INSERT INTO {temp_table} (amount, weighted) VALUES ($1, $2)",
                    data,
                )
        await update_stage(run_id, "STAGING", 50, orchestrator)
        if await _sleep_checking_crash(run_id, step_delay):
            crashed = True
            return

        async with pool.acquire() as conn:
            compute_row = await conn.fetchrow(
                f"""
                SELECT
                    COUNT(*)::INT AS rows_processed,
                    COALESCE(SUM(amount), 0) AS total_exposure,
                    COALESCE(SUM(weighted), 0) AS weighted_risk
                FROM {temp_table}
                """
            )
        await update_stage(run_id, "COMPUTE", 80, orchestrator)
        if await _sleep_checking_crash(run_id, step_delay):
            crashed = True
            return

        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO mart_risk_results (
                    process_id, target_date, total_exposure,
                    weighted_risk, rows_processed, last_run_id, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, NOW())
                ON CONFLICT (process_id, target_date) DO UPDATE SET
                    total_exposure = EXCLUDED.total_exposure,
                    weighted_risk = EXCLUDED.weighted_risk,
                    rows_processed = EXCLUDED.rows_processed,
                    last_run_id = EXCLUDED.last_run_id,
                    updated_at = NOW()
                """,
                process_id,
                target_date,
                compute_row["total_exposure"],
                compute_row["weighted_risk"],
                compute_row["rows_processed"],
                run_id,
            )
        await update_stage(run_id, "LOAD", 95, orchestrator)
        if await _sleep_checking_crash(run_id, step_delay):
            crashed = True
            return

        if _is_crashed(run_id):
            crashed = True
            return

        await orchestrator.complete_run(run_id, status="COMPLETED")
    except Exception as exc:
        if not crashed and not _is_crashed(run_id):
            logger.exception("Ошибка выполнения воркера %s: %s", run_id, exc)
            await orchestrator.complete_run(run_id, status="FAILED", error=str(exc))
    finally:
        try:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
        except Exception:
            pass
        CRASHED_WORKERS.discard(run_id)
        CRASHED_WORKERS.discard(run_uuid)
        CRASHED_WORKERS.discard(str(run_id))
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"DROP TABLE IF EXISTS {temp_table}")
        except Exception:
            pass