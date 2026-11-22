"""Database layer for the risk orchestrator.

This module provides a small helper around :mod:`asyncpg` that
initialises the database schema, seeds synthetic data and offers a simple
audit logging API.  It is intentionally lightweight – only the parts
required by the project specification are implemented.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timedelta
from typing import List, Optional

import asyncpg

__all__ = [
    "get_pool",
    "init_db",
    "seed_synthetic_data",
    "log_audit_event",
]

# ---------------------------------------------------------------------------
# Connection handling
# ---------------------------------------------------------------------------
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """Return a singleton asyncpg connection pool.

    The connection string is read from the ``DATABASE_URL`` environment
    variable – this follows the Twelve‑Factor App convention and is
    sufficient for local test runs (``postgresql://postgres:postgres@localhost:5432/postgres``).
    """

    global _pool
    if _pool is None:
        conn_str = os.getenv(
            "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"
        )
        _pool = await asyncpg.create_pool(dsn=conn_str, min_size=1, max_size=10)
    return _pool


async def close_pool() -> None:
    """Close the global connection pool – useful for tests."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


# ---------------------------------------------------------------------------
# Schema definition (DDL) – copied verbatim from ARCHITECTURE.md 5.2
# ---------------------------------------------------------------------------
DDL_STEPS: List[str] = [
    "-- 1. Реестр всех запусков (активных, ждущих и завершённых)",
    "CREATE TABLE IF NOT EXISTS runs (",
    "    run_id          UUID PRIMARY KEY,",
    "    process_id      VARCHAR(64) NOT NULL,",
    "    process_name    VARCHAR(128) NOT NULL,",
    "    target_date     DATE NOT NULL,",
    "    date_from       DATE NOT NULL,          -- target_date - window_days",
    "    date_to         DATE NOT NULL,          -- target_date",
    "    initiator       VARCHAR(16) NOT NULL,   -- 'USER' | 'MACHINE'",
    "    user_name       VARCHAR(64) NOT NULL,",
    "    status          VARCHAR(32) NOT NULL,   -- 'WAITING_TIMER' | 'QUEUED' | 'RUNNING' | 'COMPLETED' | 'FAILED' | 'CANCELLED'",
    "    stage           VARCHAR(32),            -- 'EXTRACT' | 'STAGING' | 'COMPUTE' | 'LOAD',",
    "    progress        INT NOT NULL DEFAULT 0, -- 0..100",
    "    subscribers     JSONB NOT NULL DEFAULT '[]'::jsonb,",
    "    blocked_reason  TEXT,                   -- Человекочитаемая причина ожидания для UI (FR-14)",
    "    lock_token      UUID,",
    "    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),",
    "    started_at      TIMESTAMPTZ,",
    "    finished_at     TIMESTAMPTZ",
    ");",
    "",
    "CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);",
    "CREATE INDEX IF NOT EXISTS idx_runs_dedup ON runs(process_id, target_date) WHERE status IN ('WAITING_TIMER', 'QUEUED', 'RUNNING');",
    "",
    "-- 2. Таблица активных блокировок с поддержкой TTL (NFR-02)",
    "CREATE TABLE IF NOT EXISTS active_locks (",
    "    lock_id         SERIAL PRIMARY KEY,",
    "    run_id          UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,",
    "    resource_id     VARCHAR(64) NOT NULL,",
    "    date_from       DATE NOT NULL,",
    "    date_to         DATE NOT NULL,",
    "    access_mode     VARCHAR(8) NOT NULL,    -- 'READ' | 'WRITE',",
    "    expires_at      TIMESTAMPTZ NOT NULL",
    ");",
    "",
    "CREATE INDEX IF NOT EXISTS idx_active_locks_resource ON active_locks(resource_id);",
    "",
    "-- 3. Структурированный журнал событий для аудита и ИИ-анализа (NFR-06)",
    "CREATE TABLE IF NOT EXISTS audit_logs (",
    "    id              SERIAL PRIMARY KEY,",
    "    run_id          UUID,",
    "    event_type      VARCHAR(64) NOT NULL,   -- 'CREATED' | 'DEDUPLICATED' | 'TIMER_WAIT' | 'QUEUED' | 'STARTED' | 'COMPLETED' | 'FAILED' | 'CANCELLED' | 'TTL_EXPIRED',",
    "    message         TEXT NOT NULL,",
    "    details         JSONB NOT NULL DEFAULT '{}'::jsonb,",
    "    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()",
    ");",
    "",
    "-- 4. Синтетические предметные таблицы для работы воркеров (FR-11..FR-13)",
    "CREATE TABLE IF NOT EXISTS ref_clients (",
    "    client_id       INT PRIMARY KEY,",
    "    segment         VARCHAR(32) NOT NULL,",
    "    pd_base         NUMERIC(6, 4) NOT NULL",
    ");",
    "",
    "CREATE TABLE IF NOT EXISTS src_transactions (",
    "    tx_id           SERIAL PRIMARY KEY,",
    "    tx_date         DATE NOT NULL,",
    "    client_id       INT NOT NULL REFERENCES ref_clients(client_id),",
    "    amount          NUMERIC(14, 2) NOT NULL",
    ");",
    "",
    "CREATE INDEX IF NOT EXISTS idx_src_tx_date ON src_transactions(tx_date);",
    "",
    "CREATE TABLE IF NOT EXISTS mart_risk_results (",
    "    process_id      VARCHAR(64) NOT NULL,",
    "    target_date     DATE NOT NULL,",
    "    total_exposure  NUMERIC(16, 2) NOT NULL,",
    "    weighted_risk   NUMERIC(10, 4) NOT NULL,",
    "    rows_processed  INT NOT NULL,",
    "    last_run_id     UUID NOT NULL,",
    "    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),",
    "    PRIMARY KEY (process_id, target_date)",
    ");",
]


async def init_db() -> None:
    """Execute the DDL steps and create the database schema.

    The function is idempotent – ``CREATE TABLE IF NOT EXISTS`` ensures that a subsequent
    call does not error out if the tables already exist.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        # DDL_STEPS contains one line per element. Execute statements only when
        # a line containing a semicolon is reached, which indicates the end of a
        # SQL statement. This avoids running incomplete statements such as
        # "CREATE TABLE IF NOT EXISTS runs (".
        buffer_lines: list[str] = []
        for line in DDL_STEPS:
            # Skip full-line comments
            if line.strip().startswith("--"):
                continue
            buffer_lines.append(line)
            # When a line contains a semicolon, we have a complete statement.
            if ";" in line:
                stmt = "\n".join(buffer_lines).strip()
                if stmt:
                    await conn.execute(stmt)
                buffer_lines.clear()


# ---------------------------------------------------------------------------
# Synthetic data seeding
# ---------------------------------------------------------------------------
async def seed_synthetic_data() -> None:
    """Populate ``ref_clients`` and ``src_transactions`` with synthetic rows.

    * 50 clients with random segments and PD values.
    * 1 500 transactions over the last 30 days.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        client_count = await conn.fetchval("SELECT COUNT(*) FROM ref_clients")
        if client_count and client_count > 0:
            return
        # Create 50 clients
        clients = [
            {
                "client_id": i + 1,
                "segment": "A" if i % 5 == 0 else "B",
                "pd_base": round(0.01 + (i % 10) * 0.001, 4),
            }
            for i in range(50)
        ]
        await conn.executemany(
            "INSERT INTO ref_clients (client_id, segment, pd_base) VALUES ($1, $2, $3)",
            [(c["client_id"], c["segment"], c["pd_base"]) for c in clients],
        )
        # Generate 1500 transactions for last 30 days
        today = datetime.utcnow().date()
        txs = []
        for _ in range(1500):
            tx_date = today - timedelta(days=int(os.urandom(1)[0] % 30))
            client_id = int(os.urandom(1)[0] % 50) + 1
            amount = round(
                (os.urandom(1)[0] + 1) * 1000 + (os.urandom(1)[0] % 1000) / 100, 2
            )
            txs.append((tx_date, client_id, amount))
        await conn.executemany(
            "INSERT INTO src_transactions (tx_date, client_id, amount) VALUES ($1, $2, $3)",
            txs,
        )


# ---------------------------------------------------------------------------
# Audit logging
# ---------------------------------------------------------------------------
async def log_audit_event(
    run_id: Optional[str],
    event_type: str,
    message: str,
    details: Optional[dict] = None,
):
    """Insert a row into :table:`audit_logs`.

    Parameters
    ----------
    run_id:
        UUID of the run or ``None`` for global events.
    event_type:
        One of the predefined types from the architecture spec.
    message:
        Human‑readable text.
    details:
        Optional JSON payload.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO audit_logs (run_id, event_type, message, details) VALUES ($1, $2, $3, $4)",
            run_id,
            event_type,
            message,
            details or {},
        )
