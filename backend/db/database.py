"""Слой работы с PostgreSQL через asyncpg: пул, DDL, сидирование и аудит."""

from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, List, Optional, Union
from uuid import UUID

import asyncpg

__all__ = [
    "get_pool",
    "close_pool",
    "init_db",
    "seed_synthetic_data",
    "log_audit_event",
]

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    """Возвращает синглтон-пул соединений asyncpg, привязанный к текущему event loop."""
    global _pool
    current_loop = asyncio.get_running_loop()
    if _pool is None or getattr(_pool, "_loop", None) is not current_loop or getattr(_pool, "_closed", False):
        if _pool is not None and not getattr(_pool, "_closed", False):
            try:
                await _pool.close()
            except Exception:
                pass
        conn_str = os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:password@localhost:5432/risk_orchestrator",
        )
        _pool = await asyncpg.create_pool(
            dsn=conn_str,
            min_size=10,
            max_size=30,
            command_timeout=10.0,
            timeout=5.0,
        )
    return _pool


async def close_pool() -> None:
    """Закрывает глобальный пул соединений."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


DDL_STATEMENTS: List[str] = [
    """
    CREATE TABLE IF NOT EXISTS runs (
        run_id          UUID PRIMARY KEY,
        process_id      VARCHAR(64) NOT NULL,
        process_name    VARCHAR(128) NOT NULL,
        target_date     DATE NOT NULL,
        date_from       DATE NOT NULL,
        date_to         DATE NOT NULL,
        initiator       VARCHAR(16) NOT NULL,
        user_name       VARCHAR(64) NOT NULL,
        status          VARCHAR(32) NOT NULL,
        stage           VARCHAR(32),
        progress        INT NOT NULL DEFAULT 0,
        subscribers     JSONB NOT NULL DEFAULT '[]'::jsonb,
        blocked_reason  TEXT,
        lock_token      UUID,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        started_at      TIMESTAMPTZ,
        finished_at     TIMESTAMPTZ
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);",
    """
    CREATE INDEX IF NOT EXISTS idx_runs_dedup
    ON runs(process_id, target_date)
    WHERE status IN ('WAITING_TIMER', 'QUEUED', 'RUNNING');
    """,
    "CREATE INDEX IF NOT EXISTS idx_runs_initiator_status ON runs(initiator, status);",
    """
    CREATE TABLE IF NOT EXISTS active_locks (
        lock_id         SERIAL PRIMARY KEY,
        run_id          UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
        resource_id     VARCHAR(64) NOT NULL,
        date_from       DATE NOT NULL,
        date_to         DATE NOT NULL,
        access_mode     VARCHAR(8) NOT NULL,
        expires_at      TIMESTAMPTZ NOT NULL
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_active_locks_resource ON active_locks(resource_id);",
    "CREATE INDEX IF NOT EXISTS idx_active_locks_res_exp ON active_locks(resource_id, expires_at);",
    """
    CREATE TABLE IF NOT EXISTS audit_logs (
        id              SERIAL PRIMARY KEY,
        run_id          UUID,
        event_type      VARCHAR(64) NOT NULL,
        message         TEXT NOT NULL,
        details         JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ref_clients (
        client_id       INT PRIMARY KEY,
        segment         VARCHAR(32) NOT NULL,
        pd_base         NUMERIC(6, 4) NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS src_transactions (
        tx_id           SERIAL PRIMARY KEY,
        tx_date         DATE NOT NULL,
        client_id       INT NOT NULL REFERENCES ref_clients(client_id),
        amount          NUMERIC(14, 2) NOT NULL
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_src_tx_date ON src_transactions(tx_date);",
    """
    CREATE TABLE IF NOT EXISTS mart_risk_results (
        process_id      VARCHAR(64) NOT NULL,
        target_date     DATE NOT NULL,
        total_exposure  NUMERIC(18, 2) NOT NULL,
        weighted_risk   NUMERIC(18, 4) NOT NULL,
        rows_processed  INT NOT NULL,
        last_run_id     UUID NOT NULL,
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (process_id, target_date)
    );
    """,
    "ALTER TABLE IF EXISTS mart_risk_results ALTER COLUMN total_exposure TYPE NUMERIC(18, 2);",
    "ALTER TABLE IF EXISTS mart_risk_results ALTER COLUMN weighted_risk TYPE NUMERIC(18, 4);",
]


async def init_db() -> None:
    """Создаёт схему БД (идемпотентно)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        for stmt in DDL_STATEMENTS:
            sql = stmt.strip()
            if sql:
                await conn.execute(sql)


async def seed_synthetic_data() -> None:
    """Наполняет ref_clients (50 клиентов) и src_transactions (1500+ транзакций)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        client_count = await conn.fetchval("SELECT COUNT(*) FROM ref_clients")
        if not client_count or client_count == 0:
            clients = [
                (i + 1, "A" if i % 5 == 0 else "B", round(0.01 + (i % 10) * 0.001, 4))
                for i in range(50)
            ]
            await conn.executemany(
                "INSERT INTO ref_clients (client_id, segment, pd_base) VALUES ($1, $2, $3)",
                clients,
            )

        tx_count = await conn.fetchval("SELECT COUNT(*) FROM src_transactions")
        if not tx_count or tx_count == 0:
            today = datetime.now(timezone.utc).date()
            base_dates = [today - timedelta(days=d) for d in range(31)]
            nov_dates = [date(2026, 11, d) for d in range(1, 31)]
            date_pool = sorted(list(set(base_dates + nov_dates)))

            txs = []
            for i in range(1500):
                tx_date = date_pool[i % len(date_pool)]
                client_id = (i % 50) + 1
                amount = round(1000.0 + ((i * 37) % 50000) + ((i % 100) / 100.0), 2)
                txs.append((tx_date, client_id, amount))

            await conn.executemany(
                "INSERT INTO src_transactions (tx_date, client_id, amount) VALUES ($1, $2, $3)",
                txs,
            )


async def log_audit_event(
    run_id: Optional[Union[UUID, str]],
    event_type: str,
    message: str,
    details: Optional[Union[dict[str, Any], str]] = None,
    *,
    conn: Optional[asyncpg.Connection] = None,
) -> None:
    """Записывает структурированное событие в таблицу audit_logs."""
    coerced_run_id: Optional[UUID] = None
    if run_id is not None:
        if isinstance(run_id, UUID):
            coerced_run_id = run_id
        else:
            try:
                coerced_run_id = UUID(str(run_id))
            except (ValueError, TypeError):
                coerced_run_id = None

    if isinstance(details, str):
        details_json = details
    else:
        details_json = json.dumps(details or {}, ensure_ascii=False)

    sql = (
        "INSERT INTO audit_logs (run_id, event_type, message, details) "
        "VALUES ($1, $2, $3, $4::jsonb)"
    )
    if conn is not None:
        await conn.execute(sql, coerced_run_id, event_type, message, details_json)
        return

    pool = await get_pool()
    async with pool.acquire() as acquired_conn:
        await acquired_conn.execute(sql, coerced_run_id, event_type, message, details_json)