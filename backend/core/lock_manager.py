"""Composite lock manager.

The lock manager implements the conflict rules from section 6.1 of the
architecture document.  It operates on the ``active_locks`` table defined in
``backend/db/database.py``.

All public methods are coroutine functions that expect an
``asyncpg.Connection`` or ``asyncpg.Pool`` from the database layer.  The
implementation intentionally avoids any application‑specific logic – it
provides only the primitives required by the orchestrator and unit tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List

import asyncpg

from .registry import ResourceLockSpec

__all__ = [
    "check_conflicts",
    "acquire_locks",
    "release_locks",
    "extend_ttl",
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------
def _intersects(a_from: datetime, a_to: datetime, b_from: datetime, b_to: datetime) -> bool:
    """Return ``True`` if two inclusive date ranges overlap."""

    return a_from <= b_to and b_from <= a_to


# ---------------------------------------------------------------------------
# Lock operations
# ---------------------------------------------------------------------------
async def check_conflicts(
    conn: asyncpg.Connection, required_locks: Iterable[ResourceLockSpec]
) -> bool:
    """Return ``True`` if *any* of ``required_locks`` conflicts.

    The function follows the three‑condition rule described in section
    6.1: matching resource, overlapping date ranges and at least one
    non‑READ mode.

    Parameters
    ----------
    conn:
        A live :class:`asyncpg.Connection`.
    required_locks:
        Iterable of lock specifications requested by a new run.
    """

    # Pull all *active* locks that involve a resource of interest.  We
    # filter by ``expires_at`` to avoid stale rows.
    resource_ids = {lock.resource_id for lock in required_locks}
    if not resource_ids:
        return False

    sql = """
    SELECT resource_id, date_from, date_to, access_mode
    FROM active_locks
    WHERE resource_id = ANY($1) AND expires_at > NOW()
    """
    active = await conn.fetch(sql, list(resource_ids))
    if not active:
        return False

    # Convert active rows to simple tuples for quick comparison.
    active_locks = [
        (
            row["resource_id"],
            row["date_from"].date(),
            row["date_to"].date(),
            row["access_mode"],
        )
        for row in active
    ]

    for req in required_locks:
        for act in active_locks:
            if req.resource_id != act[0]:
                continue
            # Check date overlap – inclusive boundaries.
            if not _intersects(req.date_from, req.date_to, act[1], act[2]):
                continue
            # Mode compatibility – only READ + READ is allowed.
            if req.access_mode == "READ" and act[3] == "READ":
                continue
            # Conflict found.
            return True
    return False


async def acquire_locks(
    conn: asyncpg.Connection,
    run_id: str,
    required_locks: Iterable[ResourceLockSpec],
    ttl_sec: int,
    *,
    sync_response_to_workbench: bool = False,
) -> None:
    """Atomically acquire all ``required_locks`` for ``run_id``.

    The caller must call :func:`check_conflicts` first.  The operation is
    wrapped in a transaction to guarantee the *all‑or‑nothing* property.
    ``ttl_sec`` determines the initial ``expires_at`` value.
    """

    # Using a transaction ensures that either all rows are inserted or
    # none are – which matches the lock‑acquisition semantics.
    async with conn.transaction():
        expires_at = datetime.utcnow() + timedelta(seconds=ttl_sec)
        values: List[tuple] = []
        for lock in required_locks:
            values.append(
                (
                    run_id,
                    lock.resource_id,
                    lock.date_from,
                    lock.date_to,
                    lock.access_mode,
                    expires_at,
                )
            )
        if values:
            await conn.executemany(
                """
                INSERT INTO active_locks (run_id, resource_id, date_from, date_to, access_mode, expires_at)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                values,
            )
        # If ``values`` is empty (e.g. a process with no resources) the
        # transaction does nothing – that's acceptable.


async def release_locks(conn: asyncpg.Connection, run_id: str) -> None:
    """Remove all locks held by ``run_id``.

    The operation is idempotent – if no rows match the ``run_id`` the
    statement simply affects zero rows.
    """

    await conn.execute("DELETE FROM active_locks WHERE run_id = $1", run_id)


async def extend_ttl(conn: asyncpg.Connection, run_id: str, ttl_sec: int) -> None:
    """Extend the ``expires_at`` timestamp of all locks for ``run_id``.

    The new expiry is calculated relative to the current UTC time.
    """

    new_expiry = datetime.utcnow() + timedelta(seconds=ttl_sec)
    await conn.execute(
        "UPDATE active_locks SET expires_at = $1 WHERE run_id = $2",
        new_expiry,
        run_id,
    )
