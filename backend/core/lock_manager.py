"""Композитный Lock-менеджер: проверка пересечений (Resource + Date_Range + Mode) и TTL."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, List, Optional, Union
from uuid import UUID

import asyncpg

from .registry import ResourceLockSpec

__all__ = [
    "check_conflicts",
    "acquire_locks",
    "release_locks",
    "extend_ttl",
]


def _to_date(value: Union[date, datetime]) -> date:
    """Нормализует date/datetime к date."""
    return value.date() if isinstance(value, datetime) else value


def _to_uuid(value: Union[UUID, str]) -> UUID:
    """Нормализует UUID/str к объекту UUID."""
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _intersects(a_from: date, a_to: date, b_from: date, b_to: date) -> bool:
    """Возвращает True, если два включительных интервала дат пересекаются (FR-06)."""
    return a_from <= b_to and b_from <= a_to


async def check_conflicts(
    conn: asyncpg.Connection,
    required_locks: Iterable[ResourceLockSpec],
    exclude_run_id: Optional[Union[UUID, str]] = None,
) -> List[dict[str, Any]]:
    """Проверяет конфликты с активными блокировками (раздел 6.1 SDD)."""
    req_list = list(required_locks)
    resource_ids = {lock.resource_id for lock in req_list}
    if not resource_ids:
        return []

    sql = """
    SELECT run_id, resource_id, date_from, date_to, access_mode
    FROM active_locks
    WHERE resource_id = ANY($1)
      AND expires_at > NOW()
    """
    active = await conn.fetch(sql, list(resource_ids))
    if not active:
        return []

    exclude_uuid = _to_uuid(exclude_run_id) if exclude_run_id is not None else None

    conflicts: List[dict[str, Any]] = []
    for req in req_list:
        req_from = _to_date(req.date_from)
        req_to = _to_date(req.date_to)
        req_mode = req.access_mode.upper()
        for row in active:
            if exclude_uuid is not None and row["run_id"] == exclude_uuid:
                continue
            if req.resource_id != row["resource_id"]:
                continue
            act_from = _to_date(row["date_from"])
            act_to = _to_date(row["date_to"])
            act_mode = str(row["access_mode"]).upper()
            if not _intersects(req_from, req_to, act_from, act_to):
                continue
            # Правило несовместимости: параллельно разрешено только READ + READ (FR-07)
            if req_mode == "READ" and act_mode == "READ":
                continue

            conflicts.append(
                {
                    "run_id": str(row["run_id"]),
                    "resource_id": row["resource_id"],
                    "date_from": act_from,
                    "date_to": act_to,
                    "req_mode": req_mode,
                    "act_mode": act_mode,
                }
            )
    return conflicts


async def acquire_locks(
    conn: asyncpg.Connection,
    run_id: Union[UUID, str],
    required_locks: Iterable[ResourceLockSpec],
    ttl_sec: int,
) -> None:
    """Атомарно резервирует все требуемые блокировки для run_id в одной транзакции (FR-09)."""
    run_uuid = _to_uuid(run_id)
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_sec)
    values = [
        (
            run_uuid,
            lock.resource_id,
            _to_date(lock.date_from),
            _to_date(lock.date_to),
            lock.access_mode.upper(),
            expires_at,
        )
        for lock in required_locks
    ]
    if values:
        params: list[Any] = []
        placeholders: list[str] = []
        for i, row in enumerate(values):
            offset = i * 6
            placeholders.append(
                f"(${offset+1}, ${offset+2}, ${offset+3}, ${offset+4}, ${offset+5}, ${offset+6})"
            )
            params.extend(row)
        sql = (
            "INSERT INTO active_locks (run_id, resource_id, date_from, date_to, access_mode, expires_at) VALUES "
            + ", ".join(placeholders)
        )
        await conn.execute(sql, *params)


async def release_locks(conn: asyncpg.Connection, run_id: Union[UUID, str]) -> None:
    """Идемпотентно освобождает все блокировки, удерживаемые запуском run_id."""
    run_uuid = _to_uuid(run_id)
    await conn.execute("DELETE FROM active_locks WHERE run_id = $1", run_uuid)


async def extend_ttl(
    conn: asyncpg.Connection,
    run_id: Union[UUID, str],
    ttl_sec: int,
) -> None:
    """Продлевает срок действия (TTL) всех активных блокировок запуска run_id."""
    run_uuid = _to_uuid(run_id)
    new_expiry = datetime.now(timezone.utc) + timedelta(seconds=ttl_sec)
    await conn.execute(
        "UPDATE active_locks SET expires_at = $1 WHERE run_id = $2",
        new_expiry,
        run_uuid,
    )