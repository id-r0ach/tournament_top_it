from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
#  Run creation request sent by the API layer
# ---------------------------------------------------------------------------
class RunCreateRequest(BaseModel):
    """Payload used when creating a new risk run.

    * ``process_id`` – key from :py:data:`config.processes.json`.
    * ``target_date`` – the date for which the run should be performed.
    * ``initiator`` – either ``USER`` or ``MACHINE``.
    * ``user_name`` – name of the requesting user (for UI display).
    """

    process_id: str
    target_date: datetime
    initiator: str = Field(..., pattern=r"^(USER|MACHINE)$")
    user_name: str


# ---------------------------------------------------------------------------
#  Response for dry‑run endpoint – the same information that would be stored
#  in ``runs`` but without actually creating a record.
# ---------------------------------------------------------------------------
class DryRunResponse(BaseModel):
    run_id: str
    status: str
    estimated_duration_sec: int
    required_locks: List["LockInfo"]
    prediction: str
    date_from: datetime
    date_to: datetime
    reason: str


# ---------------------------------------------------------------------------
#  Lock information used by LockManager and API
# ---------------------------------------------------------------------------
class LockInfo(BaseModel):
    resource_id: str
    date_from: datetime
    date_to: datetime
    access_mode: str = Field(..., pattern=r"^(READ|WRITE)$")


# ---------------------------------------------------------------------------
#  Summary of a run – what the API returns after a run is finished.
# ---------------------------------------------------------------------------
class RunInfo(BaseModel):
    run_id: str
    process_id: str
    status: str
    stage: Optional[str]
    progress: int
    started_at: Optional[datetime]
    finished_at: Optional[datetime]


# ---------------------------------------------------------------------------
#  Snapshot of the system state – used by the live‑dashboard endpoint.
# ---------------------------------------------------------------------------
class SystemStateSnapshot(BaseModel):
    active_runs: List[RunInfo]
    queue: List[RunInfo]
    timer_waiting: List[RunInfo]
    active_locks: List[LockInfo]
    history: List[RunInfo]
    audit_logs: List[dict[str, Any]]
    config: dict[str, int]


# ---------------------------------------------------------------------------
#  Payload for dynamic configuration changes via the API.
# ---------------------------------------------------------------------------
class ConfigUpdateRequest(BaseModel):
    machine_timer_interval_sec: Optional[int]
    lock_ttl_sec: Optional[int]
    heartbeat_interval_sec: Optional[int]


__all__ = [
    "RunCreateRequest",
    "DryRunResponse",
    "RunInfo",
    "LockInfo",
    "SystemStateSnapshot",
    "ConfigUpdateRequest",
]
