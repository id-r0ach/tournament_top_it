"""Pydantic-модели запросов, ответов и снимков состояния оркестратора."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class RunCreateRequest(BaseModel):
    """Запрос на запуск расчёта или предпроверку (dry-run)."""

    process_id: str
    target_date: date
    initiator: str = Field(default="USER", pattern=r"^(USER|MACHINE)$")
    user_name: str = Field(default="Аналитик 1")

    @field_validator("initiator", mode="before")
    @classmethod
    def _normalize_initiator(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().upper()
        return value


class LockInfo(BaseModel):
    """Информация об активной или требуемой композитной блокировке."""

    lock_id: Optional[int] = None
    run_id: Optional[str] = None
    resource_id: str
    date_from: date
    date_to: date
    access_mode: str = Field(..., pattern=r"^(READ|WRITE)$")
    expires_at: Optional[datetime] = None

    @field_validator("run_id", mode="before")
    @classmethod
    def _coerce_run_id(cls, value: Any) -> Optional[str]:
        if isinstance(value, UUID):
            return str(value)
        return str(value) if value is not None else None

    @field_validator("access_mode", mode="before")
    @classmethod
    def _normalize_mode(cls, value: Any) -> Any:
        if isinstance(value, str):
            return value.strip().upper()
        return value


class DryRunResponse(BaseModel):
    """Результат предпроверки конфликтов без создания задачи (FR-15)."""

    prediction: str
    date_from: date
    date_to: date
    reason: str
    required_locks: List[LockInfo] = Field(default_factory=list)


class RunInfo(BaseModel):
    """Полное описание запуска для UI и API (соответствует DDL таблицы runs)."""

    run_id: str
    process_id: str
    process_name: str = ""
    target_date: Optional[date] = None
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    initiator: str = "USER"
    user_name: str = ""
    status: str
    stage: Optional[str] = None
    progress: int = 0
    subscribers: List[str] = Field(default_factory=list)
    blocked_reason: Optional[str] = None
    lock_token: Optional[str] = None
    position: Optional[int] = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    @field_validator("run_id", "lock_token", mode="before")
    @classmethod
    def _coerce_uuid(cls, value: Any) -> Optional[str]:
        if isinstance(value, UUID):
            return str(value)
        return str(value) if value is not None else None


class AuditLogEntry(BaseModel):
    """Запись в структурированном журнале audit_logs (NFR-06)."""

    id: Optional[int] = None
    run_id: Optional[str] = None
    event_type: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[Union[datetime, str]] = None

    @field_validator("run_id", mode="before")
    @classmethod
    def _coerce_run_id(cls, value: Any) -> Optional[str]:
        if isinstance(value, UUID):
            return str(value)
        return str(value) if value is not None else None


class SystemStateSnapshot(BaseModel):
    """Снимок состояния системы для REST и WebSocket."""

    active_runs: List[RunInfo]
    queue: List[RunInfo]
    timer_waiting: List[RunInfo]
    active_locks: List[LockInfo]
    history: List[RunInfo]
    audit_logs: List[Union[AuditLogEntry, dict[str, Any]]]
    config: dict[str, Any]


class ConfigUpdateRequest(BaseModel):
    """Динамическое обновление таймингов оркестратора."""

    machine_timer_interval_sec: Optional[int] = Field(default=None, ge=1)
    lock_ttl_sec: Optional[int] = Field(default=None, ge=5)
    heartbeat_interval_sec: Optional[int] = Field(default=None, ge=1)


__all__ = [
    "RunCreateRequest",
    "DryRunResponse",
    "RunInfo",
    "LockInfo",
    "AuditLogEntry",
    "SystemStateSnapshot",
    "ConfigUpdateRequest",
]