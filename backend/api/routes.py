"""REST и WebSocket маршруты для API оркестратора."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import UUID

from fastapi import APIRouter, Body, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError

from backend.api.ws import ConnectionManager
from backend.core.orchestrator import Orchestrator
from backend.core.worker import execute_risk_job, kill_worker
from backend.db import database
from backend.db.models import (
    ConfigUpdateRequest,
    RunCreateRequest,
    SystemStateSnapshot,
)

logger = logging.getLogger(__name__)

# Единые экземпляры на всё приложение (импортируются также в backend/main.py)
ws_manager = ConnectionManager()
orchestrator = Orchestrator(ws_manager=ws_manager)
orchestrator._worker_callback = lambda run_id: execute_risk_job(
    run_id, orchestrator, ws_manager
)

router = APIRouter(prefix="/api", tags=["orchestrator"])
ws_router = APIRouter(tags=["websocket"])


class DemoCronRequest(BaseModel):
    """Опциональное JSON-тело для демо-эндпоинта запуска регламента."""

    process_id: str = Field(default="credit_risk")
    target_date: date = Field(default_factory=date.today)


@router.get("/processes")
async def list_processes() -> List[Dict[str, Any]]:
    """Возвращает список доступных процессов и их параметров из config/processes.json."""
    try:
        cfg_path = Path(__file__).resolve().parents[2] / "config" / "processes.json"
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as exc:
        logger.exception("Ошибка чтения config/processes.json")
        raise HTTPException(
            status_code=500, detail=f"Не удалось прочитать конфиг: {exc}"
        )

    return [
        {
            "id": pid,
            "name": meta.get("name", pid),
            "description": meta.get("description", ""),
            "window_days": meta.get("window_days", 1),
            "duration_sec": meta.get("duration_sec", 8),
            "resources": meta.get("resources", []),
        }
        for pid, meta in cfg.get("processes", {}).items()
    ]


@router.post("/runs/dry-run")
async def dry_run(payload: RunCreateRequest):
    """Предварительная проверка конфликтов без запуска воркера (FR-15)."""
    try:
        result = await orchestrator.dry_run(payload)
    except ValidationError as exc:
        logger.exception("Внутренняя ошибка Pydantic в dry_run")
        raise HTTPException(status_code=500, detail=f"Ошибка модели данных: {exc}")
    except ValueError as exc:
        logger.warning(
            "Ошибка dry_run (400): %s | payload=%s",
            exc,
            payload.model_dump(mode="json"),
        )
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result.model_dump(mode="json"))


@router.post("/runs")
async def submit_run(payload: RunCreateRequest):
    """Создаёт новый расчёт или присоединяет инициатора к существующему дублю."""
    try:
        result = await orchestrator.submit_run(payload)
    except ValidationError as exc:
        logger.exception("Внутренняя ошибка Pydantic в submit_run")
        raise HTTPException(status_code=500, detail=f"Ошибка модели данных: {exc}")
    except ValueError as exc:
        logger.warning(
            "Ошибка submit_run (400): %s | payload=%s",
            exc,
            payload.model_dump(mode="json"),
        )
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result)


@router.delete("/runs/{run_id}")
async def cancel_run(run_id: UUID):
    """Отменяет ожидающую задачу из очереди или таймера (FR-16)."""
    try:
        result = await orchestrator.cancel_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return JSONResponse(content=result)


@router.get("/state")
async def get_state():
    """Возвращает текущий снимок состояния системы (FR-14)."""
    snapshot: SystemStateSnapshot = await orchestrator.get_state_snapshot()
    return JSONResponse(content=snapshot.model_dump(mode="json"))


@router.put("/config")
async def update_config(payload: ConfigUpdateRequest):
    """Динамически обновляет параметры таймера машины, TTL и Heartbeat."""
    result = await orchestrator.update_config(payload)
    return JSONResponse(content=result)


@router.post("/demo/trigger-cron")
async def trigger_cron(
    process_id: Optional[str] = None,
    target_date: Optional[date] = None,
    body: Optional[DemoCronRequest] = Body(default=None),
):
    """Демо-кнопка: имитирует регламентный запуск от планировщика (MACHINE)."""
    resolved_process_id = process_id or (body.process_id if body else "credit_risk")
    resolved_target_date = target_date or (body.target_date if body else date.today())

    req = RunCreateRequest(
        process_id=resolved_process_id,
        target_date=resolved_target_date,
        initiator="MACHINE",
        user_name="Планировщик (Cron)",
    )
    try:
        result = await orchestrator.submit_run(req)
    except ValidationError as exc:
        logger.exception("Внутренняя ошибка Pydantic в trigger_cron")
        raise HTTPException(status_code=500, detail=f"Ошибка модели данных: {exc}")
    except ValueError as exc:
        logger.warning("Ошибка trigger_cron (400): %s", exc)
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result)


@router.post("/demo/kill-worker/{run_id}")
async def kill_worker_endpoint(run_id: UUID):
    """Демо-кнопка: имитирует крах воркера для проверки Watchdog TTL (NFR-02)."""
    kill_worker(run_id)
    await database.log_audit_event(
        run_id=run_id,
        event_type="WORKER_CRASH_SIMULATED",
        message=f"Имитирован сбой воркера #{str(run_id)[:8]} (Heartbeat остановлен, ожидается Watchdog TTL).",
    )
    await orchestrator._broadcast_state_safe()
    return JSONResponse(
        content={
            "status": "HEARTBEAT_STOPPED",
            "ttl_remaining_sec": orchestrator._lock_ttl_sec,
            "run_id": str(run_id),
        }
    )


@ws_router.websocket("/ws/events")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket-поток обновлений состояния системы."""
    await ws_manager.connect(websocket)
    try:
        state = await orchestrator.get_state_snapshot()
        await ws_manager.send_state_to_client(websocket, state.model_dump(mode="json"))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)
