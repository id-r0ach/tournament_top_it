"""REST и WebSocket маршруты для API.

Файлы включают только минимально необходимые эндпоинты, которые
используют уже существующий класс :class:`~backend.core.orchestrator.Orchestrator`.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Dict, List

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pathlib import Path

from backend.api.ws import ConnectionManager
from backend.core.orchestrator import Orchestrator
from backend.core.worker import execute_risk_job, kill_worker
from backend.db import database
from backend.db.models import (
    ConfigUpdateRequest,
    RunCreateRequest,
    SystemStateSnapshot,
)

# ---------------------------------------------------------------------------
# 1. Создаём менеджер WebSocket и экземпляр оркестра.
# ---------------------------------------------------------------------------

ws_manager = ConnectionManager()
# Create orchestrator без callback сначала
orchestrator = Orchestrator(ws_manager=ws_manager)
# Далее задаём callback, который будет передавать необходимые аргументы
orchestrator._worker_callback = lambda run_id: execute_risk_job(run_id, orchestrator, ws_manager)

# ---------------------------------------------------------------------------
# 2. Определяем роутеры.
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/api", tags=["orchestrator"])
ws_router = APIRouter(tags=["websocket"])

# ---------------------------------------------------------------------------
# 3. REST эндпоинты.
# ---------------------------------------------------------------------------


@router.get("/processes")
async def list_processes() -> List[Dict[str, str]]:
    """Возвращает список доступных процессов (id из config/processes.json)."""
    try:
        cfg_path = Path(__file__).resolve().parents[2] / "config" / "processes.json"
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Не удалось прочитать конфиг: {exc}")
    return [{"id": pid} for pid in cfg.get("processes", {}).keys()]


@router.post("/runs/dry-run")
async def dry_run(payload: RunCreateRequest):
    try:
        result = await orchestrator.dry_run(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result.model_dump())


@router.post("/runs")
async def submit_run(payload: RunCreateRequest):
    try:
        result = await orchestrator.submit_run(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result)


@router.delete("/runs/{run_id}")
async def cancel_run(run_id: str):
    uid = uuid.UUID(run_id)
    try:
        result = await orchestrator.cancel_run(uid)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    return JSONResponse(content=result)


@router.get("/state")
async def get_state():
    snapshot: SystemStateSnapshot = await orchestrator.get_state_snapshot()
    return JSONResponse(content=snapshot.model_dump())


@router.put("/config")
async def update_config(payload: ConfigUpdateRequest):
    result = await orchestrator.update_config(payload)
    return JSONResponse(content=result)


@router.post("/demo/trigger-cron")
async def trigger_cron(process_id: str, target_date: str):
    try:
        target = datetime.fromisoformat(target_date)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Неверный формат даты: {exc}")
    req = RunCreateRequest(
        process_id=process_id,
        target_date=target,
        initiator="MACHINE",
        user_name="Планировщик (Cron)",
    )
    try:
        result = await orchestrator.submit_run(req)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return JSONResponse(content=result)


@router.post("/demo/kill-worker/{run_id}")
async def kill_worker_endpoint(run_id: str):
    uid = uuid.UUID(run_id)
    kill_worker(uid)
    await database.log_audit_event(
        run_id=str(uid),
        event_type="WORKER_CRASH_SIMULATED",
        message="Simulated worker crash for testing.",
    )
    await orchestrator._broadcast_state_safe()
    return JSONResponse(content={"status": "crash simulated"})

# ---------------------------------------------------------------------------
# 4. WebSocket эндпоинт.
# ---------------------------------------------------------------------------


@ws_router.websocket("/ws/events")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        state = await orchestrator.get_state_snapshot()
        await ws_manager.send_state_to_client(websocket, state.model_dump())
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)
