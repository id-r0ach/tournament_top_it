"""Главный модуль FastAPI приложения."""
import asyncio
import logging
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

from backend.api.routes import router, ws_router
from backend.api.ws import ConnectionManager
from backend.core.orchestrator import Orchestrator
from backend.core.worker import execute_risk_job
from backend.db import database

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # При старте приложения
    await database.init_db()
    await database.seed_synthetic_data()
    # Создаём менеджер WebSocket и оркестратор
    ws_manager = ConnectionManager()
    orchestrator = Orchestrator(ws_manager=ws_manager)
    # Привязываем callback к воркеру
    orchestrator._worker_callback = lambda run_id: asyncio.create_task(
        execute_risk_job(run_id, orchestrator, ws_manager)
    )
    # Сохраняем в состояние приложения
    app.state.ws_manager = ws_manager
    app.state.orchestrator = orchestrator
    orchestrator.start_background_tasks()
    try:
        yield
    finally:
        # При остановке сервера
        await orchestrator.stop_background_tasks()
        await database.close_pool()


app = FastAPI(title="Risk Process Orchestrator MVP", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(ws_router)

# Монтируем фронтенд, если он присутствует
frontend_dir = Path("frontend")
if frontend_dir.is_dir():
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")

index_file = frontend_dir / "index.html"

@app.get("/")
async def read_index():
    if index_file.exists():
        return FileResponse(str(index_file))
    return JSONResponse(
        {
            "service": "Risk Process Orchestrator MVP",
            "status": "running",
            "docs": "/docs",
        }
    )
