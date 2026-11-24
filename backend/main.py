"""Точка входа FastAPI приложения."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from backend.api.routes import orchestrator, router, ws_manager, ws_router
from backend.db import database

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.init_db()
    await database.seed_synthetic_data()

    app.state.ws_manager = ws_manager
    app.state.orchestrator = orchestrator
    orchestrator.start_background_tasks()
    logger.info("Сервер оркестратора рисков готов к работе.")
    try:
        yield
    finally:
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

BASE_DIR = Path(__file__).resolve().parents[1]
frontend_dir = BASE_DIR / "frontend"
if frontend_dir.is_dir():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

index_file = frontend_dir / "index.html"
favicon_file = frontend_dir / "favicon.ico"


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


@app.get("/favicon.ico", include_in_schema=False)
async def read_favicon():
    if favicon_file.exists():
        return FileResponse(str(favicon_file))
    return JSONResponse(status_code=404, content={"detail": "Not found"})