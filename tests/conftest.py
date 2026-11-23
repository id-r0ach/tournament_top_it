"""Общие фикстуры и конфигурация pytest."""

from __future__ import annotations

import asyncio
import os
import sys
from typing import AsyncGenerator, Optional

# Принудительная настройка подключения к PostgreSQL для Docker-окружения
# Если переменная DATABASE_URL не задана или указывает на localhost внутри контейнера
default_docker_url = "postgresql://postgres:password@postgres:5432/risk_orchestrator"
current_env_url = os.environ.get("DATABASE_URL", "")

if not current_env_url:
    os.environ["DATABASE_URL"] = default_docker_url
elif os.path.exists("/.dockerenv") and ("localhost" in current_env_url or "127.0.0.1" in current_env_url):
    os.environ["DATABASE_URL"] = default_docker_url

try:
    import pytest
    import pytest_asyncio
except ImportError:
    pytest = None
    pytest_asyncio = None

import asyncpg
from httpx import ASGITransport, AsyncClient

from backend.api import routes
from backend.core.orchestrator import Orchestrator
from backend.core.worker import CRASHED_WORKERS
from backend.db import database
from backend.main import app

_db_available_status: Optional[bool] = None


async def _probe_connection(url: str) -> bool:
    try:
        conn = await asyncpg.connect(url, timeout=2.0)
        await conn.execute("SELECT 1")
        await conn.close()
        return True
    except Exception:
        return False


async def is_db_available() -> bool:
    """Проверяет доступность базы данных PostgreSQL с автоматическим переключением хоста."""
    global _db_available_status
    if _db_available_status is not None:
        return _db_available_status

    docker_url = "postgresql://postgres:password@postgres:5432/risk_orchestrator"
    local_url = "postgresql://postgres:password@localhost:5432/risk_orchestrator"
    current_url = os.environ.get("DATABASE_URL", docker_url)

    # 1. Сначала пробуем текущий DATABASE_URL
    if await _probe_connection(current_url):
        _db_available_status = True
        return True

    # 2. При неудаче переключаем на Docker-хост postgres:5432
    if current_url != docker_url and await _probe_connection(docker_url):
        os.environ["DATABASE_URL"] = docker_url
        if database._pool is not None:
            try:
                await database.close_pool()
            except Exception:
                pass
            database._pool = None
        _db_available_status = True
        return True

    # 3. При неудаче пробуем localhost:5432 (для запуска на хост-машине)
    if current_url != local_url and await _probe_connection(local_url):
        os.environ["DATABASE_URL"] = local_url
        if database._pool is not None:
            try:
                await database.close_pool()
            except Exception:
                pass
            database._pool = None
        _db_available_status = True
        return True

    _db_available_status = False
    return False


if pytest_asyncio is not None:
    @pytest_asyncio.fixture(autouse=True)
    async def manage_test_lifecycle(request):
        """Управляет жизненным циклом тестовой среды и очисткой таблиц."""
        # Для синхронных модульных тестов БД не требуется
        if request.node.get_closest_marker("asyncio") is None:
            yield
            return

        db_up = await is_db_available()
        if not db_up:
            yield
            return

        # Подготовка перед запуском теста
        await routes.orchestrator.cancel_all_workers()
        await database.init_db()
        await database.seed_synthetic_data()

        pool = await database.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM active_locks")
            await conn.execute("DELETE FROM runs")
            await conn.execute("DELETE FROM audit_logs")

        routes.orchestrator._lock_ttl_sec = 25
        routes.orchestrator._machine_timer_interval_sec = 15
        routes.orchestrator._heartbeat_interval_sec = 3
        CRASHED_WORKERS.clear()
        if hasattr(routes.orchestrator, "reset_memory_state"):
            routes.orchestrator.reset_memory_state()

        routes.orchestrator.start_background_tasks()

        try:
            yield
        finally:
            await routes.orchestrator.stop_background_tasks()
            await routes.orchestrator.cancel_all_workers()
            if hasattr(routes.orchestrator, "wait_pending_persists"):
                await routes.orchestrator.wait_pending_persists()
            if hasattr(routes.orchestrator, "reset_memory_state"):
                routes.orchestrator.reset_memory_state()
            try:
                pool = await database.get_pool()
                async with pool.acquire() as conn:
                    await conn.execute("DELETE FROM active_locks")
                    await conn.execute("DELETE FROM runs")
                    await conn.execute("DELETE FROM audit_logs")
            except Exception:
                pass
            CRASHED_WORKERS.clear()

    @pytest_asyncio.fixture
    async def client() -> AsyncGenerator[AsyncClient, None]:
        """Асинхронный HTTP-клиент для тестирования API."""
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
            yield ac


if pytest is not None:
    @pytest.fixture
    def requires_db():
        """Пропускает тест, если база данных PostgreSQL не запущена."""
        return is_db_available
