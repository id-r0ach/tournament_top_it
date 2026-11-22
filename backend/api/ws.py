"""WebSocket manager for real‑time updates.

Класс :class:`ConnectionManager` хранит активные подключения и умеет
асинхронно рассыла��ть им сообщения о состоянии системы.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Set

from fastapi import WebSocket, WebSocketDisconnect


class ConnectionManager:
    """Управляет подключениями WebSocket."""

    def __init__(self) -> None:
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        """Добавляет клиент в активные подключения."""
        await websocket.accept()
        self.active_connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        """Удаляет клиент из активных подключений."""
        self.active_connections.discard(websocket)

    async def send_state_to_client(self, websocket: WebSocket, state_payload: dict) -> None:
        """Отправляет клиенту событие обновления состояния."""
        event = {
            "event": "STATE_UPDATE",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "payload": state_payload,
        }
        await websocket.send_json(event)

    async def broadcast_state(self, state_payload: dict) -> None:
        """Рассылает обновление всем клиентам, отключая падшие сокеты."""
        disconnected = []
        for ws in list(self.active_connections):
            try:
                await ws.send_json(
                    {
                        "event": "STATE_UPDATE",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "payload": state_payload,
                    }
                )
            except Exception:
                disconnected.append(ws)
        for ws in disconnected:
            self.disconnect(ws)