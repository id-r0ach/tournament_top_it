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
        """Отправляет клиенту событие обновления состояния (раздел 7.2 SDD)."""
        now_ts = datetime.now(timezone.utc).isoformat()
        event = {
            "type": "STATE_UPDATE",
            "event": "STATE_UPDATE",
            "timestamp": now_ts,
            "data": state_payload,
            "payload": state_payload,
        }
        await websocket.send_json(event)

    async def broadcast_state(self, state_payload: dict) -> None:
        """Рассылает обновление всем клиентам (раздел 7.2 SDD, задержка < 1 сек)."""
        now_ts = datetime.now(timezone.utc).isoformat()
        event = {
            "type": "STATE_UPDATE",
            "event": "STATE_UPDATE",
            "timestamp": now_ts,
            "data": state_payload,
            "payload": state_payload,
        }
        disconnected = []
        for ws in list(self.active_connections):
            try:
                await ws.send_json(event)
            except Exception:
                disconnected.append(ws)
        for ws in disconnected:
            self.disconnect(ws)