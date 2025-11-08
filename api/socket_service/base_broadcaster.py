import websockets
import asyncio
from abc import ABC, abstractmethod
import json
from datetime import datetime
from websockets.asyncio.server import ServerConnection
from api.socket_service.adapters import ServerConnectionAdapter


class BaseBroadcaster(ABC):

    def __init__(self, timeout=None):
        self.clients = set()
        self.timeout = timeout

        self.clients_lock = asyncio.Lock()

    async def add_client(self, websocket: ServerConnectionAdapter):
        async with self.clients_lock:
            self.clients.add(websocket)

    async def broadcast_message(self, message: dict):
        async with self.clients_lock:
            clients_copy = self.clients.copy()
        await asyncio.gather(*[client.send(json.dumps(message)) for client in clients_copy],
                             return_exceptions=True)

    async def broadcast_batch(self, client):
        message = await self.create_batch_message()
        await client.send(json.dumps(message))

    async def send_error(self, websocket: ServerConnectionAdapter, error_type: str, error_message: str):
        error_response = {
            "type": "error",
            "error_type": error_type,
            "error_message": error_message,
            "timestamp": datetime.now().isoformat()
        }
        await websocket.send(json.dumps(error_response))

    @ abstractmethod
    async def initial_connection_action(self, websocket: ServerConnectionAdapter, params: dict = {}):
        pass

    @ abstractmethod
    async def create_batch_message(self):
        pass

    @ abstractmethod
    async def create_message(self) -> dict:
        pass

    @ abstractmethod
    def on_message(self, msg: dict, websocket: ServerConnectionAdapter):
        pass

    @abstractmethod
    async def remove_client(self, websocket: ServerConnection):
        pass
