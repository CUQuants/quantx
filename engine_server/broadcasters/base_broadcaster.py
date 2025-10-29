import websockets
import asyncio
from abc import ABC, abstractmethod
import json
from datetime import datetime
from websockets.asyncio.server import ServerConnection


class BaseBroadcaster(ABC):

    def __init__(self, host, port, interval: float, timeout=None):
        self.interval = interval
        self.host = host
        self.port = port
        self.clients = set()
        self.timeout = timeout

        self.clients_lock = asyncio.Lock()

    def start_server(self):
        asyncio.run(self.server_initializer())

    async def server_initializer(self):

        async with websockets.serve(self.handler, self.host, self.port):
            print("WebSocket server started on ws://localhost:8765")
            asyncio.create_task(self.broadcast_periodic())
            await asyncio.Future()

    async def handler(self, websocket: ServerConnection):

        async with self.clients_lock:
            self.clients.add(websocket)
        await self.initial_connection_action(client=websocket)

        try:
            async for raw in websocket:
                msg = json.loads(raw)
                await self.on_message(msg, websocket)
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            print("Client disconnected")
            async with self.clients_lock:
                self.clients.discard(websocket)

    async def broadcast_periodic(self):
        while True:
            await asyncio.sleep(self.interval)
            try:
                message = await self.create_message()
                print(message)
            except Exception as e:
                print(e)
                message = {"error": e}

            await self.broadcast_message(message)

    async def broadcast_message(self, message: dict):
        async with self.clients_lock:
            clients_copy = self.clients.copy()
        await asyncio.gather(*[client.send(json.dumps(message)) for client in clients_copy],
                             return_exceptions=True)

    async def broadcast_batch(self, client):
        message = await self.create_batch_message()
        await client.send(json.dumps(message))

    async def send_error(self, websocket: ServerConnection, error_type: str, error_message: str):
        error_response = {
            "type": "error",
            "error_type": error_type,
            "error_message": error_message,
            "timestamp": datetime.now().isoformat()
        }
        await websocket.send(json.dumps(error_response))

    @ abstractmethod
    async def initial_connection_action(self):
        pass

    @ abstractmethod
    async def create_batch_message(self):
        pass

    @ abstractmethod
    async def create_message(self) -> dict:
        pass

    @ abstractmethod
    def on_message(self, msg: dict, websocket: ServerConnection):
        pass
