import requests
from websocket import WebSocket
from typing import Callable, Any, Dict, List
import json
import asyncio


"""
This is the skeleton of the client library that users can use to trade.
It allows users to receive trades, orders, order cancellation, and price changes in real time.
This allows users to develop their own flexible trading strategies in Python to possibly boost performance.
"""


class InvalidAPIKeyException(Exception):
    msg: str


EventType = "trade" | "order" | "price_change" | "order_cancellation"

MessageHandler = Callable[[Dict[str, Any]], Any]


class QuantXWebSocket:

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.ws: WebSocket | None = None
        self.url = ""

        self._is_running = False

        self._handlers: Dict[str, List[MessageHandler]] = {}

    def on(self, event: Dict, callback: Callable):
        return True

    def _validate_ticker(self, ticker: str):
        return True

    def _register_handler(self, event: Dict, handler: MessageHandler):
        return True

    async def connect(self):

        try:
            self.ws = await self.ws.connect(self.url)

            if self.api_key:
                await self.ws.send(json.dumps(
                    {
                        "api_key": self.api_key,
                        "type": "auth"
                    }
                ))

            asyncio.create_task(self._listen())
            self._is_running = True

        except Exception as e:
            print("Error connecting")

    async def _listen(self):
        try:
            async for message in self.ws:
                data = json.loads(message)

                await self.handle_message(data)

        except Exception as e:
            print("Error listening to events")

    async def handle_message(self, message: Dict):
        msg_type = message.get("type")

        handlers = handlers.get(msg_type, [])

        for handler in handlers:
            try:

                if asyncio.iscoroutinefunction(handler):
                    await handler(message)
                else:
                    handler(message)

            except Exception as e:
                print("Error dispatching handler handler")

    async def close(self):
        self._running = False
        if self.ws:
            await self.ws.close()

    async def send_order(self, order_payload: Dict[str: Any]):
        pass
