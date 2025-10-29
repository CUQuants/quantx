from engine_server.broadcasters.base_broadcaster import BaseBroadcaster
from engine_server.auth.auth_service import AuthService
import json
import asyncio
from typing import List
from websockets.asyncio.server import ServerConnection
from sqlalchemy.ext.asyncio import AsyncSession
from models import Order, OrderSide as Side, Trade, OrderType, OrderStatus
from services.engine_holder import engine_singleton
from engine_server.event_bus.event_bus import EventBus


class OrderBroadcaster(BaseBroadcaster):
    def __init__(self, host, port, interval: float, price_lower_bound: float, price_upper_bound: float, auth_service: AuthService, tickers: List[str], db_session: AsyncSession, bus: EventBus):
        super().__init__(host, port, interval)
        self.price_lower_bound = price_lower_bound
        self.price_upper_bound = price_upper_bound
        self.auth_service = auth_service
        self.db_session = db_session

        self.locks = {ticker: asyncio.Lock() for ticker in tickers}

        self.orders_lock = asyncio.Lock()

    def create_ticker_map(self, tickers: List[str]):
        ticker_map = {}
        for ticker in tickers:
            ticker_map[ticker] = {"bids": {}, "asks": {}}
        return ticker_map

    async def create_message(self):
        pass

    async def initial_connection_action(self, client: ServerConnection):
        pass

    async def create_batch_message(self):
        pass

    async def on_message(self, msg: dict, websocket: ServerConnection):
        message_type = msg.get("type", None)

        if not message_type:
            await self.send_error(websocket, "MISSING_TYPE", "A message type is required")
            return

        elif message_type == "order":
            await self.handle_order(websocket, msg)
        else:
            await self.send_error(websocket, "INVALID_MESSAGE_TYPE", "Message type is invalid")

    async def handle_order(self, websocket: ServerConnection, msg: dict):
        token = msg.get("token", None)
        response = self.auth_service.validate_token(token)

        if response.get("success", False) == False:
            error_type, error_message = response.get(
                "error_code", None), response.get("error_message", None)
            print(f"ERROR: {error_type}, {error_message}")
            await self.send_error(websocket, error_type, error_message)
            return

        order = msg.get("order", None)

        if not order:
            await self.send_error(websocket, "NO_ORDER", "Order field must be present")
            return

        else:
            price = order.get("price")
            if price <= 0:
                await websocket.send(json.dumps({"type": "error", "error_type": "VALUE_ERROR", "error_message": "Price must be positive"}))
                return
            user_id = response.get("user_id", None)

        ticker = "QNTX"
        quantity = order["quantity"]

        order_side = order["type"]

        order_side = Side.BUY if order_side == "buy" else Side.SELL
        price = order["price"]
        # Create order object
        db_order = Order(symbol=ticker, account_id=user_id,
                         side=order_side, quantity=quantity, price=price)
        async with self.orders_lock:
            await engine_singleton().add_order(db_order, self.db_session)

        await asyncio.gather(
            websocket.send(json.dumps(
                {"type": "order_success", "message": "Order placed successfully"})),
            self.broadcast_message({"type": "update", "order": order})
        )
