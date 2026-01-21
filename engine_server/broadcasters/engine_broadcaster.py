from engine_server.broadcasters.base_broadcaster import BaseBroadcaster
from engine_server.auth.auth_service import AuthService
import json
import asyncio
from typing import List
from websockets.asyncio.server import ServerConnection
from sqlalchemy.ext.asyncio import AsyncSession
from models import Order, OrderSide as Side, Trade, OrderType, OrderStatus
from engine_server.event_bus.event_bus import EventBus, EventType
from engine_server.broadcasters.broadcast_data import MarketDataSnapshot
from datetime import datetime, timezone
from engine_server.db_functions import add_db_order
from engine_server.db_session import SessionFactory

SNAPSHOT_LENGTH = 10


class OrderBroadcaster(BaseBroadcaster):

    def __init__(self, host, port, auth_service: AuthService, tickers: List[str], bus: EventBus):
        super().__init__(host, port)
        self.auth_service = auth_service
        self.tickers = tickers
        self.bus = bus

        self.market_data = {ticker: MarketDataSnapshot(
            ticker, SNAPSHOT_LENGTH) for ticker in tickers}

        self.client_subscriptions = {ticker: set() for ticker in tickers}
        self.locks = {ticker: asyncio.Lock() for ticker in tickers}

    async def create_message(self):
        pass

    async def create_batch_message(self):
        pass

    async def initial_connection_action(self, client: ServerConnection):
        ticker = self.extract_ticker(client)
        if not ticker:
            await self.send_error(client, "ROOM_ERROR", "Ticker string not provided!")
        elif ticker not in self.client_subscriptions:
            await self.send_error(client, "INVALID_TICKER", f"Ticker: {ticker} is invalid")
        else:
            async with self.clients_lock:
                self.client_subscriptions[ticker].add(client)
            async with self.locks[ticker]:
                ticker_data = self.market_data.get(ticker)
                initial_snapshot = ticker_data.get_snapshot()
            message = {"type": "batch", "orders": initial_snapshot}
            await client.send(json.dumps(message))

    def extract_ticker(self, client: ServerConnection):
        try:
            raw_url = client.request.path
            ticker = raw_url.split("/")[-1].upper()
            return ticker
        except Exception as e:
            print(e)
            return None

    async def on_trade(self, msg):

        # handle errors more gracefully later
        bid_price = msg.get("bid_price")
        ask_price = msg.get("ask_price")
        quantity = msg.get("quantity")
        ticker = msg.get("ticker")
        price = msg.get("price")

        async with self.locks[ticker]:
            try:
                ticker_data = self.market_data[ticker]
                ticker_data.remove_order(bid_price, quantity, Side.BUY)
                ticker_data.remove_order(ask_price, quantity, Side.SELL)
                new_data = ticker_data.get_snapshot()
            except Exception as e:
                print(e)
        await self.broadcast_to_ticker(ticker, {"type": "batch", "orders": new_data, "last_trade": price})

    async def broadcast_to_ticker(self, ticker: str, msg: dict):
        async with self.clients_lock:
            clients = list(self.client_subscriptions[ticker])

        payload = json.dumps(msg)

        for client in clients:
            try:
                await client.send(payload)
            except Exception as e:
                await self.remove_client(client)
                print(f"Failed to send to client: {e}")

    async def on_message(self, msg: dict, websocket: ServerConnection):
        message_type = msg.get("type")

        if not message_type:
            await self.send_error(websocket, "MISSING_TYPE", "A message type is required")
            return

        elif message_type == "order":
            await self.handle_order(websocket, msg)

        else:
            await self.send_error(websocket, "INVALID_MESSAGE_TYPE", "Message type is invalid")

    async def handle_order(self, websocket: ServerConnection, msg: dict):
        token = msg.get("token", None)
        response = await self.auth_service.validate_token(token)

        if response.get("success", False) == False:
            error_type, error_message = response.get(
                "error_code"), response.get("error_message")
            print(f"ERROR: {error_type}, {error_message}")
            await self.send_error(websocket, error_type, error_message)
            return

        order = msg.get("order", None)
        side = order_side = Side.BUY if order["type"] == "Buy" else Side.SELL

        user_id = response.get("user_id")
        email = response.get("email")

        quantity = order.get("quantity")
        ticker = order.get("ticker")
        price = order.get("price")

        async with SessionFactory() as session:
            try:
                async with session.begin():
                    order_object = Order(symbol=ticker, account_id=user_id,
                                         side=side, quantity=quantity, price=price, type=OrderType.LIMIT, filled_quantity=0, created_at=datetime.now(timezone.utc))

                    db_order = await add_db_order(order_object, email, user_id, session)
            except Exception as e:
                print(e)
                await self.send_error(websocket, "ORDER_VALIDATION_ERROR", str(e))
                return

        # Create order object

        async with self.locks[ticker]:
            self.market_data[ticker].add_order(price, quantity, order_side)
            new_snapshot = self.market_data[ticker].get_snapshot()

        await self.broadcast_to_ticker(ticker, {"type": "batch", "orders": new_snapshot})
        await self.bus.publish(EventType.ORDER, {"order": db_order})

        await asyncio.gather(
            websocket.send(json.dumps(
                {"type": "order_success", "message": "Order placed successfully"})),
        )

    async def remove_client(self, websocket: ServerConnection):
        async with self.clients_lock:
            for ticker in self.client_subscriptions:
                self.client_subscriptions[ticker].discard(websocket)
