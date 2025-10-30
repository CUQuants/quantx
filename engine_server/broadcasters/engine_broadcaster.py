from engine_server.broadcasters.base_broadcaster import BaseBroadcaster
from engine_server.auth.auth_service import AuthService
import json
import asyncio
from typing import List
from websockets.asyncio.server import ServerConnection
from sqlalchemy.ext.asyncio import AsyncSession
from models import Order, OrderSide as Side, Trade, OrderType, OrderStatus
from services.engine_holder import engine_singleton
from engine_server.event_bus.event_bus import EventBus, EventType
from engine_server.broadcasters.broadcast_data import MarketDataSnapshot


class OrderBroadcaster(BaseBroadcaster):
    def __init__(self, host, port, interval: float, price_lower_bound: float, price_upper_bound: float, auth_service: AuthService, tickers: List[str], db_session: AsyncSession, bus: EventBus):
        super().__init__(host, port, interval)
        self.price_lower_bound = price_lower_bound
        self.price_upper_bound = price_upper_bound
        self.auth_service = auth_service
        self.db_session = db_session
        self.tickers = tickers
        self.bus = bus

        self.market_data = {ticker: MarketDataSnapshot() for ticker in tickers}

        self.client_subscriptions = {ticker: set() for ticker in tickers}
        self.locks = {ticker: asyncio.Lock() for ticker in tickers}

        self.orders_lock = asyncio.Lock()

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
                message = {"type": "snapshot", "orders": initial_snapshot}
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
        bid_price = msg.get("bid_price")
        ask_price = msg.get("ask_price")
        quantity = msg.get("amount_fulfilled")
        ticker = msg.get("ticker")

        async with self.locks[ticker]:
            ticker_data = self.market_data[ticker]
            ticker_data.remove_order(bid_price, quantity, Side.BUY)
            ticker_data.remove_order(ask_price, quantity, Side.SELL)

        await self.broadcast_to_ticker(ticker, {
            "type": "trade",
            "price": ask_price,
            "quantity": quantity
        })

    async def broadcast_to_ticker(self, ticker: str, msg: dict):
        async with self.clients_lock:
            clients = list(self.client_subscriptions[ticker])

        message = json.loads(msg)

        for client in clients:
            try:
                await client.send(message)
            except Exception as e:
                # Handle logic for discarding dead clients later
                print(f"Failed to send to client: {e}")

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
            await self.bus.publish(EventType.ORDER, {"order": db_order, "db": self.db_session})

        await asyncio.gather(
            websocket.send(json.dumps(
                {"type": "order_success", "message": "Order placed successfully"})),
            self.broadcast_message({"type": "update", "order": order})
        )
