from api.socket_service.base_broadcaster import BaseBroadcaster
from engine_server.auth.auth_service import AuthService
import json
import asyncio
from typing import List
from websockets.asyncio.server import ServerConnection
from models import Order, OrderSide as Side, OrderType
from api.socket_service.event_bus import EventBus
from engine_server.broadcasters.broadcast_data import MarketDataSnapshot
from datetime import datetime, timezone
from engine_server.db_functions import add_db_order
from engine_server.db_session import SessionFactory
from api.socket_service.adapters import ServerConnectionAdapter
from api.socket_service.event_bus import EventType

SNAPSHOT_LENGTH = 10


class OrderBroadcaster(BaseBroadcaster):

    def __init__(self, auth_service: AuthService, tickers: List[str], bus: EventBus):
        super().__init__()
        self.auth_service = auth_service
        self.tickers = tickers
        self.bus = bus

        self.market_data = {ticker: MarketDataSnapshot(
            ticker, SNAPSHOT_LENGTH) for ticker in tickers}

        self.client_subscriptions = {ticker: set() for ticker in tickers}
        self.locks = {ticker: asyncio.Lock() for ticker in tickers}

        self.update_queues = {ticker: asyncio.Queue() for ticker in tickers}
        self.queue_processors = {}

        for ticker in tickers:
            self.queue_processors[ticker] = asyncio.create_task(
                self._process_snapshot_updates(ticker)
            )

    async def create_message(self):
        pass

    async def create_batch_message(self):
        pass

    async def _process_snapshot_updates(self, ticker: str):
        """
        This solves the problem of asychronous race conditions for market data updates.
        When many orders are placed simultaneously, the corresponding market data updates are processed sequentially.
        """
        while True:
            try:
                update_data = await self.update_queues[ticker].get()

                if update_data["type"] == "add_order":
                    self.market_data[ticker].add_order(
                        update_data["price"],
                        update_data["quantity"],
                        update_data["side"]
                    )
                elif update_data["type"] == "remove_order":
                    try:
                        self.market_data[ticker].remove_order(
                            update_data["bid_price"],
                            update_data["quantity"],
                            Side.BUY
                        )
                        self.market_data[ticker].remove_order(
                            update_data["ask_price"],
                            update_data["quantity"],
                            Side.SELL
                        )
                    except ValueError as e:
                        print(f"Trade removal error for {ticker}: {e}")
                elif update_data["type"] == "build_orderbook":
                    self.market_data[ticker].build(update_data["orders"])

                snapshot = self.market_data[ticker].get_snapshot()
                message = {"type": "batch", "orders": snapshot}

                if "last_trade" in update_data:
                    message["last_trade"] = update_data["last_trade"]

                await self.broadcast_to_ticker(ticker, message)

                self.update_queues[ticker].task_done()

            except Exception as e:
                print(f"Queue processor error for {ticker}: {e}")

    async def build_orderbook(self, ticker, orders: Order):
        await self.update_queues[ticker].put({
            "type": "build_orderbook",
            "orders": orders
        })

    async def add_subscription(self, ws: ServerConnectionAdapter, ticker: str):
        await self.add_client(ws)
        await self.initial_connection_action(ws, {"ticker": ticker})

    async def initial_connection_action(self, client: ServerConnectionAdapter, params: dict = {}):
        ticker = params.get("ticker")
        if not ticker:
            await self.send_error(client, "ROOM_ERROR", "Ticker string not provided!")
        elif ticker not in self.client_subscriptions:
            await self.send_error(client, "INVALID_TICKER", f"Ticker: {ticker} is invalid")
        else:
            async with self.clients_lock:
                self.client_subscriptions[ticker].add(client)
            async with self.locks[ticker]:
                ticker_data = self.market_data.get(ticker)
                if not ticker_data:
                    raise KeyError(f"Invalid market data for ticker: {ticker}")
                if ticker_data.is_empty():
                    await self.bus.publish(EventType.ORDERBOOK_SNAPSHOT,
                                           payload={"ticker": ticker})
                    return
                else:
                    initial_snapshot = ticker_data.get_snapshot()
                message = {"type": "batch", "orders": initial_snapshot}
                await client.send(message)

    def extract_ticker(self, client: ServerConnection):
        try:
            raw_url = client.request.path
            ticker = raw_url.split("/")[-1].upper()
            return ticker
        except Exception as e:
            print(e)
            return None

    async def on_trade(self, msg):
        # Queue the trade update instead of processing directly
        bid_price = msg.get("bid_price")
        ask_price = msg.get("ask_price")
        quantity = msg.get("quantity")
        ticker = msg.get("ticker")
        price = msg.get("price")

        await self.update_queues[ticker].put({
            "type": "remove_order",
            "bid_price": bid_price,
            "ask_price": ask_price,
            "quantity": quantity,
            "last_trade": price
        })

    async def broadcast_to_ticker(self, ticker: str, msg: dict):
        async with self.clients_lock:
            clients = list(self.client_subscriptions[ticker])

        payload = msg
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
        response = self.auth_service.validate_token(token)

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
                    db_order = await add_db_order(order_object, user_id, email, session)
            except Exception as e:
                print(e)
                await self.send_error(websocket, "ORDER_VALIDATION_ERROR", str(e))
                return

        await self.update_queues[ticker].put({
            "type": "add_order",
            "price": price,
            "quantity": quantity,
            "side": order_side
        })

        await self.bus.publish(EventType.ORDER, {"order": db_order})

        await asyncio.gather(
            websocket.send(json.dumps(
                {"type": "order_success", "message": "Order placed successfully"})),
        )

    async def remove_client(self, websocket: ServerConnection):
        async with self.clients_lock:
            for ticker in self.client_subscriptions:
                self.client_subscriptions[ticker].discard(websocket)

    async def shutdown(self):
        for ticker, task in self.queue_processors.items():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
