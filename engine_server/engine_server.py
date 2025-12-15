from engine_server.auth.firebase_auth_service import FirebaseAuth
from engine_server.broadcasters.engine_broadcaster import OrderBroadcaster
from engine_server.event_bus.event_bus import EventBus, EventType
from engine import MatchingEngine
from engine_server.db_session import SessionFactory
from api.db import init_models
import asyncio


TICKERS = ["QNTX"]


if __name__ == "__main__":

    asyncio.run(init_models())

    event_bus = EventBus()
    matching_engine = MatchingEngine(event_bus)
    firebase_service = FirebaseAuth()
    broadcaster = OrderBroadcaster(host="localhost", port=8765,
                                   auth_service=firebase_service, tickers=TICKERS, bus=event_bus)

    async def order_handler(payload: dict):
        order = payload["order"]
        async with SessionFactory() as session:
            async with session.begin():
                await matching_engine.add_order(order, session)

    async def trade_handler(payload: dict):
        await broadcaster.on_trade(payload)

    event_bus.subscribe_event(EventType.ORDER, order_handler)
    event_bus.subscribe_event(EventType.TRADE, trade_handler)

    broadcaster.start_server()
