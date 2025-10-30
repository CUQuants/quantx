from engine_server.auth.firebase_auth_service import FirebaseAuth
from engine_server.broadcasters.engine_broadcaster import OrderBroadcaster
from engine_server.event_bus.event_bus import EventBus, EventType
from engine import MatchingEngine

TICKERS = ["QNTX"]

if __name__ == "__main__":
    matching_engine = MatchingEngine()
    event_bus = EventBus()
    firebase_service = FirebaseAuth()
    broadcaster = OrderBroadcaster(host="localhost", port=67, interval=10, price_lower_bound=10,
                                   price_upper_bound=20, auth_service=firebase_service, tickers=TICKERS, db_session=None, event_bus=event_bus)

    event_bus.subscribe_event(EventType.ORDER, matching_engine.add_order)
    event_bus.subscribe_event(EventType.TRADE, broadcaster.on_trade)

    broadcaster.start_server()
