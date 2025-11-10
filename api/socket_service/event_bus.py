from enum import Enum
from typing import Awaitable, Callable, Dict, Set, Any
import asyncio


class EventType(Enum):
    ORDER = "order"
    TRADE = "trade"
    ORDERBOOK_SNAPSHOT = "orderbook_snapshot"
    PRICE_UPDATE = "price_update"
    BUILD_MARKETDATA = "build_marketdata"


SubscriberFn = Callable[[dict], Awaitable[None]]


class EventBus:
    """
    The event bus handles communication between the matching engine and the broadcasting service.
    When adding a subscriber, you pass in an event type, and a callback function that gets executed when a specific type of event occurs.
    When running it in the main file, you would pass in methods that are a part of both the matching engine and the broadcasting service.

    For example:

    event_bus = EventBus()

    engine = MatchingEngine()

    event_bus.subscribe_event(EventType.ORDER, engine.add_order)

    When an order is published to the event bus, the matching engine then receives this order and executes the add_order method
    """

    def __init__(self):
        self.rooms: Dict[EventType, Set[SubscriberFn]] = {
            event_type: set() for event_type in EventType
        }

    def subscribe_event(self, event: EventType, callable: SubscriberFn):
        if event not in self.rooms:
            raise ValueError("Invalid event type!")
        self.rooms[event].add(callable)

    async def publish(self, event_type: EventType, payload: dict):
        subscribers = list(self.rooms[event_type])
        try:
            await asyncio.gather(*(sub(payload) for sub in subscribers))
        except Exception as e:
            print("PUBLISH EXCEPTION", e)
