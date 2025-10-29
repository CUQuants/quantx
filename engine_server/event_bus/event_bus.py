from enum import Enum
from typing import Awaitable, Callable, Dict, Set, Any
import asyncio


class EventType(Enum):
    ORDER = "order"
    TRADE = "trade"
    ORDERBOOK_SNAPSHOT = "orderbook_snapshot"
    PRICE_UPDATE = "price_update"


SubscriberFn = Callable[[dict], Awaitable[None]]


class EventBus:

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

        await asyncio.gather(*(sub(payload) for sub in subscribers))
