from api.services.broadcasting_service import WebSocketClient
from api.services import BaseService
from .event_bus import EventBus, EventHandler
from .events import (
    Event,
    EventType,
    RawOrderPayload,
    OrderRejectedPayload,
    OrderPersistedPayload,
    CancelOrderPayload,
    OrderCancelledPayload,
    CancelRejectedPayload,
)
from typing import List, Tuple


class APIBroadcastingService(BaseService):
    """
    This class is the entrypoint for the QuantX websocket API.
    It should share methods with the main BroadcastingService, and as such there
    should be an abstract base class that both of these inherit from.
    """

    def __init__(self, event_bus: EventBus, auth_service, tickers: List[str]):
        super().__init__(event_bus)
        self.auth_service = auth_service
        self.tickers = tickers

    @property
    def service_name(self) -> str:
        return "APIBroadcastingService"

    def _get_subscriptions(self) -> List[Tuple[EventType, EventHandler]]:
        return [
            (EventType.ORDER_CANCELLED, None),
            (EventType.TRADE_EXECUTED, None),
            (EventType.ORDER_PERSISTED, None),
        ]
