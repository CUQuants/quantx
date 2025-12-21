"""
Service-Oriented Architecture for QuantX Trading System

This package contains all services that communicate via an asyncio.Queue-based event bus.
Each service has a single responsibility and communicates exclusively through events.

Services:
    - BroadcastingService: WebSocket handler, authentication, client management
    - RiskEngine: Order validation before matching
    - MatchingEngine: Order book management and trade execution
    - PersistenceService: All database operations
    - MarketDataBroadcaster: Market data distribution to clients

Event Flow:
    RAW_ORDER → RiskEngine validates → VALIDATED_ORDER or ORDER_REJECTED
    VALIDATED_ORDER → MatchingEngine → TRADE_EXECUTED + MARKET_DATA_UPDATE
    VALIDATED_ORDER → PersistenceService → ORDER_PERSISTED
    TRADE_EXECUTED → PersistenceService → updates DB
    MARKET_DATA_UPDATE → MarketDataBroadcaster → WebSocket clients
"""

from .events import EventType, Event
from .event_bus import EventBus
from .base_service import BaseService
from .matching_engine import MatchingEngine
from .risk_engine import RiskEngine
from .persistence_service import PersistenceService
from .broadcasting_service import BroadcastingService
from .market_data_broadcaster import MarketDataBroadcaster
from .event_stream_broadcaster import EventStreamBroadcaster
from .api_key_service import ApiKeyService
from .service_container import ServiceContainer

__all__ = [
    "EventType",
    "Event",
    "EventBus",
    "BaseService",
    "MatchingEngine",
    "RiskEngine",
    "PersistenceService",
    "BroadcastingService",
    "MarketDataBroadcaster",
    "EventStreamBroadcaster",
    "ApiKeyService",
    "ServiceContainer",
]
