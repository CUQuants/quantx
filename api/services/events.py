"""
Event definitions for the QuantX trading system event bus.

This module defines all event types and their payload structures used for
inter-service communication. Events are immutable dataclasses that flow
through the asyncio.Queue-based event bus.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Optional
import uuid

from models import OrderSide, OrderType, OrderStatus


class EventType(Enum):
    """
    All event types in the system.

    Event Flow:
        RAW_ORDER → RiskEngine validates → VALIDATED_ORDER or ORDER_REJECTED
        VALIDATED_ORDER → MatchingEngine processes → TRADE_EXECUTED + MARKET_DATA_UPDATE
        VALIDATED_ORDER → PersistenceService → ORDER_PERSISTED
        TRADE_EXECUTED → PersistenceService → updates DB
        MARKET_DATA_UPDATE → MarketDataBroadcaster → WebSocket clients
    """
    # Order lifecycle events
    RAW_ORDER = auto()           # Unauthenticated order from WebSocket (after auth)
    VALIDATED_ORDER = auto()     # Order passed risk checks, ready for matching
    ORDER_REJECTED = auto()      # Order failed validation
    ORDER_PERSISTED = auto()     # Order successfully written to DB

    # Trade events
    TRADE_EXECUTED = auto()      # Match occurred in matching engine

    # Market data events
    MARKET_DATA_UPDATE = auto()  # Order book changed, broadcast to clients
    REFRESH_MARKET_DATA = auto()


@dataclass(frozen=True)
class Event:
    """
    Base event wrapper for all events in the system.

    Attributes:
        type: The event type enum
        payload: Event-specific data
        timestamp: When the event was created
        correlation_id: Unique ID to trace related events through the system
    """
    type: EventType
    payload: Dict[str, Any]
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))


# ============================================================================
# Event Payload Dataclasses
# These provide type-safe structures for event payloads
# ============================================================================

@dataclass(frozen=True)
class RawOrderPayload:
    """
    Payload for RAW_ORDER events.
    Sent by BroadcastingService after authenticating the user.
    """
    ticker: str
    side: OrderSide
    order_type: OrderType
    quantity: int
    price: Optional[float]  # None for market orders
    user_id: str            # Firebase UID
    email: str
    websocket_id: str       # To send response back to correct client

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": self.quantity,
            "price": self.price,
            "user_id": self.user_id,
            "email": self.email,
            "websocket_id": self.websocket_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RawOrderPayload":
        return cls(
            ticker=data["ticker"],
            side=OrderSide(data["side"]) if isinstance(
                data["side"], str) else data["side"],
            order_type=OrderType(data["order_type"]) if isinstance(
                data["order_type"], str) else data["order_type"],
            quantity=data["quantity"],
            price=data.get("price"),
            user_id=data["user_id"],
            email=data["email"],
            websocket_id=data["websocket_id"],
        )


@dataclass(frozen=True)
class ValidatedOrderPayload:
    """
    Payload for VALIDATED_ORDER events.
    Sent by RiskEngine after order passes all validation checks.
    """
    ticker: str
    side: OrderSide
    order_type: OrderType
    quantity: int
    price: float            # For market orders, this is the estimated execution price
    user_id: str
    email: str
    account_id: int         # Resolved DB account ID
    websocket_id: str
    estimated_value: float  # price * quantity (for cash reservation)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": self.quantity,
            "price": self.price,
            "user_id": self.user_id,
            "email": self.email,
            "account_id": self.account_id,
            "websocket_id": self.websocket_id,
            "estimated_value": self.estimated_value,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ValidatedOrderPayload":
        return cls(
            ticker=data["ticker"],
            side=OrderSide(data["side"]) if isinstance(
                data["side"], str) else data["side"],
            order_type=OrderType(data["order_type"]) if isinstance(
                data["order_type"], str) else data["order_type"],
            quantity=data["quantity"],
            price=data["price"],
            user_id=data["user_id"],
            email=data["email"],
            account_id=data["account_id"],
            websocket_id=data["websocket_id"],
            estimated_value=data["estimated_value"],
        )


@dataclass(frozen=True)
class RefreshBookPayload:
    """
    Payload for calling the matching engine to refresh the book.
    This calls a refresh of orders from the database into memory.
    """
    ticker: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RefreshBookPayload":
        return cls(ticker=data["ticker"])


@dataclass(frozen=True)
class OrderRejectedPayload:
    """
    Payload for ORDER_REJECTED events.
    Sent by RiskEngine when order fails validation.
    """
    ticker: str
    side: OrderSide
    order_type: OrderType
    quantity: int
    price: Optional[float]
    user_id: str
    websocket_id: str
    rejection_reason: str
    rejection_code: str     # e.g., "INSUFFICIENT_FUNDS", "INSUFFICIENT_SHARES"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker,
            "side": self.side.value,
            "order_type": self.order_type.value,
            "quantity": self.quantity,
            "price": self.price,
            "user_id": self.user_id,
            "websocket_id": self.websocket_id,
            "rejection_reason": self.rejection_reason,
            "rejection_code": self.rejection_code,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderRejectedPayload":
        return cls(
            ticker=data["ticker"],
            side=OrderSide(data["side"]) if isinstance(
                data["side"], str) else data["side"],
            order_type=OrderType(data["order_type"]) if isinstance(
                data["order_type"], str) else data["order_type"],
            quantity=data["quantity"],
            price=data.get("price"),
            user_id=data["user_id"],
            websocket_id=data["websocket_id"],
            rejection_reason=data["rejection_reason"],
            rejection_code=data["rejection_code"],
        )


@dataclass(frozen=True)
class TradeExecutedPayload:
    """
    Payload for TRADE_EXECUTED events.
    Sent by MatchingEngine when a match occurs.
    """
    trade_id: str           # Generated UUID for the trade
    ticker: str
    price: float
    quantity: int
    buyer_order_id: str
    seller_order_id: str
    buyer_account_id: int
    seller_account_id: int
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "trade_id": self.trade_id,
            "ticker": self.ticker,
            "price": self.price,
            "quantity": self.quantity,
            "buyer_order_id": self.buyer_order_id,
            "seller_order_id": self.seller_order_id,
            "buyer_account_id": self.buyer_account_id,
            "seller_account_id": self.seller_account_id,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TradeExecutedPayload":
        timestamp = data["timestamp"]
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        return cls(
            trade_id=data["trade_id"],
            ticker=data["ticker"],
            price=data["price"],
            quantity=data["quantity"],
            buyer_order_id=data["buyer_order_id"],
            seller_order_id=data["seller_order_id"],
            buyer_account_id=data["buyer_account_id"],
            seller_account_id=data["seller_account_id"],
            timestamp=timestamp,
        )


@dataclass(frozen=True)
class OrderBookLevel:
    """Single price level in the order book."""
    price: float
    quantity: int


@dataclass(frozen=True)
class MarketDataUpdatePayload:
    """
    Payload for MARKET_DATA_UPDATE events.
    Sent by MatchingEngine after any book change.
    """
    ticker: str
    bids: List[tuple]       # List of (price, quantity) tuples, best first
    asks: List[tuple]       # List of (price, quantity) tuples, best first
    total_bid_quantity: int
    total_ask_quantity: int
    best_bid: Optional[float]
    best_ask: Optional[float]
    mid_price: Optional[float]
    last_trade_price: Optional[float]
    last_trade_quantity: Optional[int]
    timestamp: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ticker": self.ticker,
            "bids": self.bids,
            "asks": self.asks,
            "total_bid_quantity": self.total_bid_quantity,
            "total_ask_quantity": self.total_ask_quantity,
            "best_bid": self.best_bid,
            "best_ask": self.best_ask,
            "mid_price": self.mid_price,
            "last_trade_price": self.last_trade_price,
            "last_trade_quantity": self.last_trade_quantity,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MarketDataUpdatePayload":
        timestamp = data["timestamp"]
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        return cls(
            ticker=data["ticker"],
            bids=data["bids"],
            asks=data["asks"],
            total_bid_quantity=data["total_bid_quantity"],
            total_ask_quantity=data["total_ask_quantity"],
            best_bid=data.get("best_bid"),
            best_ask=data.get("best_ask"),
            mid_price=data.get("mid_price"),
            last_trade_price=data.get("last_trade_price"),
            last_trade_quantity=data.get("last_trade_quantity"),
            timestamp=timestamp,
        )


@dataclass(frozen=True)
class OrderPersistedPayload:
    """
    Payload for ORDER_PERSISTED events.
    Sent by PersistenceService after successfully writing order to DB.
    """
    order_id: str
    ticker: str
    account_id: int
    websocket_id: str
    status: OrderStatus

    def to_dict(self) -> Dict[str, Any]:
        return {
            "order_id": self.order_id,
            "ticker": self.ticker,
            "account_id": self.account_id,
            "websocket_id": self.websocket_id,
            "status": self.status.value,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OrderPersistedPayload":
        return cls(
            order_id=data["order_id"],
            ticker=data["ticker"],
            account_id=data["account_id"],
            websocket_id=data["websocket_id"],
            status=OrderStatus(data["status"]) if isinstance(
                data["status"], str) else data["status"],
        )
