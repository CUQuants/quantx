"""
Constants and enums for the QuantX client SDK.
"""

from enum import Enum


class EventType(str, Enum):
    """Event types emitted by the client."""
    
    # Connection lifecycle
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    AUTHENTICATED = "authenticated"
    ERROR = "error"
    
    # Market data (public stream)
    MARKET_DATA = "market_data"
    TRADE = "trade"
    
    # Private order events
    ORDER_ACCEPTED = "order_accepted"
    ORDER_REJECTED = "order_rejected"
    ORDER_FILLED = "order_filled"
    ORDER_CANCELLED = "order_cancelled"
    
    # Subscription confirmations
    SUBSCRIBED = "subscribed"


class OrderSide(str, Enum):
    """Order side (buy or sell)."""
    BUY = "Buy"
    SELL = "Sell"


class OrderType(str, Enum):
    """Order type."""
    LIMIT = "LIMIT"
    MARKET = "MARKET"


class OrderStatus(str, Enum):
    """Order status values."""
    PENDING = "PENDING"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"

