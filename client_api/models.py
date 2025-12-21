"""
Data models for the QuantX client SDK.

These dataclasses represent the structured data received from the server.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List

from .constants import OrderSide, OrderType, OrderStatus


@dataclass
class OrderBookLevel:
    """A single price level in the order book."""
    price: float
    quantity: int


@dataclass
class MarketData:
    """Order book snapshot for a ticker."""
    ticker: str
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    mid_price: Optional[float] = None
    last_trade_price: Optional[float] = None
    last_trade_quantity: Optional[int] = None
    total_bid_quantity: int = 0
    total_ask_quantity: int = 0
    
    @classmethod
    def from_dict(cls, data: dict) -> "MarketData":
        """Parse market data from server response."""
        bids = [OrderBookLevel(price=b["price"], quantity=b["quantity"]) for b in data.get("bids", [])]
        asks = [OrderBookLevel(price=a["price"], quantity=a["quantity"]) for a in data.get("asks", [])]
        
        return cls(
            ticker=data.get("ticker", ""),
            bids=bids,
            asks=asks,
            best_bid=data.get("best_bid"),
            best_ask=data.get("best_ask"),
            mid_price=data.get("mid_price"),
            last_trade_price=data.get("last_trade_price"),
            last_trade_quantity=data.get("last_trade_quantity"),
            total_bid_quantity=data.get("total_bid_quantity", 0),
            total_ask_quantity=data.get("total_ask_quantity", 0),
        )


@dataclass
class Trade:
    """A trade that occurred on the exchange."""
    id: str
    ticker: str
    price: float
    quantity: int
    buyer_order_id: Optional[str] = None
    seller_order_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> "Trade":
        """Parse trade from server response."""
        timestamp = None
        if data.get("timestamp"):
            try:
                timestamp = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass
        
        return cls(
            id=str(data.get("id", data.get("trade_id", ""))),
            ticker=data.get("ticker", data.get("symbol", "")),
            price=float(data.get("price", 0)),
            quantity=int(data.get("quantity", 0)),
            buyer_order_id=data.get("buyer_order_id"),
            seller_order_id=data.get("seller_order_id"),
            timestamp=timestamp,
        )


@dataclass
class Order:
    """An order in the system."""
    id: str
    ticker: str
    side: OrderSide
    quantity: int
    price: float
    order_type: OrderType
    status: OrderStatus
    filled_quantity: int = 0
    remaining_quantity: int = 0
    created_at: Optional[datetime] = None
    
    @classmethod
    def from_dict(cls, data: dict) -> "Order":
        """Parse order from server response."""
        created_at = None
        if data.get("created_at"):
            try:
                created_at = datetime.fromisoformat(data["created_at"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass
        
        # Handle side - could be "Buy"/"Sell" or "BUY"/"SELL"
        side_str = data.get("side", data.get("type", "Buy"))
        side = OrderSide.BUY if side_str.upper() in ("BUY", "B") else OrderSide.SELL
        
        # Handle order type
        order_type_str = data.get("order_type", data.get("orderType", "LIMIT"))
        order_type = OrderType.LIMIT if order_type_str.upper() == "LIMIT" else OrderType.MARKET
        
        # Handle status
        status_str = data.get("status", "PENDING").upper()
        try:
            status = OrderStatus(status_str)
        except ValueError:
            status = OrderStatus.PENDING
        
        quantity = int(data.get("quantity", 0))
        filled = int(data.get("filled_quantity", 0))
        remaining = int(data.get("remaining_quantity", quantity - filled))
        
        return cls(
            id=str(data.get("id", data.get("order_id", ""))),
            ticker=data.get("ticker", data.get("symbol", "")),
            side=side,
            quantity=quantity,
            price=float(data.get("price", 0)),
            order_type=order_type,
            status=status,
            filled_quantity=filled,
            remaining_quantity=remaining,
            created_at=created_at,
        )


@dataclass
class AuthInfo:
    """Authentication information returned after successful auth."""
    account_id: int
    username: str
    
    @classmethod
    def from_dict(cls, data: dict) -> "AuthInfo":
        """Parse auth info from server response."""
        return cls(
            account_id=int(data.get("account_id", 0)),
            username=data.get("username", ""),
        )


@dataclass
class ErrorInfo:
    """Error information from the server."""
    error_type: str
    error_message: str
    
    @classmethod
    def from_dict(cls, data: dict) -> "ErrorInfo":
        """Parse error from server response."""
        return cls(
            error_type=data.get("error_type", "UNKNOWN_ERROR"),
            error_message=data.get("error_message", "An unknown error occurred"),
        )

