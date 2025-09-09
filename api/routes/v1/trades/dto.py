from datetime import datetime

from pydantic import BaseModel

from models import OrderSide, OrderType, OrderStatus

class OrderDTO(BaseModel):
    id: str
    symbol: str
    side: OrderSide
    type: OrderType
    quantity: int
    price: float
    status: OrderStatus
    remaining_quantity: int
    created_at: datetime
    updated_at: datetime

class TradeDTO(BaseModel):
    id: str
    buy_order: OrderDTO
    sell_order: OrderDTO
    symbol: str
    quantity: int
    price: float
    value: float
    created_at: datetime

class PositionDTO(BaseModel):
    id: str
    symbol: str
    quantity: int
    average_price: float
    unrealized_pnl: float
    realized_pnl: float
    updated_at: datetime