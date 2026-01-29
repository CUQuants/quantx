from datetime import datetime

from pydantic import BaseModel, ConfigDict, computed_field
from typing import Literal

from models import OrderSide, OrderType, OrderStatus


class OrderDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id: str
    symbol: str
    side: OrderSide
    type: OrderType
    quantity: int
    price: float
    status: OrderStatus
    filled_quantity: int
    created_at: datetime
    updated_at: datetime
    
    @computed_field
    @property
    def remaining_quantity(self) -> int:
        """Compute remaining quantity from quantity and filled_quantity."""
        return self.quantity - self.filled_quantity


class TradeDTO(BaseModel):

    model_config = ConfigDict(from_attributes=True, extra='ignore')

    id: str
    symbol: str
    quantity: int
    price: float
    trade_value: float
    created_at: datetime
    buy_account_id: int
    sell_account_id: int


class PositionDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')

    id: int
    symbol: str
    quantity: int
    average_price: float
    unrealized_pnl: float
    realized_pnl: float
    updated_at: datetime


class TradeRequest(BaseModel):
    symbol: str
    quantity: int
    price: float
    value: float
    order_type: OrderSide
    buy_order_id: str
    sell_order_id: str
