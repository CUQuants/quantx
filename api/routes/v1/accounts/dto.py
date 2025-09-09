from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel
from pydantic.v1 import Field

from api.routes.v1.trades.dto import TradeDTO, OrderDTO, PositionDTO
from models import AccountRole, OrderStatus


class AccountDTO(BaseModel):
    id: int
    username: str
    balance: float
    role: AccountRole
    last_login_at: Optional[datetime]

#Be weary of use, super duper expensive to query
class FullAccountDTO(BaseModel):
    account: AccountDTO
    orders: List[OrderDTO]
    trades_affecting: List[TradeDTO]
    positions: List[PositionDTO]

#DTOs for Paginated Results; Orders, Trades, etc.
class PaginatedResult(BaseModel):
    page: int
    page_size: int

class OrdersResult(PaginatedResult):
    orders: List[OrderDTO]

class TradesResult(PaginatedResult):
    trades: List[TradeDTO]

class PositionsResult(PaginatedResult):
    positions: List[PositionDTO]

#Query Parameter Schemas for Orders,Trades,Positions filtering
class PaginatedFilters(BaseModel):
    after: Optional[datetime] = None
    before: Optional[datetime] = None
    page: Optional[int] = 0
    page_size: int = 50

class OrdersFilters(PaginatedFilters):
    status: Optional[OrderStatus] = None
    symbol: Optional[str] = None

class TradesFilters(PaginatedFilters):
    symbol: Optional[str] = None
    quantity: Optional[int] = None

class PositionsFilters(PaginatedFilters):
    symbol: Optional[str] = None
    quantity: Optional[int] = None

#Post/Patch Request/Response Schemas
class AccountUpdateBalanceRequest(BaseModel):
    balance: float