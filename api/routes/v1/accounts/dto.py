from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, ConfigDict

from api.routes.v1.trades.dto import TradeDTO, OrderDTO, PositionDTO
from api.util.pagination import PaginatedResult, PaginatedFilters
from models import AccountRole, OrderStatus


class AccountFilters(PaginatedFilters):
    role: Optional[AccountRole] = None


class AccountDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra='ignore')
    id: int
    username: str
    balance: float
    role: AccountRole
    last_login_at: Optional[datetime]


# Be weary of use, super duper expensive to query


class FullAccountDTO(BaseModel):
    account: AccountDTO
    orders: List[OrderDTO]
    trades_affecting: List[TradeDTO]
    positions: List[PositionDTO]

# DTOs for Paginated Results; Orders, Trades, etc.


class OrdersResult(PaginatedResult):
    orders: List[OrderDTO]


class TradesResult(PaginatedResult):
    trades: List[TradeDTO]


class PositionsResult(PaginatedResult):
    positions: List[PositionDTO]

# Query Parameter Schemas for Orders,Trades,Positions filtering


class OrdersFilters(PaginatedFilters):
    status: Optional[OrderStatus] = None
    symbol: Optional[str] = None


class TradesFilters(PaginatedFilters):
    symbol: Optional[str] = None
    quantity: Optional[int] = None


class PositionsFilters(PaginatedFilters):
    symbol: Optional[str] = None
    quantity: Optional[int] = None

# Post/Patch Request/Response Schemas


class AccountUpdateBalanceRequest(BaseModel):
    balance: float


class AccountUpdateRoleRequest(BaseModel):
    role: str


class CreateAccountRequest(BaseModel):
    firebase_token: str
