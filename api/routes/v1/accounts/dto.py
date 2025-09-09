from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel

from api.routes.v1.trades.dto import TradeDTO, OrderDTO, PositionDTO
from models import AccountRole

class AccountDTO(BaseModel):
    id: int
    username: str
    balance: float
    role: AccountRole
    last_login_at: Optional[datetime]

class AccountDetailsDTO(BaseModel):
    account: AccountDTO
    orders: List[OrderDTO]
    trades_affecting: List[TradeDTO]
    positions: List[PositionDTO]