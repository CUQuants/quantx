from datetime import datetime
from typing import Optional

from pydantic.v1 import BaseModel

from models import AccountRole


class AccountDTO(BaseModel):
    id: int
    username: str
    balance: float
    role: AccountRole
    last_login_at: Optional[datetime]

class AccountDetailsDTO(BaseModel):
    account: AccountDTO
