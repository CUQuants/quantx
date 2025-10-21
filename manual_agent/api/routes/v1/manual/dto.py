from typing import Optional, Literal
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict

class OrderCreate(BaseModel):
    account_id: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    side: Literal["BUY", "SELL"]
    quantity: float = Field(gt=0)
    limit_price: Optional[float] = Field(default=None, gt=0)

class OrderOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    account_id: str
    symbol: str
    side: str
    quantity: float
    limit_price: Optional[float]
    status: str
    created_at: datetime
