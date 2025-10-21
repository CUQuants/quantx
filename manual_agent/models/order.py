from typing import Optional
from datetime import datetime
from enum import Enum as PyEnum
from sqlalchemy import String, Integer, Float, DateTime, Enum
from sqlalchemy.orm import Mapped, mapped_column
from ..api.db import Base  # manual_agent.api.db

class Side(str, PyEnum):
    BUY = "BUY"
    SELL = "SELL"

class OrderStatus(str, PyEnum):
    NEW = "NEW"
    CANCELLED = "CANCELLED"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"

class ManualOrder(Base):
    __tablename__ = "manual_orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    account_id: Mapped[str] = mapped_column(String(64), index=True)
    symbol: Mapped[str] = mapped_column(String(24), index=True)
    side: Mapped[Side] = mapped_column(Enum(Side), nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    limit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True) 
    status: Mapped[OrderStatus] = mapped_column(Enum(OrderStatus), default=OrderStatus.NEW, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
