from typing import Optional, Callable

from sqlalchemy import select, Select, insert, Insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload, Mapped, mapped_column, DeclarativeBase
import uuid
from datetime import datetime
from sqlalchemy.sql.schema import ForeignKey
from api.routes.v1.trades.dto import TradeRequest


from models import Account, Trade, Order, Position


class Base(DeclarativeBase):
    pass


class Trade(Base):
    __tablename__ = "trades"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    buy_order_id: Mapped[str] = mapped_column(ForeignKey(
        "orders.id", ondelete="CASCADE"), nullable=False, index=True)
    sell_order_id: Mapped[str] = mapped_column(ForeignKey(
        "orders.id", ondelete="CASCADE"), nullable=False, index=True)
    account_id: Mapped[int] = mapped_column(ForeignKey(
        "accounts.id", ondelete="CASCADE"), nullable=False, index=True)

    symbol: Mapped[str] = mapped_column(String, default="CQAF", index=True)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    trade_value: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now(timezone.utc), nullable=False, index=True)

    buy_order: Mapped["Order"] = relationship(
        "Order",
        primaryjoin="Order.id == foreign(Trade.buy_order_id)",
        foreign_keys="[Trade.buy_order_id]",
        back_populates="buy_trades",   # <-- fixed
        lazy="selectin",
    )

    sell_order: Mapped["Order"] = relationship(
        "Order",
        primaryjoin="Order.id == foreign(Trade.sell_order_id)",
        foreign_keys="[Trade.sell_order_id]",
        back_populates="sell_trades",  # <-- fixed
        lazy="selectin",
    )

    account: Mapped["Account"] = relationship(
        "Account",
        primaryjoin="Account.id == foreign(Trade.account_id)",
        foreign_keys="[Trade.account_id]",
        back_populates="trades",
        lazy="selectin",
    )


get_trade_by_id: Callable[[int], Select] = \
    lambda trade_id: select(Trade).where(Trade.id == trade_id)


def get_trades():
    return select(Trade)
