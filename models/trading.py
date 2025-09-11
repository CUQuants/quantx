from datetime import datetime, timezone
from typing import Optional, List
import uuid
from enum import Enum

import sqlalchemy
from sqlalchemy import Integer, String, Float, DateTime
from sqlalchemy.orm import Mapped, mapped_column, relationship, foreign
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql import func
from sqlalchemy import UniqueConstraint

from . import Base


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    LIMIT = "limit"
    MARKET = "market"


class OrderStatus(str, Enum):
    PENDING = "pending"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    account_id: Mapped[int] = mapped_column(ForeignKey(
        "accounts.id", ondelete="CASCADE"), index=True, nullable=False)

    symbol: Mapped[str] = mapped_column(String, default="CQAF")
    side: Mapped[OrderSide] = mapped_column(
        sqlalchemy.Enum(OrderSide), nullable=False)
    type: Mapped[OrderType] = mapped_column(
        sqlalchemy.Enum(OrderType), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[Optional[float]] = mapped_column(
        Float, nullable=True)  # null for market orders
    filled_quantity: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[OrderStatus] = mapped_column(
        sqlalchemy.Enum(OrderStatus), default=OrderStatus.PENDING)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.now(timezone.utc), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.now(
        timezone.utc), onupdate=func.now(), nullable=False)

    account: Mapped["Account"] = relationship(
        "Account",
        back_populates="orders",
        lazy="selectin",
    )

    buy_trades: Mapped[list["Trade"]] = relationship(
        "Trade",
        primaryjoin="Order.id == foreign(Trade.buy_order_id)",
        foreign_keys="[Trade.buy_order_id]",
        back_populates="buy_order",
        cascade="all, delete-orphan",
        lazy="selectin",
        passive_deletes=True,
    )

    sell_trades: Mapped[list["Trade"]] = relationship(
        "Trade",
        primaryjoin="Order.id == foreign(Trade.sell_order_id)",
        foreign_keys="[Trade.sell_order_id]",
        back_populates="sell_order",
        cascade="all, delete-orphan",
        lazy="selectin",
        passive_deletes=True,
    )

    @property
    def trades(self) -> List["Trade"]:
        return [*self.buy_trades, *self.sell_trades]

    @property
    def remaining_quantity(self) -> int:
        return self.quantity - self.filled_quantity


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


class Position(Base):
    __tablename__ = "positions"
    __table_args__ = (
        UniqueConstraint("account_id", "symbol",
                         name="uq_position_account_symbol"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey(
        "accounts.id", ondelete="CASCADE"), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String, default="CQAF", index=True)
    quantity: Mapped[int] = mapped_column(Integer, default=0)
    average_price: Mapped[float] = mapped_column(Float, default=0.0)
    unrealized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.now(timezone.utc),
        onupdate=func.now(),
        nullable=False,
        index=True,
    )

    account: Mapped["Account"] = relationship(
        "Account",
        primaryjoin="Account.id == foreign(Position.account_id)",
        foreign_keys="[Position.account_id]",
        back_populates="positions",
        lazy="selectin",
    )
