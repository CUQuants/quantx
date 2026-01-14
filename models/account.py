from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import sqlalchemy
from sqlalchemy import Integer, String, Float, DateTime, Boolean
# <-- import foreign
from sqlalchemy.orm import Mapped, mapped_column, relationship, foreign

from . import Base


class AccountRole(str, Enum):
    OWNER = "owner"
    ADMIN = "admin"
    MODERATOR = "moderator"
    USER = "user"
    AGENT = "agent"


class Account(Base):
    __tablename__ = "accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    username: Mapped[str] = mapped_column(
        String, unique=True, index=True, nullable=False)
    firebase_uid: Mapped[Optional[str]] = mapped_column(
        String, unique=True, index=True, nullable=True)
    balance: Mapped[float] = mapped_column(Float, default=1000.0)
    available_cash: Mapped[float] = mapped_column(Float, default=1000.0)
    role: Mapped[AccountRole] = mapped_column(
        sqlalchemy.Enum(AccountRole), default=AccountRole.USER)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.now(timezone.utc))
    last_login_at: Mapped[Optional[datetime]
                          ] = mapped_column(DateTime(timezone=True), nullable=True, default=datetime.now(timezone.utc))

    orders: Mapped[list["Order"]] = relationship(
        "Order",
        back_populates="account",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="selectin",
        primaryjoin="Account.id == foreign(Order.account_id)",
    )

    sell_trades: Mapped[list["Trade"]] = relationship(
        "Trade",
        back_populates="sell_account",
        cascade="all, delete-orphan",
        passive_deletes=True,  # keep only if Trade.account_id has ondelete="CASCADE"
        lazy="selectin",
        primaryjoin="Account.id == foreign(Trade.sell_account_id)",
    )

    buy_trades: Mapped[list["Trade"]] = relationship(
        "Trade",
        back_populates="buy_account",
        cascade="all, delete-orphan",
        passive_deletes=True,  # keep only if Trade.account_id has ondelete="CASCADE"
        lazy="selectin",
        primaryjoin="Account.id == foreign(Trade.buy_account_id)",
    )

    positions: Mapped[list["Position"]] = relationship(
        "Position",
        back_populates="account",
        cascade="all, delete-orphan",
        passive_deletes=True,  # keep only if Position.account_id has ondelete="CASCADE"
        lazy="selectin",
        primaryjoin="Account.id == foreign(Position.account_id)",
    )

    api_keys: Mapped[list["ApiKey"]] = relationship(
        "ApiKey",
        back_populates="account",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="selectin",
    )
