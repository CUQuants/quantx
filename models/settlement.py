from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List

import sqlalchemy
from sqlalchemy import Integer, String, Float, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship, foreign  # <-- import foreign

from . import Base

class SettlementStatus(str, Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"

class AttendanceEvent(Base):
    __tablename__ = "attendance_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, index=True)  # e.g. "CQAL" or event-specific symbol
    meeting_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    headcount: Mapped[int] = mapped_column(Integer)

class SettlementRun(Base):
    __tablename__ = "settlement_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, index=True)
    effective_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    settlement_price: Mapped[float] = mapped_column(Float)
    status: Mapped[SettlementStatus] = mapped_column(default=SettlementStatus.PENDING)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    payouts: Mapped[list["Payout"]] = relationship(back_populates="run", cascade="all, delete-orphan")

class Payout(Base):
    __tablename__ = "payouts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("settlement_runs.id", ondelete="CASCADE"), index=True)
    account_id: Mapped[int] = mapped_column(Integer, index=True)
    symbol: Mapped[str] = mapped_column(String, index=True)

    quantity: Mapped[int] = mapped_column(Integer)    # quantity (could be negative)
    settlement_price: Mapped[float] = mapped_column(Float)
    realized_pnl: Mapped[float] = mapped_column(Float)    # cash to credit/debit

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    run: Mapped["SettlementRun"] = relationship(back_populates="payouts")