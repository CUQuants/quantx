from datetime import datetime
from enum import Enum
from typing import Optional

import sqlalchemy
from sqlalchemy import Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.sql.schema import ForeignKey

from . import Base


class InstrumentType(str, Enum):
    EQUITY = "equity"
    OPTION = "option"
    FUTURE = "future"


class OptionType(str, Enum):
    CALL = "call"
    PUT = "put"

class Instrument(Base):
    __tablename__ = "instruments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    symbol: Mapped[str] = mapped_column(String, unique=True, index=True)
    type: Mapped[InstrumentType] = mapped_column(sqlalchemy.Enum(InstrumentType), index=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    __mapper_args__ = {"polymorphic_on": type}

class Equity(Instrument):
    __tablename__ = "equities"

    id: Mapped[int] = mapped_column(ForeignKey("instruments.id"), primary_key=True)
    name: Mapped[str] = mapped_column(String)
    spot_price: Mapped[float] = mapped_column(Float, default=0.0)
    dividend_yield: Mapped[float] = mapped_column(Float, default=0.0)
    sector: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    __mapper_args__ = {
        "polymorphic_identity": InstrumentType.EQUITY,
        "inherit_condition": (id == Instrument.id),
    }

class Option(Instrument):
    __tablename__ = "options"


    id: Mapped[int] = mapped_column(ForeignKey("instruments.id"), primary_key=True)
    underlying_id: Mapped[int] = mapped_column(ForeignKey("instruments.id"), index=True)

    strike: Mapped[float] = mapped_column(Float, nullable=False)
    expiry: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    option_type: Mapped[OptionType] = mapped_column(sqlalchemy.Enum(OptionType), nullable=False)
    multiplier: Mapped[float] = mapped_column(Float, default=0.0)

    implied_volatility: Mapped[Optional[float]] = mapped_column(Float)
    open_interest: Mapped[Optional[float]] = mapped_column(Float)
    volume: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    underlying: Mapped[Instrument] = relationship(foreign_keys=[underlying_id])

    __mapper_args__ = {
        "polymorphic_identity": InstrumentType.OPTION,
        "inherit_condition": (id == Instrument.id),
    }

class Future(Instrument):
    __tablename__ = "futures"

    id: Mapped[int] = mapped_column(ForeignKey("instruments.id"), primary_key=True)
    underlying_id: Mapped[int] = mapped_column(ForeignKey("instruments.id"), index=True)
    expiry: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    multiplier: Mapped[float] = mapped_column(Float, default=1.0)
    initial_margin_rate: Mapped[float] = mapped_column(Float, default=0.1)
    maintenance_margin_rate: Mapped[float] = mapped_column(Float, default=0.08)

    underlying: Mapped[Instrument] = relationship(foreign_keys=[underlying_id])

    __mapper_args__ = {
        "polymorphic_identity": InstrumentType.FUTURE,
        "inherit_condition": (id == Instrument.id),
    }