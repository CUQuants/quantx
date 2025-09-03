from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from models import Equity, Instrument, InstrumentType, Option, OptionType, Future

def utc_now():
    return datetime.now(timezone.utc)

@pytest.mark.anyio
async def test_equity_insert_and_discriminator(session):
    aapl = Equity(
        symbol="AAPL",
        type=InstrumentType.EQUITY,
        is_active=True,
        name="Apple Inc.",
        spot_price=190.0,
        dividend_yield=0.0,
        sector="Tech",
    )
    session.add(aapl)
    await session.commit()

    # Fetch via base class; should return Equity instance (polymorphic)
    inst = await session.get(Instrument, aapl.id)
    assert inst is not None
    assert inst.type == InstrumentType.EQUITY
    assert isinstance(inst, Equity)
    assert inst.symbol == "AAPL"

@pytest.mark.anyio
async def test_symbol_uniqueness_violation(session):
    e1 = Equity(symbol="DUPL", type=InstrumentType.EQUITY, is_active=True, name="X", spot_price=1, dividend_yield=0, sector="Y")
    e2 = Equity(symbol="DUPL", type=InstrumentType.EQUITY, is_active=True, name="Z", spot_price=2, dividend_yield=0, sector="Y")
    session.add_all([e1, e2])
    with pytest.raises(IntegrityError):
        await session.commit()

@pytest.mark.anyio
async def test_option_underlying_can_be_equity(session):
    spy = Equity(
        symbol="SPY",
        type=InstrumentType.EQUITY,
        is_active=True,
        name="SPDR S&P 500 ETF",
        spot_price=500.0,
        dividend_yield=0.0,
        sector="ETF",
    )
    session.add(spy)
    await session.flush()

    call = Option(
        symbol="SPY250919C00500000",
        type=InstrumentType.OPTION,
        is_active=True,
        underlying_id=spy.id,             # Option points to instruments.id of Equity
        strike=500.0,
        expiry=utc_now() + timedelta(days=20),
        option_type=OptionType.CALL,
        multiplier=100.0,
        implied_volatility=0.25,
        open_interest=1234.0,
        volume=0,
    )
    session.add(call)
    await session.commit()

    # Re-query with eager load to avoid async lazy I/O
    opt = (
        await session.execute(
            select(Option)
            .options(selectinload(Option.underlying))
            .where(Option.id == call.id)
        )
    ).scalar_one()

    assert isinstance(opt, Option)
    assert opt.type == InstrumentType.OPTION
    assert opt.underlying is not None
    # polymorphic: underlying is Equity instance
    assert isinstance(opt.underlying, Equity)
    assert opt.underlying.id == spy.id
    assert opt.underlying.symbol == "SPY"

@pytest.mark.anyio
async def test_future_relationship_points_to_equity(session):
    ndx = Equity(
        symbol="NDX",
        type=InstrumentType.EQUITY,
        is_active=True,
        name="Nasdaq 100",
        spot_price=17000.0,
        dividend_yield=0.0,
        sector="Index",
    )
    session.add(ndx)
    await session.flush()

    fut = Future(
        symbol="NQZ5",
        type=InstrumentType.FUTURE,
        is_active=True,
        underlying_id=ndx.id,  # Future points to instruments.id of Equity
        expiry=utc_now() + timedelta(days=90),
        multiplier=20.0,
        initial_margin_rate=0.1,
        maintenance_margin_rate=0.08,
    )
    session.add(fut)
    await session.commit()

    fut2 = (
        await session.execute(
            select(Future)
            .options(selectinload(Future.underlying))
            .where(Future.id == fut.id)
        )
    ).scalar_one()

    assert fut2.type == InstrumentType.FUTURE
    assert fut2.underlying is not None
    assert isinstance(fut2.underlying, Equity)
    assert fut2.underlying.id == ndx.id

@pytest.mark.anyio
async def test_option_underlying_can_be_future(session):
    """
    Your schema allows Option.underlying_id -> instruments.id,
    so an option can reference a Future as its underlying.
    """
    # Create an equity and a future on it
    base = Equity(
        symbol="CQAL_EQ",
        type=InstrumentType.EQUITY,
        is_active=True,
        name="Club Attendance",
        spot_price=25.0,
        dividend_yield=0.0,
        sector="Alt",
    )
    session.add(base)
    await session.flush()

    cq_fut = Future(
        symbol="CQAL",
        type=InstrumentType.FUTURE,
        is_active=True,
        underlying_id=base.id,
        expiry=utc_now() + timedelta(days=7),
        multiplier=1.0,
    )
    session.add(cq_fut)
    await session.flush()

    opt_on_future = Option(
        symbol="CQAL-OPT-C-30",
        type=InstrumentType.OPTION,
        is_active=True,
        underlying_id=cq_fut.id,  # <-- underlying is the FUTURE instrument
        strike=30.0,
        expiry=cq_fut.expiry,
        option_type=OptionType.CALL,
        multiplier=1.0,
    )
    session.add(opt_on_future)
    await session.commit()

    # Load with eager relationship
    opt_loaded = (
        await session.execute(
            select(Option)
            .options(selectinload(Option.underlying))
            .where(Option.id == opt_on_future.id)
        )
    ).scalar_one()
    assert isinstance(opt_loaded.underlying, Future)
    assert opt_loaded.underlying.id == cq_fut.id

@pytest.mark.anyio
async def test_future_expiry_timezone(session):
    """Future.expiry uses timezone=True; verify we can store UTC-aware datetimes."""
    eq = Equity(
        symbol="TZ_EQ",
        type=InstrumentType.EQUITY,
        is_active=True,
        name="TZ",
        spot_price=1.0,
        dividend_yield=0.0,
        sector="Z",
    )
    session.add(eq)
    await session.flush()

    aware_expiry = utc_now() + timedelta(days=1)
    fut = Future(
        symbol="TZ_FUT",
        type=InstrumentType.FUTURE,
        is_active=True,
        underlying_id=eq.id,
        expiry=aware_expiry,
        multiplier=1.0,
    )
    session.add(fut)
    await session.commit()

    got = await session.get(Future, fut.id)
    # Some SQLite dialects may drop tzinfo; assert equivalence up to naive UTC
    assert got.expiry.replace(tzinfo=None) == aware_expiry.replace(tzinfo=None)

@pytest.mark.anyio
async def test_base_select_returns_subclass(session):
    """Selecting from Instrument returns concrete subclass instances."""
    eq = Equity(symbol="BASE_EQ", type=InstrumentType.EQUITY, is_active=True, name="X", spot_price=0, dividend_yield=0, sector="S")
    session.add(eq)
    await session.commit()

    row = (
        await session.execute(
            select(Instrument).where(Instrument.symbol == "BASE_EQ")
        )
    ).scalar_one()
    assert isinstance(row, Equity)
    assert row.type == InstrumentType.EQUITY