import uuid
from datetime import datetime, timezone, timedelta

import pytest

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Payout, AccountRole, Account, Position, SettlementStatus, OptionType, Option, Equity, Future
from services.settlement_service import perform_settlement


@pytest.mark.anyio
async def test_futures_settlement_updates_balances_positions_and_payouts(session: AsyncSession):
    # --- Setup instrument (Future with multiplier) ---
    und = Equity(symbol="UND_CQALF", is_active=True, name="Underlying", spot_price=100.0, dividend_yield=0.0, sector="Test")
    session.add(und); await session.flush()
    fut = Future(
        symbol="CQALF",
        is_active=True,
        underlying_id=und.id,
        expiry=datetime.now(timezone.utc) + timedelta(days=30),
        multiplier=100.0,        # important for PnL
    )
    session.add(fut); await session.flush()

    # --- Two accounts & positions (long + short) ---
    a1 = Account(username="fut_long", role=AccountRole.USER, balance=1000.0)
    a2 = Account(username="fut_short", role=AccountRole.USER, balance=1000.0)
    session.add_all([a1, a2]); await session.flush()

    # Long +2 @ 50, Short -1 @ 50
    p1 = Position(account_id=a1.id, symbol="CQALF", quantity=+2, average_price=50.0)
    p2 = Position(account_id=a2.id, symbol="CQALF", quantity=-1, average_price=50.0)
    session.add_all([p1, p2]); await session.commit()

    # --- Run settlement at 55.0 ---
    run = await perform_settlement(session, symbol="CQALF", settlement_price=55.0)

    assert run.status == SettlementStatus.COMPLETED
    assert run.symbol == "CQALF"
    assert run.settlement_price == 55.0

    # Reload accounts / positions
    a1r = await session.get(Account, a1.id)
    a2r = await session.get(Account, a2.id)
    p1r = await session.get(Position, p1.id)
    p2r = await session.get(Position, p2.id)

    # PnL: (55-50)*multiplier*qty
    # a1: (5)*100*2 = +1000 ; a2: (5)*100*(-1) = -500
    assert a1r.balance == pytest.approx(1000.0 + 1000.0)
    assert a2r.balance == pytest.approx(1000.0 - 500.0)

    # Positions flattened and unrealized cleared
    assert p1r.quantity == 0 and p1r.unrealized_pnl == 0.0 and p1r.average_price == 0.0
    assert p2r.quantity == 0 and p2r.unrealized_pnl == 0.0 and p2r.average_price == 0.0

    # Payout rows exist and match totals
    pays = list((await session.execute(select(Payout).where(Payout.run_id == run.id))).scalars())
    assert {p.account_id for p in pays} == {a1.id, a2.id}
    got = {p.account_id: p.realized_pnl for p in pays}
    assert got[a1.id] == pytest.approx(+1000.0)
    assert got[a2.id] == pytest.approx(-500.0)


@pytest.mark.anyio
async def test_options_settlement_call_updates_balances_positions_and_payouts(session: AsyncSession):
    # --- Underlying equity + CALL option ---
    und = Equity(symbol="UND_OPT", is_active=True, name="Underlying", spot_price=100.0, dividend_yield=0.0, sector="Test")
    session.add(und); await session.flush()

    opt = Option(
        symbol="OPT1",
        is_active=True,
        underlying_id=und.id,
        strike=100.0,
        expiry=datetime.now(timezone.utc) + timedelta(days=15),
        option_type=OptionType.CALL,
        multiplier=100.0,
        implied_volatility=0.25,
        open_interest=0.0, volume=0,
    )
    session.add(opt); await session.flush()

    # --- Two accounts & positions ---
    a1 = Account(username="opt_long", role=AccountRole.USER, balance=1000.0)
    a2 = Account(username="opt_short", role=AccountRole.USER, balance=1000.0)
    session.add_all([a1, a2]); await session.flush()

    # Interpret average_price as premium *already* in currency units (NOT per share),
    # consistent with service: realized = (payoff - avg) * qty, where payoff includes multiplier.
    # Long +1, paid $2.00 * 100 = 200; Short -2, received $1.50 * 100 = 150 per contract.
    p1 = Position(account_id=a1.id, symbol="OPT1", quantity=+1, average_price=200.0)
    p2 = Position(account_id=a2.id, symbol="OPT1", quantity=-2, average_price=150.0)
    session.add_all([p1, p2]); await session.commit()

    # Settlement at 110 => payoff per contract: (110-100)*100 = 1000
    run = await perform_settlement(session, symbol="OPT1", settlement_price=110.0)
    assert run.status == SettlementStatus.COMPLETED

    a1r = await session.get(Account, a1.id)
    a2r = await session.get(Account, a2.id)
    p1r = await session.get(Position, p1.id)
    p2r = await session.get(Position, p2.id)

    # Realized:
    # long: (1000 - 200) * 1 = +800
    # short: (1000 - 150) * (-2) = -1700
    assert a1r.balance == pytest.approx(1000.0 + 800.0)
    assert a2r.balance == pytest.approx(1000.0 - 1700.0)

    # Positions flattened
    assert p1r.quantity == 0 and p1r.unrealized_pnl == 0.0 and p1r.average_price == 0.0
    assert p2r.quantity == 0 and p2r.unrealized_pnl == 0.0 and p2r.average_price == 0.0

    pays = list((await session.execute(select(Payout).where(Payout.run_id == run.id))).scalars())
    got = {p.account_id: p.realized_pnl for p in pays}
    assert got[a1.id] == pytest.approx(+800.0)
    assert got[a2.id] == pytest.approx(-1700.0)


@pytest.mark.anyio
async def test_default_semantics_when_instrument_missing(session: AsyncSession):
    # No Instrument row for this symbol -> futures-like with multiplier 1.0
    a = Account(username="ghost", role=AccountRole.USER, balance=1000.0)
    session.add(a); await session.flush()

    p = Position(account_id=a.id, symbol="VIRTUAL", quantity=+3, average_price=10.0)
    session.add(p); await session.commit()

    # Settle at 12.00 => (12 - 10) * 3 * 1.0 = +6
    run = await perform_settlement(session, symbol="VIRTUAL", settlement_price=12.0)
    assert run.status == SettlementStatus.COMPLETED

    ar = await session.get(Account, a.id)
    pr = await session.get(Position, p.id)
    assert ar.balance == pytest.approx(1000.0 + 6.0)
    assert pr.quantity == 0 and pr.unrealized_pnl == 0.0 and pr.average_price == 0.0

    pays = list((await session.execute(select(Payout).where(Payout.run_id == run.id))).scalars())
    assert len(pays) == 1 and pays[0].realized_pnl == pytest.approx(6.0)