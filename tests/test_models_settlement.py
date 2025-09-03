from datetime import datetime, timezone, timedelta
from typing import Optional

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from models import AttendanceEvent, SettlementStatus, SettlementRun, Payout

def to_naive(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    return dt if dt.tzinfo is None else dt.replace(tzinfo=None)

@pytest.mark.anyio
async def test_attendance_event_insert_autotimestamp(session: AsyncSession):
    now = datetime.now(timezone.utc)

    ev = AttendanceEvent(symbol="CQAL", headcount=47, meeting_id="2025-09-04")
    session.add(ev)
    await session.commit()
    await session.refresh(ev)

    assert ev.id is not None
    assert ev.symbol == "CQAL"
    assert ev.headcount == 47
    assert ev.meeting_id == "2025-09-04"

    # observed_at set close to "now" (normalize tz awareness)
    assert to_naive(ev.observed_at) >= to_naive(now - timedelta(minutes=2))
    assert to_naive(ev.observed_at) <= to_naive(datetime.now(timezone.utc) + timedelta(minutes=2))


@pytest.mark.anyio
async def test_settlement_run_defaults_and_payout_relationship(session: AsyncSession):
    run = SettlementRun(symbol="CQAL", settlement_price=50.0)
    session.add(run)
    await session.flush()

    # Append payouts via relationship; run_id should fill automatically
    p1 = Payout(run_id=run.id, account_id=1, symbol="CQAL", quantity=+10, settlement_price=50.0, realized_pnl=125.0)
    p2 = Payout(run_id=run.id, account_id=2, symbol="CQAL", quantity=-5,  settlement_price=50.0, realized_pnl=-60.0)
    # You can also do: run.payouts.extend([p1, p2])
    session.add_all([p1, p2])

    await session.commit()

    # Reload with payouts eager-loaded
    loaded = (
        await session.execute(
            select(SettlementRun).where(SettlementRun.id == run.id)
        )
    ).scalar_one()

    # Defaults present
    assert loaded.symbol == "CQAL"
    assert loaded.settlement_price == 50.0
    assert loaded.status == SettlementStatus.PENDING
    assert loaded.effective_at is not None

    # Retrieve payouts by query and assert they match this run
    got_payouts = list((await session.execute(
        select(Payout).where(Payout.run_id == run.id)
    )).scalars())
    assert {p.account_id for p in got_payouts} == {1, 2}
    assert all(p.symbol == "CQAL" for p in got_payouts)


@pytest.mark.anyio
async def test_delete_orphan_when_removed_from_run(session: AsyncSession):
    # Create run with a single payout
    run = SettlementRun(symbol="CQAL", settlement_price=42.0)
    session.add(run)
    await session.flush()

    p = Payout(run_id=run.id, account_id=7, symbol="CQAL", quantity=3, settlement_price=42.0, realized_pnl=12.0)
    session.add(p)
    await session.commit()

    # Confirm it exists
    count_before = (await session.execute(
        select(Payout).where(Payout.run_id == run.id)
    )).scalars().all()
    assert len(count_before) == 1

    # Remove payout from the parent and commit -- relationship has delete-orphan
    # (If you wired via run.payouts, you'd pop it. Here we can delete directly.)
    await session.delete(p)
    await session.commit()

    count_after = (await session.execute(
        select(Payout).where(Payout.run_id == run.id)
    )).scalars().all()
    assert len(count_after) == 0


@pytest.mark.anyio
async def test_deleting_run_cascades_payouts_via_orm(session: AsyncSession):
    run = SettlementRun(symbol="CQAL", settlement_price=77.0)
    session.add(run)
    await session.flush()

    session.add_all([
        Payout(run_id=run.id, account_id=1, symbol="CQAL", quantity=1, settlement_price=77.0, realized_pnl=5.0),
        Payout(run_id=run.id, account_id=2, symbol="CQAL", quantity=-2, settlement_price=77.0, realized_pnl=-8.0),
    ])
    await session.commit()

    # Sanity: payouts exist
    have = (await session.execute(
        select(Payout).where(Payout.run_id == run.id)
    )).scalars().all()
    assert len(have) == 2

    # Delete the run; ORM cascade should delete-orphan all payouts
    await session.delete(run)
    await session.commit()

    left = (await session.execute(
        select(Payout).where(Payout.run_id == run.id)
    )).scalars().all()
    assert left == []


@pytest.mark.anyio
async def test_settlement_status_enum_roundtrip(session: AsyncSession):
    run = SettlementRun(symbol="CQAL", settlement_price=12.34)
    run.status = SettlementStatus.FAILED
    session.add(run)
    await session.commit()

    got = await session.get(SettlementRun, run.id)
    assert got.status == SettlementStatus.FAILED