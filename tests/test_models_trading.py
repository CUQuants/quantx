from datetime import datetime

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from models import Account, AccountRole, Order, OrderSide, OrderType, OrderStatus, Position, Trade

@pytest.mark.anyio
async def test_create_limit_order_for_account(session: AsyncSession):
    acct = Account(username="alice", role=AccountRole.USER)
    session.add(acct)
    await session.flush()

    o = Order(
        account_id=acct.id,
        symbol="CQAF",
        side=OrderSide.BUY,
        type=OrderType.LIMIT,
        quantity=5,
        price=10.0,
        status=OrderStatus.PENDING,
    )
    session.add(o)
    await session.commit()

    got = await session.get(Order, o.id)
    assert got is not None
    assert got.account_id == acct.id
    assert got.side == OrderSide.BUY
    assert got.type == OrderType.LIMIT
    assert got.status in {
        OrderStatus.PENDING, OrderStatus.PARTIAL,
        OrderStatus.FILLED, OrderStatus.CANCELED, OrderStatus.REJECTED
    }
    assert got.created_at is not None

    # Eager-load orders to avoid implicit lazy IO
    a = (
        await session.execute(
            select(Account)
            .options(selectinload(Account.orders))
            .where(Account.id == acct.id)
        )
    ).scalar_one()
    assert len(a.orders) == 1
    assert a.orders[0].id == o.id


@pytest.mark.anyio
async def test_trade_relationships_between_two_orders(session: AsyncSession):
    acct = Account(username="carol", role=AccountRole.USER)
    session.add(acct)
    await session.flush()

    obuy = Order(
        account_id=acct.id, symbol="CQAF",
        side=OrderSide.BUY, type=OrderType.LIMIT,
        quantity=2, price=10.0
    )
    osell = Order(
        account_id=acct.id, symbol="CQAF",
        side=OrderSide.SELL, type=OrderType.LIMIT,
        quantity=2, price=10.0
    )
    session.add_all([obuy, osell])
    await session.flush()

    t = Trade(
        buy_order_id=obuy.id,
        sell_order_id=osell.id,
        account_id=acct.id,
        symbol="CQAF",
        quantity=2,
        price=10.0,
        trade_value=20.0,
    )
    session.add(t)
    await session.commit()

    # Eager-load each side’s trades
    ob = (
        await session.execute(
            select(Order)
            .options(selectinload(Order.buy_trades))
            .where(Order.id == obuy.id)
        )
    ).scalar_one()
    os = (
        await session.execute(
            select(Order)
            .options(selectinload(Order.sell_trades))
            .where(Order.id == osell.id)
        )
    ).scalar_one()
    tr = await session.get(Trade, t.id)

    assert tr.buy_order.id == obuy.id
    assert tr.sell_order.id == osell.id
    assert any(tt.id == t.id for tt in ob.buy_trades)
    assert any(tt.id == t.id for tt in os.sell_trades)

    # Combined convenience property works because we loaded both sides
    combo_ids = {tt.id for tt in ob.trades} | {tt.id for tt in os.trades}
    assert t.id in combo_ids

    # Trade <-> Account (also eager-load)
    acc = (
        await session.execute(
            select(Account)
            .options(selectinload(Account.trades))
            .where(Account.id == acct.id)
        )
    ).scalar_one()
    assert any(tt.id == t.id for tt in acc.trades)


@pytest.mark.anyio
async def test_position_uniqueness_and_relationship(session: AsyncSession):
    acct = Account(username="dave", role=AccountRole.USER)
    session.add(acct)
    await session.flush()

    p1 = Position(account_id=acct.id, symbol="CQAF", quantity=5, average_price=10.0)
    session.add(p1)
    await session.commit()

    # Duplicate (account, symbol) should fail
    p_dup = Position(account_id=acct.id, symbol="CQAF", quantity=1, average_price=9.0)
    session.add(p_dup)

    acct_id = acct.id

    with pytest.raises(IntegrityError):
        await session.commit()

    # IMPORTANT: rollback after the raised commit before using the session again
    await session.rollback()

    got_acct = (
        await session.execute(
            select(Account)
            .options(selectinload(Account.positions))
            .where(Account.id == acct_id)
        )
    ).scalar_one()
    assert len(got_acct.positions) == 1
    assert got_acct.positions[0].symbol == "CQAF"


@pytest.mark.anyio
async def test_order_status_progression_and_timestamps(session: AsyncSession):
    acct = Account(username="erin", role=AccountRole.USER)
    session.add(acct)
    await session.flush()

    o = Order(
        account_id=acct.id, symbol="CQAF",
        side=OrderSide.BUY, type=OrderType.LIMIT,
        quantity=4, price=11.5
    )
    session.add(o)
    await session.commit()

    created = o.created_at
    assert isinstance(created, datetime)

    # Simulate a partial fill
    o.filled_quantity = 2
    o.status = OrderStatus.PARTIAL
    await session.commit()

    got = await session.get(Order, o.id)
    await session.refresh(got)

    def to_naive(dt):
        return dt if dt is None or dt.tzinfo is None else dt.replace(tzinfo=None)

    assert got.status == OrderStatus.PARTIAL
    if hasattr(got, "updated_at") and got.updated_at is not None:
        assert to_naive(got.updated_at) >= to_naive(created)