import uuid
from datetime import datetime, timezone, timedelta

import pytest

from engine import OrderBook, MatchingEngine
from models import Trade, OrderSide as Side, OrderType, Order, OrderStatus

"""
A smoking engine is never good!
"""

SYMBOL = "CQAF"

def make_order(
    *,
    side: Side,
    type_: OrderType,
    qty: int,
    price: float | None,
    created_at: datetime,
    symbol: str = SYMBOL,
):
    """
    Create a *detached* Order instance with a UUID id and explicitly set timestamps.
    We do not add it to the DB in these tests.
    """
    kwargs = dict(
        id=str(uuid.uuid4()),
        symbol=symbol,
        side=side,
        type=type_,
        quantity=qty,
        price=price,
        filled_quantity=0,
        status=OrderStatus.PENDING,
        created_at=created_at,
    )
    # Some schemas also require account_id; set a dummy value if present.
    if hasattr(Order, "account_id"):
        kwargs["account_id"] = 1
    o = Order(**kwargs)  # type: ignore[arg-type]
    # If your model defines remaining_quantity as a computed @property, great.
    # If it's a mutable attribute, initialize it here to qty.
    if not hasattr(o, "remaining_quantity"):
        # Provide a compatibility shim: assume remaining = quantity - filled_quantity via property.
        pass
    else:
        try:
            # If it's mutable, set it explicitly.
            setattr(o, "remaining_quantity", qty)  # type: ignore[attr-defined]
        except Exception:
            # If it's a read-only property, ignore.
            pass
    return o

@pytest.mark.anyio
async def test_orderbook_price_time_priority():
    book = OrderBook(SYMBOL)
    now = datetime.now(timezone.utc)

    # Bids: higher price first; for same price, earlier time first
    b1 = make_order(side=Side.BUY,  type_=OrderType.LIMIT, qty=5, price=100.0, created_at=now - timedelta(seconds=5))
    b2 = make_order(side=Side.BUY,  type_=OrderType.LIMIT, qty=5, price=101.0, created_at=now - timedelta(seconds=4))
    b3 = make_order(side=Side.BUY,  type_=OrderType.LIMIT, qty=5, price=101.0, created_at=now - timedelta(seconds=3))

    # Asks: lower price first; for same price, earlier time first
    a1 = make_order(side=Side.SELL, type_=OrderType.LIMIT, qty=5, price=103.0, created_at=now - timedelta(seconds=5))
    a2 = make_order(side=Side.SELL, type_=OrderType.LIMIT, qty=5, price=102.0, created_at=now - timedelta(seconds=4))
    a3 = make_order(side=Side.SELL, type_=OrderType.LIMIT, qty=5, price=102.0, created_at=now - timedelta(seconds=3))

    for o in (b1, b2, b3, a1, a2, a3):
        book.push(o)

    best_bid, best_ask = book.best()
    assert best_bid.id == b2.id  # highest price (101), and older than b3
    assert best_ask.id == a2.id  # lowest price (102), and older than a3

    # pop bests
    popped_bid = book.pop_best(Side.BUY)
    popped_ask = book.pop_best(Side.SELL)
    assert popped_bid.id == b2.id
    assert popped_ask.id == a2.id

    # next level
    best_bid2, best_ask2 = book.best()
    assert best_bid2.id == b3.id
    assert best_ask2.id == a3.id

@pytest.mark.anyio
async def test_limit_cross_generates_trade(session):
    eng = MatchingEngine()
    now = datetime.now(timezone.utc)

    # Older SELL at 100, newer BUY at 101 -> trade at older order's price (100)
    ask = make_order(side=Side.SELL, type_=OrderType.LIMIT, qty=3, price=100.0, created_at=now - timedelta(seconds=5))
    bid = make_order(side=Side.BUY,  type_=OrderType.LIMIT, qty=3, price=101.0, created_at=now - timedelta(seconds=4))

    # Add older one first (sell), then buy to trigger cross
    trades1 = await eng.add_order(ask, session)
    assert trades1 == []  # no contra yet

    trades2 = await eng.add_order(bid, session)
    assert len(trades2) == 1
    t = trades2[0]
    assert isinstance(t, Trade)
    assert t.symbol == SYMBOL
    assert t.buy_order_id == bid.id and t.sell_order_id == ask.id
    assert t.quantity == 3
    assert t.price == 100.0  # earlier side's price

    # Book should be empty at top after full fill
    book = eng.get_book(SYMBOL)
    btop, atop = book.best()
    assert btop is None and atop is None


@pytest.mark.anyio
async def test_partial_fill_and_requeue(session):
    eng = MatchingEngine()
    now = datetime.now(timezone.utc)

    # SELL 10 @ 100 (older), BUY 6 @ 101 (newer) => one trade 6 @ 100
    ask = make_order(side=Side.SELL, type_=OrderType.LIMIT, qty=10, price=100.0, created_at=now - timedelta(seconds=5))
    bid = make_order(side=Side.BUY,  type_=OrderType.LIMIT, qty=6,  price=101.0, created_at=now - timedelta(seconds=4))

    await eng.add_order(ask, session)
    trades = await eng.add_order(bid, session)

    assert len(trades) == 1
    t = trades[0]
    assert t.quantity == 6
    assert t.price == 100.0

    # Remaining on the ask should requeue at the same price
    book = eng.get_book(SYMBOL)
    best_bid, best_ask = book.best()
    assert best_bid is None
    assert best_ask is not None
    assert best_ask.price == 100.0

    # If remaining_quantity is a true mutable attr, it should be 4; if it's a property,
    # then quantity/filled_quantity may reflect the change in your model implementation.
    if hasattr(ask, "remaining_quantity"):
        assert best_ask.remaining_quantity == 4  # type: ignore[attr-defined]


@pytest.mark.anyio
async def test_market_buy_sweeps_multiple_asks(session):
    eng = MatchingEngine()
    now = datetime.now(timezone.utc)

    # Liquidity: 5 @ 100 (older), 5 @ 101 (newer)
    a1 = make_order(side=Side.SELL, type_=OrderType.LIMIT, qty=5, price=100.0, created_at=now - timedelta(seconds=6))
    a2 = make_order(side=Side.SELL, type_=OrderType.LIMIT, qty=5, price=101.0, created_at=now - timedelta(seconds=5))
    await eng.add_order(a1, session)
    await eng.add_order(a2, session)

    # Market BUY for 8 should consume 5 @ 100 and 3 @ 101
    mkt = make_order(side=Side.BUY, type_=OrderType.MARKET, qty=8, price=None, created_at=now - timedelta(seconds=4))
    trades = await eng.add_order(mkt, session)

    # Two trades expected: (5 @ 100), (3 @ 101)
    assert len(trades) == 2
    qties = [tr.quantity for tr in trades]
    prices = [tr.price for tr in trades]
    assert qties == [5, 3]
    assert prices == [100.0, 101.0]
    assert mkt.status in (OrderStatus.FILLED, OrderStatus.PARTIAL)

    # Remaining top of book should be 2 @ 101
    book = eng.get_book(SYMBOL)
    best_bid, best_ask = book.best()
    assert best_bid is None
    assert best_ask is not None
    assert best_ask.price == 101.0
    if hasattr(best_ask, "remaining_quantity"):
        assert getattr(best_ask, "remaining_quantity") == 2  # type: ignore[attr-defined]


@pytest.mark.anyio
async def test_mark_snapshot_updates_after_trade(session):
    eng = MatchingEngine()
    now = datetime.now(timezone.utc)

    # Add book and cross once to set last_trade and mark
    a = make_order(side=Side.SELL, type_=OrderType.LIMIT, qty=1, price=100.0, created_at=now - timedelta(seconds=2))
    b = make_order(side=Side.BUY,  type_=OrderType.LIMIT, qty=1, price=101.0, created_at=now - timedelta(seconds=1))
    await eng.add_order(a, session)
    await eng.add_order(b, session)

    snap = await eng.get_mark_snapshot(SYMBOL)
    # Expect last_trade set and mark populated; best bid/ask none after full cross
    assert snap.last_trade == 100.0
    assert snap.mark == 100.0
    assert snap.best_bid is None and snap.best_ask is None