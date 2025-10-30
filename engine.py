import heapq
from asyncio import Lock
from datetime import datetime, timezone
from typing import Optional

from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncSession

from models import Order, OrderSide as Side, Trade, OrderType, OrderStatus

from engine_server.event_bus.event_bus import EventBus, EventType

"""
This will have a bit of a funky design to it for now,
this is so that if we need to switch over to C++ for the part
of the code that needs to be performant, we can do so quite easily
"""


class OrderBook:
    def __init__(self, instrument_symbol: str):
        self.instrument_symbol = instrument_symbol
        self.bids = []
        self.asks = []

    def push(self, o: Order):
        """
        Key outline

        (-Price, Time) - BID
        (Price, Time) - ASK
        """
        key = (-o.price, o.created_at,
               o) if o.side == Side.BUY else (o.price, o.created_at, o)
        heapq.heappush(self.bids if o.side == Side.BUY else self.asks, key)

    def pop_best(self, side: Side) -> Optional[Order]:
        heap = self.bids if side == Side.BUY else self.asks

        if not heap:
            return None

        _, _, o = heapq.heappop(heap)

        return o

    def best(self):
        bid = self.bids[0][2] if self.bids else None
        ask = self.asks[0][2] if self.asks else None

        return bid, ask


"""
3AM Soren having some neuron activation here--we'll probably want a quick way to peek the orderbook,
knowing this, I'm going to define some quick cache for it. Also probably should remove the order wrapping
"""


@dataclass
class MarkSnapshot:
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    last_trade: Optional[float] = None
    mark: Optional[float] = None


class MatchingEngine:

    """
    Semi large refactor here, we need to support multiple tickers,
    therefore, we should probably store them somewhere, and I've decided that that place is here
    """

    def __init__(self, bus: EventBus):
        self.books: dict[str, OrderBook] = {}
        self._marks: dict[str, MarkSnapshot] = {}
        self.bus = bus
        """
        Coarse async lock for cache updates, 
        could also be smart to do something for concurrent orders eventually
        """
        self._lock = Lock()

    def get_book(self, instrument_symbol: str) -> OrderBook:
        if instrument_symbol not in self.books:
            new_book = OrderBook(instrument_symbol)
            self._marks[instrument_symbol] = MarkSnapshot()
            self.books[instrument_symbol] = new_book

            return new_book

        return self.books[instrument_symbol]

    def _remaining(self, o: Order) -> int:
        """
        Robust remaining: prefer Order.remaining_quantity if present,
        otherwise compute quantity - filled_quantity.
        """
        if hasattr(o, "remaining_quantity"):
            try:
                return int(getattr(o, "remaining_quantity"))
            except Exception:
                pass
        q = getattr(o, "quantity", 0) or 0
        f = getattr(o, "filled_quantity", 0) or 0
        return int(q - f)

    async def _refresh_mark_from_book(self, instrument_symbol: str):
        bid, ask = self.books[instrument_symbol].best()
        bb = bid.price if bid else None
        ba = ask.price if ask else None

        snap = self._marks[instrument_symbol]
        snap.best_bid, snap.best_ask = bb, ba

        if bb is not None and ba is not None:
            snap.mark = (bb + ba) / 2.0
        else:
            snap.mark = snap.last_trade

    async def _on_trade(self, instrument_symbol: str, trade_price: float):
        snap = self._marks[instrument_symbol]
        snap.last_trade = trade_price

        if snap.best_bid is None or snap.best_ask is None:
            snap.mark = trade_price

    async def get_mark_snapshot(self, instrument_symbol: str) -> MarkSnapshot:
        return self._marks.get(instrument_symbol, MarkSnapshot())

    async def add_order(self, o: Order, db: AsyncSession):
        book = self.get_book(o.symbol)
        trades: list[Trade] = []

        # Market orders consume immediately
        if o.type == OrderType.MARKET:
            trades = await self._execute_market(o, db)

            async with self._lock:
                await self._refresh_mark_from_book(o.symbol)

            return trades

        book.push(o)
        trades = await self._cross(o.symbol, db)

        async with self._lock:
            await self._refresh_mark_from_book(o.symbol)

            if trades:
                await self._on_trade(o.symbol, trades[-1].price)

                # after trade, top-of-book may have shifted, so we'll recompute again
                await self._refresh_mark_from_book(o.symbol)

        return trades

    async def _execute_market(self, mkt: Order, db: AsyncSession):
        trades: list[Trade] = []
        book = self.get_book(mkt.symbol)

        remaining = self._remaining(mkt)

        while remaining > 0:
            bid, ask = book.best()
            contra = ask if mkt.side == Side.BUY else bid
            if not contra:
                break

            popped = book.pop_best(
                Side.SELL if mkt.side == Side.BUY else Side.BUY)
            if popped is None:
                break  # defensive

            maker_rem = self._remaining(popped)
            if maker_rem <= 0:
                continue  # skip empty (shouldn't happen, but safe)

            trade_qty = min(remaining, maker_rem)
            trade_px = popped.price

            t = Trade(
                symbol=mkt.symbol,
                buy_order_id=mkt.id if mkt.side == Side.BUY else popped.id,
                sell_order_id=popped.id if mkt.side == Side.BUY else mkt.id,
                price=trade_px, quantity=trade_qty, created_at=datetime.now(
                    timezone.utc),
            )
            db.add(t)
            trades.append(t)

            # advance fills (NEVER write remaining_quantity directly)
            mkt.filled_quantity = (mkt.filled_quantity or 0) + trade_qty
            popped.filled_quantity = (popped.filled_quantity or 0) + trade_qty

            # update statuses
            remaining = self._remaining(mkt)
            maker_rem_after = self._remaining(popped)
            popped.status = OrderStatus.FILLED if maker_rem_after == 0 else OrderStatus.PARTIAL

            # Figure out exact payload
            await self.bus.publish(EventType.TRADE, payload={
                "ticker": mkt.symbol,
                "price": trade_px,
                "quantity": trade_qty,
                "bid_price": bid.price if mkt.side == Side.SELL else None,
                "ask_price": ask.price if mkt.side == Side.BUY else None,
            })

            # requeue maker if it still has shares/contracts left
            if maker_rem_after > 0:
                book.push(popped)

        # taker status after loop
        mkt.status = OrderStatus.FILLED if self._remaining(
            mkt) == 0 else OrderStatus.PARTIAL

        await self._update_positions(trades, db)

        if trades:
            async with self._lock:
                await self._on_trade(mkt.symbol, trades[-1].price)
                await self._refresh_mark_from_book(mkt.symbol)

        return trades

    async def _cross(self, instrument_symbol: str, db: AsyncSession):
        trades: list[Trade] = []
        book = self.get_book(instrument_symbol)

        while True:
            bid, ask = book.best()
            if not bid or not ask or bid.price < ask.price:
                break

            # Pop both sides
            b = book.pop_best(Side.BUY)
            a = book.pop_best(Side.SELL)
            if b is None or a is None:
                break

            b_rem = self._remaining(b)
            a_rem = self._remaining(a)
            if b_rem <= 0 and a_rem <= 0:
                continue

            qty = min(b_rem, a_rem)
            # Trade at older order's price
            px = a.price if a.created_at <= b.created_at else b.price

            t = Trade(
                symbol=instrument_symbol,
                buy_order_id=b.id, sell_order_id=a.id,
                price=px, quantity=qty, created_at=datetime.now(timezone.utc),
            )
            db.add(t)
            trades.append(t)

            await self.bus.publish(EventType.TRADE, payload={
                "ticker": instrument_symbol,
                "price": px,
                "quantity": qty,
                "bid_price": b.price,
                "ask_price": a.price,
            })

            # advance fills
            b.filled_quantity = (b.filled_quantity or 0) + qty
            a.filled_quantity = (a.filled_quantity or 0) + qty

            # statuses
            b.status = OrderStatus.FILLED if self._remaining(
                b) == 0 else OrderStatus.PARTIAL
            a.status = OrderStatus.FILLED if self._remaining(
                a) == 0 else OrderStatus.PARTIAL

            # requeue any remainder
            if self._remaining(b) > 0:
                book.push(b)
            if self._remaining(a) > 0:
                book.push(a)

        if trades:
            await self._update_positions(trades, db)

        return trades

    async def _update_positions(self, trades: list[Trade], db: AsyncSession):
        """
        Yeah sorry guys I'm not writing this shit rn
        """
        pass
