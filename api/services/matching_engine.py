"""
Matching Engine service for the QuantX trading system.

Responsibilities:
    - Maintains all order books in memory
    - Executes matching algorithm for limit and market orders
    - Owns and updates MarketDataSnapshot for each ticker
    - Provides get_best_price() for Risk Engine queries
    - Publishes TRADE_EXECUTED and MARKET_DATA_UPDATE events
    - Hydrates order books from database on startup or when empty

Does NOT:
    - Write to database (PersistenceService handles this)
    - Validate orders (RiskEngine handles this)
    - Handle WebSocket connections
"""

import asyncio
import heapq
import uuid
import logging
from asyncio import Lock
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Callable
from sortedcontainers import SortedDict

from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from models import Order, OrderSide, OrderType, OrderStatus

from .base_service import BaseService
from .event_bus import EventBus, EventHandler
from .events import (
    Event,
    EventType,
    ValidatedOrderPayload,
    TradeExecutedPayload,
    MarketDataUpdatePayload,
    RefreshBookPayload,
    OrderCancelledPayload,
)

logger = logging.getLogger(__name__)

# Constants
SNAPSHOT_DEPTH = 10  # Number of price levels to include in market data snapshots


@dataclass
class EngineOrder:
    """
    In-memory representation of an order for the matching engine.

    This is a lightweight copy of order data needed for matching,
    without SQLAlchemy model dependencies.
    """
    order_id: str
    account_id: int
    ticker: str
    side: OrderSide
    order_type: OrderType
    price: float
    quantity: int
    filled_quantity: int = 0
    created_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc))
    websocket_id: str = ""

    @property
    def remaining_quantity(self) -> int:
        return self.quantity - self.filled_quantity

    def is_filled(self) -> bool:
        return self.remaining_quantity <= 0


class MarketDataSnapshot:
    """
    Maintains aggregated order book data for efficient market data broadcasting.

    Uses SortedDict for O(log n) operations on price levels.
    Bids are sorted descending (best bid first).
    Asks are sorted ascending (best ask first).
    """

    def __init__(self, ticker: str, snapshot_depth: int = SNAPSHOT_DEPTH):
        self.ticker = ticker
        self.snapshot_depth = snapshot_depth

        # Bids sorted descending by price (negate key for descending)
        self.bids: SortedDict = SortedDict(lambda x: -x)
        # Asks sorted ascending by price
        self.asks: SortedDict = SortedDict()

        self.total_bid_quantity = 0
        self.total_ask_quantity = 0
        self.last_trade_price: Optional[float] = None
        self.last_trade_quantity: Optional[int] = None

    def add_order(self, price: float, quantity: int, side: OrderSide) -> None:
        """Add order quantity to a price level."""
        book = self.bids if side == OrderSide.BUY else self.asks

        if price not in book:
            book[price] = 0
        book[price] += quantity

        if side == OrderSide.BUY:
            self.total_bid_quantity += quantity
        else:
            self.total_ask_quantity += quantity

    def remove_order(self, price: float, quantity: int, side: OrderSide) -> None:
        """Remove order quantity from a price level."""
        book = self.bids if side == OrderSide.BUY else self.asks

        if price not in book:
            logger.warning(
                f"Price {price} not found in {side.value} book for {self.ticker}")
            return

        book[price] -= quantity

        if side == OrderSide.BUY:
            self.total_bid_quantity = max(
                0, self.total_bid_quantity - quantity)
        else:
            self.total_ask_quantity = max(
                0, self.total_ask_quantity - quantity)

        # Remove empty price levels
        if book[price] <= 0:
            del book[price]

    def update_last_trade(self, price: float, quantity: int) -> None:
        """Update last trade information."""
        self.last_trade_price = price
        self.last_trade_quantity = quantity

    def get_best_bid(self) -> Optional[float]:
        """Get the best (highest) bid price."""
        if not self.bids:
            return None
        return self.bids.keys()[0]

    def get_best_ask(self) -> Optional[float]:
        """Get the best (lowest) ask price."""
        if not self.asks:
            return None
        return self.asks.keys()[0]

    def get_mid_price(self) -> Optional[float]:
        """Calculate mid price from best bid and ask."""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()

        if best_bid is not None and best_ask is not None:
            return round((best_bid + best_ask) / 2, 2)
        elif best_bid is not None:
            return best_bid
        elif best_ask is not None:
            return best_ask
        return None

    def get_top_levels(self, side: OrderSide) -> List[Tuple[float, int]]:
        """Get top N price levels for a side."""
        book = self.bids if side == OrderSide.BUY else self.asks
        return list(book.items())[:self.snapshot_depth]

    def get_snapshot(self) -> MarketDataUpdatePayload:
        """Generate a complete market data snapshot."""
        return MarketDataUpdatePayload(
            ticker=self.ticker,
            bids=self.get_top_levels(OrderSide.BUY),
            asks=self.get_top_levels(OrderSide.SELL),
            total_bid_quantity=self.total_bid_quantity,
            total_ask_quantity=self.total_ask_quantity,
            best_bid=self.get_best_bid(),
            best_ask=self.get_best_ask(),
            mid_price=self.get_mid_price(),
            last_trade_price=self.last_trade_price,
            last_trade_quantity=self.last_trade_quantity,
            timestamp=datetime.now(timezone.utc),
        )

    def is_empty(self) -> bool:
        """Check if the order book is empty."""
        return self.total_bid_quantity == 0 and self.total_ask_quantity == 0


class OrderBook:
    """
    Price-time priority order book for a single instrument.

    Uses heaps for efficient best price retrieval:
        - Bids: max-heap (negated prices)
        - Asks: min-heap

    Orders are stored by (price, timestamp, order_id) for deterministic ordering.
    """

    def __init__(self, ticker: str):
        self.ticker = ticker
        # Heap entries: (priority_key, timestamp, order_id)
        # For bids: priority_key = -price (max heap via negation)
        # For asks: priority_key = price (min heap)
        self.bid_heap: List[Tuple[float, datetime, str]] = []
        self.ask_heap: List[Tuple[float, datetime, str]] = []

        # Order storage by ID for O(1) lookup
        self.orders: Dict[str, EngineOrder] = {}

    def add_order(self, order: EngineOrder) -> None:
        """Add an order to the book."""
        if order.side == OrderSide.BUY:
            # Negate price for max-heap behavior
            heapq.heappush(
                self.bid_heap,
                (-order.price, order.created_at, order.order_id)
            )
        else:
            heapq.heappush(
                self.ask_heap,
                (order.price, order.created_at, order.order_id)
            )

        self.orders[order.order_id] = order

    def get_order(self, order_id: str) -> Optional[EngineOrder]:
        """Get an order by ID."""
        return self.orders.get(order_id)

    def remove_order(self, order_id: str) -> Optional[EngineOrder]:
        """Remove an order from storage (lazy removal from heap)."""
        return self.orders.pop(order_id, None)

    def get_best_bid(self) -> Optional[Tuple[float, str]]:
        """
        Get the best bid price and order ID.

        Lazily removes stale entries (orders that have been filled/cancelled).

        Returns:
            Tuple of (price, order_id) or None if no bids
        """
        while self.bid_heap:
            neg_price, _, order_id = self.bid_heap[0]
            order = self.orders.get(order_id)

            if order and order.remaining_quantity > 0:
                return (-neg_price, order_id)

            # Stale entry, remove it
            heapq.heappop(self.bid_heap)

        return None

    def get_best_ask(self) -> Optional[Tuple[float, str]]:
        """
        Get the best ask price and order ID.

        Lazily removes stale entries.

        Returns:
            Tuple of (price, order_id) or None if no asks
        """
        while self.ask_heap:
            price, _, order_id = self.ask_heap[0]
            order = self.orders.get(order_id)

            if order and order.remaining_quantity > 0:
                return (price, order_id)

            # Stale entry, remove it
            heapq.heappop(self.ask_heap)

        return None

    def pop_best_bid(self) -> Optional[EngineOrder]:
        """Pop the best bid order."""
        result = self.get_best_bid()
        if result:
            _, order_id = result
            heapq.heappop(self.bid_heap)
            return self.orders.get(order_id)
        return None

    def pop_best_ask(self) -> Optional[EngineOrder]:
        """Pop the best ask order."""
        result = self.get_best_ask()
        if result:
            _, order_id = result
            heapq.heappop(self.ask_heap)
            return self.orders.get(order_id)
        return None

    def get_spread(self) -> Optional[Tuple[float, float]]:
        """Get current bid-ask spread."""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()

        if best_bid and best_ask:
            return (best_bid[0], best_ask[0])
        return None


class MatchingEngine(BaseService):
    """
    Core matching engine service.

    Subscribes to VALIDATED_ORDER events and executes the matching algorithm.
    Publishes TRADE_EXECUTED and MARKET_DATA_UPDATE events.
    """

    def __init__(
        self,
        event_bus: EventBus,
        tickers: List[str],
        session_factory: Optional[Callable] = None,
    ):
        """
        Initialize the matching engine.

        Args:
            event_bus: Shared event bus
            tickers: List of supported ticker symbols
            session_factory: Optional async session factory for database hydration
        """
        super().__init__(event_bus)

        self.tickers = [t.upper() for t in tickers]
        self._session_factory = session_factory

        # Order books per ticker
        self._books: Dict[str, OrderBook] = {
            ticker: OrderBook(ticker) for ticker in self.tickers
        }

        # Market data snapshots per ticker
        self._market_data: Dict[str, MarketDataSnapshot] = {
            ticker: MarketDataSnapshot(ticker) for ticker in self.tickers
        }

        # Track which tickers have been hydrated
        self._hydrated: Dict[str, bool] = {
            ticker: False for ticker in self.tickers
        }

        # Lock for thread-safe operations
        self._lock = Lock()

    @property
    def service_name(self) -> str:
        return "MatchingEngine"

    def _get_subscriptions(self) -> List[Tuple[EventType, EventHandler]]:
        return [
            (EventType.VALIDATED_ORDER, self._handle_validated_order),
            (EventType.REFRESH_MARKET_DATA, self._handle_book_refresh),
            (EventType.ORDER_CANCELLED, self._handle_order_cancelled),
        ]

    # =========================================================================
    # Public API - Can be called by other services (e.g., RiskEngine)
    # =========================================================================

    def get_best_price(self, ticker: str, side: OrderSide) -> Optional[float]:
        """
        Get the best available price for a given side.

        For BUY orders: returns best ask (price to buy at)
        For SELL orders: returns best bid (price to sell at)

        This method is called by RiskEngine for market order validation.

        Args:
            ticker: Ticker symbol
            side: Order side (BUY or SELL)

        Returns:
            Best price or None if no liquidity
        """
        ticker = ticker.upper()
        if ticker not in self._books:
            return None

        book = self._books[ticker]

        if side == OrderSide.BUY:
            # Buyer wants to know best ask
            result = book.get_best_ask()
            return result[0] if result else None
        else:
            # Seller wants to know best bid
            result = book.get_best_bid()
            return result[0] if result else None

    async def _handle_order_cancelled(self, event: Event) -> None:
        """
        Handle ORDER_CANCELLED event from PersistenceService.

        Removes the order from the in-memory order book and updates
        the market data snapshot, then broadcasts the update.
        """
        payload = OrderCancelledPayload.from_dict(event.payload)

        ticker = payload.ticker.upper()

        if ticker not in self._books:
            self._logger.warning(
                f"Cannot cancel order for unknown ticker: {ticker}"
            )
            return

        async with self._lock:
            book = self._books[ticker]
            market_data = self._market_data[ticker]

            # Remove order from the orders dict
            # This makes lazy removal work - when get_best_bid/ask encounters
            # this order_id, it won't find it in the dict and will skip it
            removed_order = book.remove_order(payload.order_id)

            if removed_order:
                self._logger.info(
                    f"Removed order {payload.order_id} from {ticker} book"
                )
            else:
                self._logger.debug(
                    f"Order {payload.order_id} not found in {ticker} book "
                    "(may have already been filled or removed)"
                )

            # Update market data snapshot - remove the remaining quantity
            # from the appropriate price level
            if payload.remaining_quantity > 0:
                market_data.remove_order(
                    payload.price,
                    payload.remaining_quantity,
                    payload.side,
                )
                self._logger.info(
                    f"Updated market data: removed {payload.remaining_quantity} "
                    f"from {payload.side.value} @ {payload.price}"
                )

        # Publish market data update to broadcast to clients
        await self._publish_market_data_update(ticker, event.correlation_id)

    async def _handle_book_refresh(self, event: Event):
        payload = RefreshBookPayload.from_dict(event.payload)
        ticker = payload.ticker

        hydrated = await self.hydrate_book(ticker)

        if hydrated:
            await self._publish_market_data_update(ticker, correlation_id="")

    def get_market_data_snapshot(self, ticker: str) -> Optional[MarketDataUpdatePayload]:
        """
        Get current market data snapshot for a ticker.

        Args:
            ticker: Ticker symbol

        Returns:
            MarketDataUpdatePayload or None if ticker not found
        """
        ticker = ticker.upper()
        if ticker not in self._market_data:
            return None
        return self._market_data[ticker].get_snapshot()

    def is_book_empty(self, ticker: str) -> bool:
        """Check if an order book is empty."""
        ticker = ticker.upper()
        if ticker not in self._market_data:
            return True
        return self._market_data[ticker].is_empty()

    # =========================================================================
    # Database Hydration
    # =========================================================================

    async def hydrate_all_books(self) -> None:
        """
        Hydrate all order books from the database.

        This should be called on server startup to rebuild in-memory
        order books from persisted PENDING/PARTIAL orders.
        """
        if not self._session_factory:
            self._logger.warning(
                "Cannot hydrate order books: no session_factory provided"
            )
            return

        self._logger.info("Hydrating all order books from database...")

        for ticker in self.tickers:
            await self.hydrate_book(ticker)

        self._logger.info("Order book hydration complete")

    async def hydrate_book(self, ticker: str) -> bool:
        """
        Hydrate a single order book from the database.

        Queries for all PENDING and PARTIAL orders for the ticker
        and populates both the OrderBook and MarketDataSnapshot.

        Args:
            ticker: Ticker symbol to hydrate

        Returns:
            True if hydration succeeded, False otherwise
        """
        ticker = ticker.upper()

        if ticker not in self._books:
            self._logger.warning(f"Cannot hydrate unknown ticker: {ticker}")
            return False

        if not self._session_factory:
            self._logger.warning(
                f"Cannot hydrate {ticker}: no session_factory provided"
            )
            return False

        # Skip if already hydrated
        if self._hydrated.get(ticker, False):
            self._logger.debug(f"Ticker {ticker} already hydrated, skipping")
            return True

        async with self._lock:
            # Double-check after acquiring lock
            if self._hydrated.get(ticker, False):
                return True

            try:
                async with self._session_factory() as session:
                    orders = await self._fetch_active_orders(session, ticker)

                    if not orders:
                        self._logger.info(
                            f"No active orders found for {ticker}, book is empty"
                        )
                        self._hydrated[ticker] = True
                        return True

                    self._logger.info(
                        f"Hydrating {ticker} with {len(orders)} active orders"
                    )

                    # Rebuild the order book and market data snapshot
                    for db_order in orders:
                        self._add_order_to_book(db_order)

                    self._hydrated[ticker] = True
                    self._logger.info(
                        f"Hydration complete for {ticker}: "
                        f"bids={self._market_data[ticker].total_bid_quantity}, "
                        f"asks={self._market_data[ticker].total_ask_quantity}"
                    )
                    return True

            except Exception as e:
                self._logger.error(
                    f"Failed to hydrate order book for {ticker}: {e}",
                    exc_info=True
                )
                return False

    async def _fetch_active_orders(
        self,
        session: AsyncSession,
        ticker: str,
    ) -> List[Order]:
        """
        Fetch all active (PENDING/PARTIAL) orders for a ticker from the database.

        Args:
            session: Database session
            ticker: Ticker symbol

        Returns:
            List of Order models
        """
        query = (
            select(Order)
            .where(Order.symbol == ticker.upper())
            .where(
                or_(
                    Order.status == OrderStatus.PENDING,
                    Order.status == OrderStatus.PARTIAL,
                )
            )
            .order_by(Order.created_at)  # Maintain time priority
        )

        result = await session.execute(query)
        return list(result.scalars().all())

    def _add_order_to_book(self, db_order: Order) -> None:
        """
        Add a database Order to the in-memory order book and market data.

        This converts the SQLAlchemy Order model to an EngineOrder
        and adds it to both the OrderBook and MarketDataSnapshot.

        Args:
            db_order: SQLAlchemy Order model
        """
        ticker = db_order.symbol.upper()

        if ticker not in self._books:
            return

        # Calculate remaining quantity
        remaining = db_order.quantity - (db_order.filled_quantity or 0)

        if remaining <= 0:
            return

        # Create engine order from DB order
        engine_order = EngineOrder(
            order_id=str(db_order.id),
            account_id=db_order.account_id,
            ticker=ticker,
            side=db_order.side,
            order_type=db_order.type,
            price=db_order.price or 0.0,
            quantity=db_order.quantity,
            filled_quantity=db_order.filled_quantity or 0,
            created_at=db_order.created_at.replace(tzinfo=timezone.utc),
            websocket_id="",  # Not needed for hydrated orders
        )

        # Add to order book
        self._books[ticker].add_order(engine_order)

        # Add remaining quantity to market data snapshot
        self._market_data[ticker].add_order(
            engine_order.price,
            remaining,  # Only add remaining quantity, not total
            engine_order.side,
        )

    async def ensure_hydrated(self, ticker: str) -> bool:
        """
        Ensure a ticker's order book is hydrated before use.

        This can be called lazily when a client first connects to a ticker.

        Args:
            ticker: Ticker symbol

        Returns:
            True if hydrated (or already was), False on error
        """
        ticker = ticker.upper()

        if self._hydrated.get(ticker, False):
            return True

        return await self.hydrate_book(ticker)

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _handle_validated_order(self, event: Event) -> None:
        """
        Handle a validated order event.

        Creates an EngineOrder and processes it through the matching algorithm.
        """
        payload = ValidatedOrderPayload.from_dict(event.payload)

        self._logger.info(
            f"Processing order: {payload.side.value} {payload.quantity} "
            f"{payload.ticker} @ {payload.price}"
        )

        # Create engine order using the same order_id from the payload
        order = EngineOrder(
            order_id=payload.order_id,
            account_id=payload.account_id,
            ticker=payload.ticker.upper(),
            side=payload.side,
            order_type=payload.order_type,
            price=payload.price,
            quantity=payload.quantity,
            filled_quantity=0,
            created_at=datetime.now(timezone.utc),
            websocket_id=payload.websocket_id,
        )

        async with self._lock:
            if order.order_type == OrderType.MARKET:
                await self._execute_market_order(order, event.correlation_id)
            else:
                await self._execute_limit_order(order, event.correlation_id)

    async def _execute_limit_order(self, order: EngineOrder, correlation_id: str) -> None:
        """
        Execute a limit order.

        1. Add to order book
        2. Update market data
        3. Attempt to cross (match)
        4. Publish market data update
        """
        ticker = order.ticker
        book = self._books[ticker]
        market_data = self._market_data[ticker]

        # Add order to book
        book.add_order(order)
        market_data.add_order(order.price, order.quantity, order.side)

        # Attempt to cross
        await self._cross(ticker, correlation_id)

        # Publish market data update
        await self._publish_market_data_update(ticker, correlation_id)

    async def _execute_market_order(self, order: EngineOrder, correlation_id: str) -> None:
        """
        Execute a market order.

        Market orders are immediately matched against resting orders
        without being added to the book.
        """
        ticker = order.ticker
        book = self._books[ticker]
        market_data = self._market_data[ticker]

        remaining = order.remaining_quantity

        while remaining > 0:
            # Get contra side order
            if order.side == OrderSide.BUY:
                contra_order = book.pop_best_ask()
            else:
                contra_order = book.pop_best_bid()

            if not contra_order:
                self._logger.warning(
                    f"Market order {order.order_id} partially filled, "
                    f"{remaining} remaining with no liquidity"
                )
                break

            # Calculate trade
            trade_qty = min(remaining, contra_order.remaining_quantity)
            trade_price = contra_order.price

            # Update quantities
            order.filled_quantity += trade_qty
            contra_order.filled_quantity += trade_qty
            remaining = order.remaining_quantity

            # Update market data
            market_data.remove_order(trade_price, trade_qty, contra_order.side)
            market_data.update_last_trade(trade_price, trade_qty)

            # Publish trade event
            await self._publish_trade(
                ticker=ticker,
                price=trade_price,
                quantity=trade_qty,
                buyer_order=order if order.side == OrderSide.BUY else contra_order,
                seller_order=contra_order if order.side == OrderSide.BUY else order,
                correlation_id=correlation_id,
            )

            # Re-add contra order if not fully filled
            if contra_order.remaining_quantity > 0:
                book.add_order(contra_order)

        # Publish market data update
        await self._publish_market_data_update(ticker, correlation_id)

    async def _cross(self, ticker: str, correlation_id: str) -> None:
        """
        Attempt to match orders in the book.

        Matches occur when best bid >= best ask.
        Trade price is the older order's price (price-time priority).
        """
        book = self._books[ticker]
        market_data = self._market_data[ticker]

        while True:
            best_bid_result = book.get_best_bid()
            best_ask_result = book.get_best_ask()

            if not best_bid_result or not best_ask_result:
                break

            bid_price, bid_order_id = best_bid_result
            ask_price, ask_order_id = best_ask_result

            # No cross possible
            if bid_price < ask_price:
                break

            # Pop both orders
            bid_order = book.pop_best_bid()
            ask_order = book.pop_best_ask()

            if not bid_order or not ask_order:
                break

            # Calculate trade
            trade_qty = min(bid_order.remaining_quantity,
                            ask_order.remaining_quantity)

            # Price is from the older (resting) order
            if bid_order.created_at <= ask_order.created_at:
                trade_price = bid_order.price
            else:
                trade_price = ask_order.price

            # Update filled quantities
            bid_order.filled_quantity += trade_qty
            ask_order.filled_quantity += trade_qty

            # Update market data
            market_data.remove_order(bid_order.price, trade_qty, OrderSide.BUY)
            market_data.remove_order(
                ask_order.price, trade_qty, OrderSide.SELL)
            market_data.update_last_trade(trade_price, trade_qty)

            # Publish trade event
            await self._publish_trade(
                ticker=ticker,
                price=trade_price,
                quantity=trade_qty,
                buyer_order=bid_order,
                seller_order=ask_order,
                correlation_id=correlation_id,
            )

            # Re-add orders with remaining quantity
            if bid_order.remaining_quantity > 0:
                book.add_order(bid_order)

            if ask_order.remaining_quantity > 0:
                book.add_order(ask_order)

    # =========================================================================
    # Event Publishing
    # =========================================================================

    async def _publish_trade(
        self,
        ticker: str,
        price: float,
        quantity: int,
        buyer_order: EngineOrder,
        seller_order: EngineOrder,
        correlation_id: str,
    ) -> None:
        """Publish a TRADE_EXECUTED event."""
        payload = TradeExecutedPayload(
            trade_id=str(uuid.uuid4()),
            ticker=ticker,
            price=price,
            quantity=quantity,
            buyer_order_id=buyer_order.order_id,
            seller_order_id=seller_order.order_id,
            buyer_account_id=buyer_order.account_id,
            seller_account_id=seller_order.account_id,
            timestamp=datetime.now(timezone.utc),
        )

        self._logger.info(
            f"Trade executed: {quantity} {ticker} @ {price} "
            f"(buyer={buyer_order.account_id}, seller={seller_order.account_id})"
        )

        await self.publish(
            EventType.TRADE_EXECUTED,
            payload.to_dict(),
            correlation_id,
        )

    async def _publish_market_data_update(self, ticker: str, correlation_id: str) -> None:
        """Publish a MARKET_DATA_UPDATE event."""
        snapshot = self._market_data[ticker].get_snapshot()

        await self.publish(
            EventType.MARKET_DATA_UPDATE,
            snapshot.to_dict(),
            correlation_id,
        )

    # =========================================================================
    # Health Check
    # =========================================================================

    async def health_check(self) -> dict:
        """Return health metrics for the matching engine."""
        base = await super().health_check()

        book_stats = {}
        for ticker in self.tickers:
            md = self._market_data[ticker]
            book_stats[ticker] = {
                "total_bids": md.total_bid_quantity,
                "total_asks": md.total_ask_quantity,
                "best_bid": md.get_best_bid(),
                "best_ask": md.get_best_ask(),
                "spread": None,
            }
            if book_stats[ticker]["best_bid"] and book_stats[ticker]["best_ask"]:
                book_stats[ticker]["spread"] = (
                    book_stats[ticker]["best_ask"] -
                    book_stats[ticker]["best_bid"]
                )

        return {
            **base,
            "tickers": self.tickers,
            "books": book_stats,
        }
