"""
Persistence Service for the QuantX trading system.

Responsibilities:
    - Subscribes to VALIDATED_ORDER events: Insert orders with PENDING status
    - Subscribes to TRADE_EXECUTED events: Update orders, create trades, update positions
    - All database operations in transactions
    - Publishes ORDER_PERSISTED events on success

Does NOT:
    - Validate orders (RiskEngine handles this)
    - Execute matching logic (MatchingEngine handles this)
    - Communicate with clients directly
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import (
    Account,
    Order,
    Trade,
    Position,
    OrderSide,
    OrderType,
    OrderStatus,
)

from .base_service import BaseService
from .event_bus import EventBus, EventHandler
from .events import (
    Event,
    EventType,
    ValidatedOrderPayload,
    TradeExecutedPayload,
    OrderPersistedPayload,
)

logger = logging.getLogger(__name__)


class PersistenceService(BaseService):
    """
    Database persistence service.

    Handles all write operations to the database including:
        - Order creation
        - Trade recording
        - Position updates
        - Balance updates
    """

    def __init__(self, event_bus: EventBus, session_factory):
        """
        Initialize the persistence service.

        Args:
            event_bus: Shared event bus
            session_factory: Async session factory for DB operations
        """
        super().__init__(event_bus)
        self._session_factory = session_factory

    @property
    def service_name(self) -> str:
        return "PersistenceService"

    def _get_subscriptions(self) -> List[Tuple[EventType, EventHandler]]:
        return [
            (EventType.VALIDATED_ORDER, self._handle_validated_order),
            (EventType.TRADE_EXECUTED, self._handle_trade_executed),
        ]

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _handle_validated_order(self, event: Event) -> None:
        """
        Handle validated order - insert into database with PENDING status.

        Also reserves cash for buy orders (deduct from available_cash).
        """
        payload = ValidatedOrderPayload.from_dict(event.payload)

        self._logger.info(
            f"Persisting order: {payload.side.value} {payload.quantity} "
            f"{payload.ticker} @ {payload.price} for account {payload.account_id}"
        )

        try:
            async with self._session_factory() as session:
                async with session.begin():
                    order = await self._create_order(session, payload)

                    # Reserve cash for buy orders
                    if payload.side == OrderSide.BUY:
                        await self._reserve_cash(
                            session,
                            payload.account_id,
                            payload.estimated_value
                        )

                    await session.flush()
                    await session.refresh(order)

                    order_id = order.id

            self._logger.info(f"Order persisted: {order_id}")

            # Publish ORDER_PERSISTED event
            persisted_payload = OrderPersistedPayload(
                order_id=order_id,
                ticker=payload.ticker,
                account_id=payload.account_id,
                websocket_id=payload.websocket_id,
                status=OrderStatus.PENDING,
            )

            await self.publish(
                EventType.ORDER_PERSISTED,
                persisted_payload.to_dict(),
                event.correlation_id,
            )

        except Exception as e:
            self._logger.error(f"Failed to persist order: {e}", exc_info=True)
            # Could publish an error event here

    async def _handle_trade_executed(self, event: Event) -> None:
        """
        Handle trade executed - update orders, create trade, update positions/balances.

        All operations are wrapped in a single transaction.
        """
        payload = TradeExecutedPayload.from_dict(event.payload)

        self._logger.info(
            f"Persisting trade: {payload.quantity} {payload.ticker} @ {payload.price} "
            f"(buyer={payload.buyer_account_id}, seller={payload.seller_account_id})"
        )

        try:
            async with self._session_factory() as session:
                async with session.begin():

                    # Create trade record
                    trade = await self._create_trade(session, payload)

                    # Update order statuses (filled_quantity and status)
                    await self._update_order_fill(
                        session, payload.buyer_order_id, payload.quantity
                    )
                    await self._update_order_fill(
                        session, payload.seller_order_id, payload.quantity
                    )

                    # Update buyer position and balance
                    await self._update_buyer(session, payload)

                    # Update seller position and balance
                    await self._update_seller(session, payload)

                    await session.flush()

            self._logger.info(f"Trade persisted: {payload.trade_id}")

        except Exception as e:
            self._logger.error(f"Failed to persist trade: {e}", exc_info=True)
            # Transaction is automatically rolled back

    # =========================================================================
    # Order Operations
    # =========================================================================

    async def _create_order(
        self,
        session: AsyncSession,
        payload: ValidatedOrderPayload,
    ) -> Order:
        """Create a new order in the database using the order_id from payload."""
        order = Order(
            id=payload.order_id,
            account_id=payload.account_id,
            symbol=payload.ticker.upper(),
            side=payload.side,
            type=payload.order_type,
            quantity=payload.quantity,
            price=payload.price,
            filled_quantity=0,
            status=OrderStatus.PENDING,
            created_at=datetime.now(timezone.utc),
        )
        session.add(order)
        return order

    async def _reserve_cash(
        self,
        session: AsyncSession,
        account_id: int,
        amount: float,
    ) -> None:
        """Reserve cash for a buy order (deduct from available_cash)."""
        account = await session.get(Account, account_id)
        if account:
            account.available_cash -= amount

    # =========================================================================
    # Trade Operations
    # =========================================================================

    async def _create_trade(
        self,
        session: AsyncSession,
        payload: TradeExecutedPayload,
    ) -> Trade:
        """Create a trade record in the database."""
        trade = Trade(
            id=payload.trade_id,
            buy_order_id=payload.buyer_order_id,
            sell_order_id=payload.seller_order_id,
            buy_account_id=payload.buyer_account_id,
            sell_account_id=payload.seller_account_id,
            symbol=payload.ticker.upper(),
            quantity=payload.quantity,
            price=payload.price,
            trade_value=payload.price * payload.quantity,
            created_at=payload.timestamp,
        )
        session.add(trade)
        return trade

    async def _update_buyer(
        self,
        session: AsyncSession,
        payload: TradeExecutedPayload,
    ) -> None:
        """
        Update buyer's account and position after trade.

        - Deduct balance (actual balance, not just available_cash)
        - Add to position (create if doesn't exist)
        """
        trade_value = payload.price * payload.quantity

        # Update account balance
        account = await session.get(Account, payload.buyer_account_id)
        if account:
            account.balance -= trade_value

        # Update position
        position = await self._get_or_create_position(
            session,
            payload.buyer_account_id,
            payload.ticker,
        )

        # Calculate new average price
        if position.quantity > 0:
            total_cost = (position.quantity *
                          position.average_price) + trade_value
            new_quantity = position.quantity + payload.quantity
            position.average_price = total_cost / new_quantity
        else:
            position.average_price = payload.price

        position.quantity += payload.quantity

    async def _update_seller(
        self,
        session: AsyncSession,
        payload: TradeExecutedPayload,
    ) -> None:
        """
        Update seller's account and position after trade.

        - Add to balance
        - Add to available_cash
        - Subtract from position
        - Calculate realized P&L
        """
        trade_value = payload.price * payload.quantity

        # Update account balance
        account = await session.get(Account, payload.seller_account_id)
        if account:
            account.balance += trade_value
            account.available_cash += trade_value

        # Update position
        position = await self._get_position(
            session,
            payload.seller_account_id,
            payload.ticker,
        )

        if position:
            # Calculate realized P&L
            realized_pnl = (
                payload.price - position.average_price) * payload.quantity
            position.realized_pnl += realized_pnl
            position.quantity -= payload.quantity

    # =========================================================================
    # Position Operations
    # =========================================================================

    async def _get_position(
        self,
        session: AsyncSession,
        account_id: int,
        ticker: str,
    ) -> Optional[Position]:
        """Get a position by account and ticker."""
        result = await session.execute(
            select(Position)
            .where(Position.account_id == account_id)
            .where(Position.symbol == ticker.upper())
        )
        return result.scalar_one_or_none()

    async def _get_or_create_position(
        self,
        session: AsyncSession,
        account_id: int,
        ticker: str,
    ) -> Position:
        """Get or create a position for an account."""
        position = await self._get_position(session, account_id, ticker)

        if not position:
            position = Position(
                account_id=account_id,
                symbol=ticker.upper(),
                quantity=0,
                average_price=0.0,
                unrealized_pnl=0.0,
                realized_pnl=0.0,
            )
            session.add(position)

        return position

    # =========================================================================
    # Order Status Updates
    # =========================================================================

    async def _update_order_fill(
        self,
        session: AsyncSession,
        order_id: str,
        fill_quantity: int,
    ) -> None:
        """
        Update order's filled_quantity and status after a trade.

        Args:
            session: Database session
            order_id: The order ID to update
            fill_quantity: The quantity filled in this trade
        """
        order = await session.get(Order, order_id)
        if not order:
            self._logger.warning(f"Order {order_id} not found for fill update")
            return

        # Increment filled quantity
        order.filled_quantity += fill_quantity

        # Update status based on fill
        if order.filled_quantity >= order.quantity:
            order.status = OrderStatus.FILLED
            self._logger.info(f"Order {order_id} fully filled")
        elif order.filled_quantity > 0:
            order.status = OrderStatus.PARTIAL
            self._logger.info(
                f"Order {order_id} partially filled: "
                f"{order.filled_quantity}/{order.quantity}"
            )
