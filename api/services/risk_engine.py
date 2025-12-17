"""
Risk Engine service for the QuantX trading system.

Responsibilities:
    - Subscribes to RAW_ORDER events
    - Validates orders before they reach the matching engine
    - For limit orders: Check user has sufficient cash (buys) or shares (sells)
    - For market orders: Query MatchingEngine's get_best_price() with safety buffer
    - Reads user balance/position data from database (read-only)
    - Publishes VALIDATED_ORDER or ORDER_REJECTED events

Does NOT:
    - Write to database
    - Execute trades
    - Manage order books
"""

import logging
import uuid
from typing import List, Optional, Tuple, TYPE_CHECKING

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Account, Position, OrderSide, OrderType

from .base_service import BaseService
from .event_bus import EventBus, EventHandler
from .events import (
    Event,
    EventType,
    RawOrderPayload,
    ValidatedOrderPayload,
    OrderRejectedPayload,
)

if TYPE_CHECKING:
    from .matching_engine import MatchingEngine

logger = logging.getLogger(__name__)

# Safety buffer for market orders (10% premium for buys)
MARKET_ORDER_SAFETY_BUFFER = 1.10


class RiskEngine(BaseService):
    """
    Risk validation service for orders.

    Validates orders against user balances and positions before
    forwarding them to the matching engine.
    """

    def __init__(
        self,
        event_bus: EventBus,
        session_factory,
        matching_engine: "MatchingEngine",
    ):
        """
        Initialize the risk engine.

        Args:
            event_bus: Shared event bus
            session_factory: Async session factory for DB reads
            matching_engine: Reference to matching engine for price queries
        """
        super().__init__(event_bus)

        self._session_factory = session_factory
        self._matching_engine = matching_engine

    @property
    def service_name(self) -> str:
        return "RiskEngine"

    def _get_subscriptions(self) -> List[Tuple[EventType, EventHandler]]:
        return [
            (EventType.RAW_ORDER, self._handle_raw_order),
        ]

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _handle_raw_order(self, event: Event) -> None:
        """
        Handle a raw order event from the broadcasting service.

        Validates the order and publishes either VALIDATED_ORDER or ORDER_REJECTED.
        """
        payload = RawOrderPayload.from_dict(event.payload)

        self._logger.info(
            f"Validating order: {payload.side.value} {payload.quantity} "
            f"{payload.ticker} @ {payload.price} for user {payload.user_id}"
        )

        try:
            # Validate and get account
            validated_payload = await self._validate_order(payload)

            self._logger.info(
                f"Order validated: {payload.side.value} {payload.quantity} "
                f"{payload.ticker} for account {validated_payload.account_id}"
            )

            # Publish validated order
            await self.publish(
                EventType.VALIDATED_ORDER,
                validated_payload.to_dict(),
                event.correlation_id,
            )

        except OrderValidationError as e:
            self._logger.warning(
                f"Order rejected: {e.code} - {e.message} "
                f"(user={payload.user_id})"
            )

            # Publish rejection
            rejection = OrderRejectedPayload(
                ticker=payload.ticker,
                side=payload.side,
                order_type=payload.order_type,
                quantity=payload.quantity,
                price=payload.price,
                user_id=payload.user_id,
                websocket_id=payload.websocket_id,
                rejection_reason=e.message,
                rejection_code=e.code,
            )

            await self.publish(
                EventType.ORDER_REJECTED,
                rejection.to_dict(),
                event.correlation_id,
            )

    # =========================================================================
    # Validation Logic
    # =========================================================================

    async def _validate_order(self, payload: RawOrderPayload) -> ValidatedOrderPayload:
        """
        Validate an order against user account and positions.

        Args:
            payload: Raw order payload

        Returns:
            ValidatedOrderPayload if valid

        Raises:
            OrderValidationError if invalid
        """
        async with self._session_factory() as session:
            # Get or create account
            account = await self._get_account(session, payload.user_id, payload.email)

            if not account:
                raise OrderValidationError(
                    "ACCOUNT_NOT_FOUND",
                    "Unable to find or create account"
                )

            # Basic validation
            self._validate_basic_fields(payload)

            # Determine execution price for validation
            execution_price = await self._get_execution_price(payload)
            estimated_value = execution_price * payload.quantity

            # Validate based on order side
            if payload.side == OrderSide.BUY:
                await self._validate_buy_order(
                    session, account, payload, execution_price, estimated_value
                )
            else:
                await self._validate_sell_order(
                    session, account, payload
                )

            # Check for self-matching (optional, can be expanded)
            # await self._check_self_matching(session, account, payload)

            return ValidatedOrderPayload(
                order_id=str(uuid.uuid4()),
                ticker=payload.ticker,
                side=payload.side,
                order_type=payload.order_type,
                quantity=payload.quantity,
                price=execution_price,
                user_id=payload.user_id,
                email=payload.email,
                account_id=account.id,
                websocket_id=payload.websocket_id,
                estimated_value=estimated_value,
            )

    def _validate_basic_fields(self, payload: RawOrderPayload) -> None:
        """Validate basic order fields."""
        if payload.quantity <= 0:
            raise OrderValidationError(
                "INVALID_QUANTITY",
                "Order quantity must be positive"
            )

        if payload.order_type == OrderType.LIMIT:
            if payload.price is None or payload.price <= 0:
                raise OrderValidationError(
                    "INVALID_PRICE",
                    "Limit orders require a positive price"
                )

    async def _get_execution_price(self, payload: RawOrderPayload) -> float:
        """
        Get the execution price for validation.

        For limit orders: use the order price
        For market orders: query matching engine and apply safety buffer
        """
        if payload.order_type == OrderType.LIMIT:
            return payload.price

        # Market order - query matching engine for best price
        best_price = self._matching_engine.get_best_price(
            payload.ticker,
            payload.side
        )

        if best_price is None:
            raise OrderValidationError(
                "NO_LIQUIDITY",
                f"No liquidity available for {payload.ticker}"
            )

        # Apply safety buffer for buy orders
        if payload.side == OrderSide.BUY:
            return best_price * MARKET_ORDER_SAFETY_BUFFER

        return best_price

    async def _validate_buy_order(
        self,
        session: AsyncSession,
        account: Account,
        payload: RawOrderPayload,
        execution_price: float,
        estimated_value: float,
    ) -> None:
        """
        Validate a buy order.

        Checks that user has sufficient available cash.
        """
        if account.available_cash < estimated_value:
            raise OrderValidationError(
                "INSUFFICIENT_FUNDS",
                f"Insufficient funds. Required: ${estimated_value:.2f}, "
                f"Available: ${account.available_cash:.2f}"
            )

    async def _validate_sell_order(
        self,
        session: AsyncSession,
        account: Account,
        payload: RawOrderPayload,
    ) -> None:
        """
        Validate a sell order.

        Checks that user has sufficient available shares to sell.
        Available shares = total quantity - reserved shares (in active sell orders).
        """
        position = await self._get_position(
            session, account.id, payload.ticker
        )

        if "BOT" in account.firebase_uid:
            return

        if not position:
            raise OrderValidationError(
                "NO_POSITION",
                f"No position found for {payload.ticker}"
            )

        # Check available shares (not reserved in other sell orders)
        available_shares = position.quantity - position.reserved_shares

        if available_shares < payload.quantity:
            raise OrderValidationError(
                "INSUFFICIENT_SHARES",
                f"Insufficient shares. Required: {payload.quantity}, "
                f"Available: {available_shares}"
            )

    # =========================================================================
    # Database Queries (Read-Only)
    # =========================================================================

    async def _get_account(
        self,
        session: AsyncSession,
        firebase_uid: str,
        email: str,
    ) -> Optional[Account]:
        """
        Get account by Firebase UID, creating if necessary.

        Note: Account creation is a side effect that may need to move
        to PersistenceService in a stricter implementation.
        """
        result = await session.execute(
            select(Account).where(Account.firebase_uid == firebase_uid)
        )
        account = result.scalar_one_or_none()

        if account:
            return account

        # Create new account
        # Note: In a stricter implementation, this should go through
        # PersistenceService. For now, we keep it here for compatibility.
        new_account = Account(
            firebase_uid=firebase_uid,
            username=email,
        )

        # Bot accounts get higher balance
        if firebase_uid.startswith("BOT_ID"):
            new_account.balance = 10000000.0
            new_account.available_cash = 10000000.0

        session.add(new_account)
        await session.flush()
        await session.refresh(new_account)

        return new_account

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


class OrderValidationError(Exception):
    """Exception raised when order validation fails."""

    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")
