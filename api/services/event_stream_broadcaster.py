"""
Event Stream Broadcaster service for the QuantX trading system.

Responsibilities:
    - Broadcasts discrete trading events (trades, new orders, cancellations)
    - Supports public streams (visible to all subscribers for a ticker)
    - Supports private streams (only the account owner sees their activity)
    - Authenticates users at subscription time for private streams

Does NOT:
    - Maintain order book state (MarketDataBroadcaster does this)
    - Send snapshots (only discrete events)
    - Handle order submission (BroadcastingService does this)
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple, TYPE_CHECKING

from .base_service import BaseService
from .event_bus import EventBus, EventHandler
from .events import (
    Event,
    EventType,
    TradeExecutedPayload,
    OrderCancelledPayload,
    ValidatedOrderPayload,
)

if TYPE_CHECKING:
    from .broadcasting_service import WebSocketClient

logger = logging.getLogger(__name__)


class EventStreamBroadcaster(BaseService):
    """
    Event stream broadcasting service.

    Broadcasts discrete trading events to subscribed clients:
    - Public: trades, new orders, order cancellations (visible to all)
    - Private: events specific to the authenticated user's account
    """

    def __init__(
        self,
        event_bus: EventBus,
        auth_service,
        tickers: List[str],
    ):
        """
        Initialize the event stream broadcaster.

        Args:
            event_bus: Shared event bus
            auth_service: Firebase authentication service for private subscriptions
            tickers: List of supported ticker symbols
        """
        super().__init__(event_bus)

        self.auth_service = auth_service
        self.tickers = [t.upper() for t in tickers]

        # Public subscriptions: ticker -> set of clients
        self._public_subscriptions: Dict[str, Set["WebSocketClient"]] = {
            ticker: set() for ticker in self.tickers
        }

        # Private subscriptions: account_id -> set of clients
        # A user can have multiple connections watching their account
        self._private_subscriptions: Dict[int, Set["WebSocketClient"]] = {}

        # Reverse lookup: client_id -> account_id (for cleanup)
        self._client_to_account: Dict[str, int] = {}

        # Lock for subscription management
        self._lock = asyncio.Lock()

        # Statistics
        self._public_events_sent = 0
        self._private_events_sent = 0

    @property
    def service_name(self) -> str:
        return "EventStreamBroadcaster"

    def _get_subscriptions(self) -> List[Tuple[EventType, EventHandler]]:
        return [
            (EventType.TRADE_EXECUTED, self._handle_trade_executed),
            (EventType.VALIDATED_ORDER, self._handle_new_order),
            (EventType.ORDER_CANCELLED, self._handle_order_cancelled),
        ]

    # =========================================================================
    # Public Subscription Management
    # =========================================================================

    async def subscribe_public(
        self,
        client: "WebSocketClient",
        ticker: str,
    ) -> bool:
        """
        Subscribe a client to public events for a ticker.

        No authentication required - anyone can see public trades/orders.

        Args:
            client: The WebSocket client
            ticker: Ticker symbol to subscribe to

        Returns:
            True if subscribed successfully
        """
        ticker = ticker.upper()

        if ticker not in self.tickers:
            self._logger.warning(
                f"Client {client.client_id} tried to subscribe to invalid ticker: {ticker}"
            )
            return False

        async with self._lock:
            self._public_subscriptions[ticker].add(client)

        self._logger.debug(
            f"Client {client.client_id} subscribed to public stream for {ticker}"
        )

        await client.send_success(
            "subscribed",
            {"stream": "public", "ticker": ticker}
        )

        return True

    async def unsubscribe_public(
        self,
        client: "WebSocketClient",
        ticker: str,
    ) -> bool:
        """Unsubscribe a client from public events for a ticker."""
        ticker = ticker.upper()

        if ticker not in self.tickers:
            return False

        async with self._lock:
            self._public_subscriptions[ticker].discard(client)

        self._logger.debug(
            f"Client {client.client_id} unsubscribed from public stream for {ticker}"
        )

        return True

    # =========================================================================
    # Private Subscription Management
    # =========================================================================

    async def subscribe_private(
        self,
        client: "WebSocketClient",
        token: str,
    ) -> bool:
        """
        Subscribe a client to private events for their account.

        Requires authentication - validates the token and extracts account_id.

        Args:
            client: The WebSocket client
            token: Firebase ID token for authentication

        Returns:
            True if subscribed successfully
        """
        # Authenticate the user
        auth_result = await self.auth_service.validate_token(token)

        if not auth_result.get("success", False):
            error_code = auth_result.get("error_code", "AUTH_ERROR")
            error_message = auth_result.get("error", "Authentication failed")
            self._logger.warning(
                f"Private subscription auth failed for client {client.client_id}: {error_code}"
            )
            await client.send_error(error_code, error_message)
            return False

        # Extract user info
        user_id = auth_result.get("user_id")
        account_id = auth_result.get("account_id")

        if not account_id:
            self._logger.warning(
                f"No account_id in auth result for client {client.client_id}"
            )
            await client.send_error(
                "NO_ACCOUNT",
                "No trading account found for this user"
            )
            return False

        # Update client auth status
        client.authenticated = True
        client.user_id = user_id

        async with self._lock:
            # Add to private subscriptions
            if account_id not in self._private_subscriptions:
                self._private_subscriptions[account_id] = set()
            self._private_subscriptions[account_id].add(client)

            # Track for cleanup
            self._client_to_account[client.client_id] = account_id

        self._logger.info(
            f"Client {client.client_id} subscribed to private stream for account {account_id}"
        )

        await client.send_success(
            "subscribed",
            {"stream": "private", "account_id": account_id}
        )

        return True

    async def unsubscribe_private(self, client: "WebSocketClient") -> bool:
        """Unsubscribe a client from their private event stream."""
        async with self._lock:
            account_id = self._client_to_account.pop(client.client_id, None)

            if account_id and account_id in self._private_subscriptions:
                self._private_subscriptions[account_id].discard(client)
                # Clean up empty sets
                if not self._private_subscriptions[account_id]:
                    del self._private_subscriptions[account_id]

        self._logger.debug(
            f"Client {client.client_id} unsubscribed from private stream"
        )

        return True

    async def unsubscribe_all(self, client: "WebSocketClient") -> None:
        """Unsubscribe a client from all streams (public and private)."""
        async with self._lock:
            # Remove from all public subscriptions
            for ticker in self.tickers:
                self._public_subscriptions[ticker].discard(client)

            # Remove from private subscriptions
            account_id = self._client_to_account.pop(client.client_id, None)
            if account_id and account_id in self._private_subscriptions:
                self._private_subscriptions[account_id].discard(client)
                if not self._private_subscriptions[account_id]:
                    del self._private_subscriptions[account_id]

        self._logger.debug(
            f"Client {client.client_id} unsubscribed from all event streams"
        )

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _handle_trade_executed(self, event: Event) -> None:
        """
        Handle TRADE_EXECUTED events.

        Broadcasts to:
        - All public subscribers for the ticker
        - Private subscribers for both buyer and seller accounts
        """
        payload = TradeExecutedPayload.from_dict(event.payload)
        ticker = payload.ticker.upper()

        # Format messages
        public_message = self._format_public_trade_message(payload)
        buyer_message = self._format_private_trade_message(payload, is_buyer=True)
        seller_message = self._format_private_trade_message(payload, is_buyer=False)

        # Broadcast to public subscribers
        await self._broadcast_to_public(ticker, public_message)

        # Broadcast to private subscribers (buyer and seller)
        await self._broadcast_to_private(payload.buyer_account_id, buyer_message)
        await self._broadcast_to_private(payload.seller_account_id, seller_message)

    async def _handle_new_order(self, event: Event) -> None:
        """
        Handle VALIDATED_ORDER events (new orders entering the book).

        Broadcasts to:
        - All public subscribers for the ticker (anonymized order info)
        - Private subscriber for the account placing the order
        """
        payload = ValidatedOrderPayload.from_dict(event.payload)
        ticker = payload.ticker.upper()

        # Format messages
        public_message = self._format_public_order_message(payload)
        private_message = self._format_private_order_message(payload)

        # Broadcast to public subscribers
        await self._broadcast_to_public(ticker, public_message)

        # Broadcast to the order owner's private stream
        await self._broadcast_to_private(payload.account_id, private_message)

    async def _handle_order_cancelled(self, event: Event) -> None:
        """
        Handle ORDER_CANCELLED events.

        Broadcasts to:
        - All public subscribers for the ticker
        - Private subscriber for the account that cancelled
        """
        payload = OrderCancelledPayload.from_dict(event.payload)
        ticker = payload.ticker.upper()

        # Format messages
        public_message = self._format_public_cancel_message(payload)
        private_message = self._format_private_cancel_message(payload)

        # Broadcast to public subscribers
        await self._broadcast_to_public(ticker, public_message)

        # Broadcast to the order owner's private stream
        await self._broadcast_to_private(payload.account_id, private_message)

    # =========================================================================
    # Broadcasting Helpers
    # =========================================================================

    async def _broadcast_to_public(self, ticker: str, message: dict) -> None:
        """Broadcast a message to all public subscribers for a ticker."""
        if ticker not in self._public_subscriptions:
            return

        async with self._lock:
            clients = list(self._public_subscriptions[ticker])

        if not clients:
            return

        results = await asyncio.gather(
            *(self._send_to_client(client, message) for client in clients),
            return_exceptions=True,
        )

        successful = sum(1 for r in results if r is True)
        self._public_events_sent += successful

        self._logger.debug(
            f"Broadcast public event to {successful}/{len(clients)} clients for {ticker}"
        )

    async def _broadcast_to_private(self, account_id: int, message: dict) -> None:
        """Broadcast a message to all private subscribers for an account."""
        async with self._lock:
            clients = list(self._private_subscriptions.get(account_id, []))

        if not clients:
            return

        results = await asyncio.gather(
            *(self._send_to_client(client, message) for client in clients),
            return_exceptions=True,
        )

        successful = sum(1 for r in results if r is True)
        self._private_events_sent += successful

        self._logger.debug(
            f"Broadcast private event to {successful}/{len(clients)} clients for account {account_id}"
        )

    async def _send_to_client(
        self,
        client: "WebSocketClient",
        message: dict,
    ) -> bool:
        """Send a message to a single client."""
        try:
            return await client.send(message)
        except Exception as e:
            self._logger.error(
                f"Error sending to client {client.client_id}: {e}"
            )
            return False

    # =========================================================================
    # Message Formatting - Public Events
    # =========================================================================

    def _format_public_trade_message(self, payload: TradeExecutedPayload) -> dict:
        """
        Format a trade for public broadcast.

        Public view is anonymized - no account IDs or order IDs.
        """
        return {
            "type": "trade",
            "ticker": payload.ticker,
            "price": payload.price,
            "quantity": payload.quantity,
            "timestamp": payload.timestamp.isoformat(),
        }

    def _format_public_order_message(self, payload: ValidatedOrderPayload) -> dict:
        """
        Format a new order for public broadcast.

        Public view is anonymized - no account ID, user info, or order ID.
        """
        return {
            "type": "new_order",
            "ticker": payload.ticker,
            "side": payload.side.value,
            "order_type": payload.order_type.value,
            "quantity": payload.quantity,
            "price": payload.price,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _format_public_cancel_message(self, payload: OrderCancelledPayload) -> dict:
        """
        Format an order cancellation for public broadcast.

        Public view is anonymized.
        """
        return {
            "type": "order_cancelled",
            "ticker": payload.ticker,
            "side": payload.side.value,
            "price": payload.price,
            "quantity": payload.remaining_quantity,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # =========================================================================
    # Message Formatting - Private Events
    # =========================================================================

    def _format_private_trade_message(
        self,
        payload: TradeExecutedPayload,
        is_buyer: bool,
    ) -> dict:
        """
        Format a trade for the private stream of a participant.

        Includes full details relevant to the user.
        """
        return {
            "type": "private_fill",
            "trade_id": payload.trade_id,
            "ticker": payload.ticker,
            "side": "buy" if is_buyer else "sell",
            "price": payload.price,
            "quantity": payload.quantity,
            "order_id": payload.buyer_order_id if is_buyer else payload.seller_order_id,
            "timestamp": payload.timestamp.isoformat(),
        }

    def _format_private_order_message(self, payload: ValidatedOrderPayload) -> dict:
        """
        Format a new order for the private stream of the order owner.

        Includes full order details.
        """
        return {
            "type": "private_order_accepted",
            "order_id": payload.order_id,
            "ticker": payload.ticker,
            "side": payload.side.value,
            "order_type": payload.order_type.value,
            "quantity": payload.quantity,
            "price": payload.price,
            "estimated_value": payload.estimated_value,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def _format_private_cancel_message(self, payload: OrderCancelledPayload) -> dict:
        """
        Format an order cancellation for the private stream of the order owner.

        Includes order ID for tracking.
        """
        return {
            "type": "private_order_cancelled",
            "order_id": payload.order_id,
            "ticker": payload.ticker,
            "side": payload.side.value,
            "price": payload.price,
            "cancelled_quantity": payload.remaining_quantity,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # =========================================================================
    # Health Check
    # =========================================================================

    async def health_check(self) -> dict:
        """Return health metrics for the event stream broadcaster."""
        base = await super().health_check()

        public_subscriber_counts = {}
        private_subscriber_count = 0

        async with self._lock:
            for ticker in self.tickers:
                public_subscriber_counts[ticker] = len(self._public_subscriptions[ticker])
            private_subscriber_count = sum(
                len(clients) for clients in self._private_subscriptions.values()
            )

        return {
            **base,
            "tickers": self.tickers,
            "public_subscribers_per_ticker": public_subscriber_counts,
            "total_public_subscribers": sum(public_subscriber_counts.values()),
            "total_private_subscribers": private_subscriber_count,
            "public_events_sent": self._public_events_sent,
            "private_events_sent": self._private_events_sent,
        }
