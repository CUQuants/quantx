"""
Broadcasting Service (WebSocket Handler) for the QuantX trading system.

Responsibilities:
    - Runs WebSocket server to receive client orders
    - Authenticates users using Firebase auth
    - Publishes RAW_ORDER events after authentication
    - Subscribes to ORDER_REJECTED events to send error responses
    - Subscribes to ORDER_PERSISTED events to send confirmations
    - Manages client connections by websocket ID

Does NOT:
    - Validate orders (RiskEngine handles this)
    - Touch the database (PersistenceService handles this)
    - Maintain order book state (MatchingEngine handles this)
    - Broadcast market data (MarketDataBroadcaster handles this)
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

from models import OrderSide, OrderType

from .base_service import BaseService
from .event_bus import EventBus, EventHandler
from .events import (
    Event,
    EventType,
    RawOrderPayload,
    OrderRejectedPayload,
    OrderPersistedPayload,
)

logger = logging.getLogger(__name__)


class WebSocketClient:
    """
    Wrapper for a WebSocket connection.

    Provides a consistent interface for sending messages
    regardless of the underlying WebSocket implementation.
    """

    def __init__(self, websocket, client_id: str):
        """
        Initialize a WebSocket client wrapper.

        Args:
            websocket: The underlying WebSocket connection
            client_id: Unique identifier for this client
        """
        self.websocket = websocket
        self.client_id = client_id
        self.connected_at = datetime.now(timezone.utc)
        self.authenticated = False
        self.user_id: Optional[str] = None
        self.email: Optional[str] = None
        self.subscribed_tickers: Set[str] = set()

    async def send(self, message: dict) -> bool:
        """
        Send a message to the client.

        Args:
            message: Dictionary to send as JSON

        Returns:
            True if sent successfully, False otherwise
        """
        try:
            await self.websocket.send_json(message)
            return True
        except Exception as e:
            logger.error(f"Failed to send to client {self.client_id}: {e}")
            return False

    async def send_error(self, error_type: str, error_message: str) -> bool:
        """Send an error message to the client."""
        return await self.send({
            "type": "error",
            "error_type": error_type,
            "error_message": error_message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    async def send_success(self, message_type: str, data: dict = None) -> bool:
        """Send a success message to the client."""
        msg = {
            "type": message_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if data:
            msg.update(data)
        return await self.send(msg)


class BroadcastingService(BaseService):
    """
    WebSocket broadcasting service for client communication.

    Handles client connections, authentication, and order submission.
    Does NOT broadcast market data (MarketDataBroadcaster handles that).
    """

    def __init__(
        self,
        event_bus: EventBus,
        auth_service,
        tickers: List[str],
    ):
        """
        Initialize the broadcasting service.

        Args:
            event_bus: Shared event bus
            auth_service: Firebase auth service for token validation
            tickers: List of supported ticker symbols
        """
        super().__init__(event_bus)

        self.auth_service = auth_service
        self.tickers = [t.upper() for t in tickers]

        # Client management
        self._clients: Dict[str, WebSocketClient] = {}
        self._clients_lock = asyncio.Lock()

    @property
    def service_name(self) -> str:
        return "BroadcastingService"

    def _get_subscriptions(self) -> List[Tuple[EventType, EventHandler]]:
        return [
            (EventType.ORDER_REJECTED, self._handle_order_rejected),
            (EventType.ORDER_PERSISTED, self._handle_order_persisted),
        ]

    # =========================================================================
    # Client Management
    # =========================================================================

    async def add_client(self, websocket, ticker: str) -> Optional[WebSocketClient]:
        """
        Add a new WebSocket client.

        Args:
            websocket: The WebSocket connection
            ticker: The ticker the client is subscribing to

        Returns:
            WebSocketClient wrapper or None if ticker is invalid
        """
        ticker = ticker.upper()

        if ticker not in self.tickers:
            self._logger.warning(
                f"Client tried to subscribe to invalid ticker: {ticker}")
            return None

        client_id = str(uuid.uuid4())
        client = WebSocketClient(websocket, client_id)
        client.subscribed_tickers.add(ticker)

        async with self._clients_lock:
            self._clients[client_id] = client

        self._logger.info(f"Client {client_id} connected for ticker {ticker}")
        return client

    async def remove_client(self, client: WebSocketClient) -> None:
        """Remove a client from the service."""
        async with self._clients_lock:
            if client.client_id in self._clients:
                del self._clients[client.client_id]
                self._logger.info(f"Client {client.client_id} disconnected")

    async def get_client(self, client_id: str) -> Optional[WebSocketClient]:
        """Get a client by ID."""
        async with self._clients_lock:
            return self._clients.get(client_id)

    # =========================================================================
    # Message Handling
    # =========================================================================

    async def handle_message(self, client: WebSocketClient, message: dict) -> None:
        """
        Handle an incoming message from a client.

        Args:
            client: The client that sent the message
            message: The parsed message dictionary
        """
        message_type = message.get("type")

        if not message_type:
            await client.send_error("MISSING_TYPE", "Message type is required")
            return

        if message_type == "order":
            await self._handle_order_message(client, message)
        else:
            await client.send_error(
                "INVALID_MESSAGE_TYPE",
                f"Unknown message type: {message_type}"
            )

    async def _handle_order_message(
        self,
        client: WebSocketClient,
        message: dict,
    ) -> None:
        """
        Handle an order message from a client.

        Authenticates the user and publishes a RAW_ORDER event.
        """
        # Authenticate
        token = message.get("token")
        auth_result = self.auth_service.validate_token(token)

        if not auth_result.get("success", False):
            error_code = auth_result.get("error_code", "AUTH_ERROR")
            error_message = auth_result.get("error", "Authentication failed")
            self._logger.warning(
                f"Auth failed for client {client.client_id}: {error_code}")
            await client.send_error(error_code, error_message)
            return

        # Extract user info
        user_id = auth_result.get("user_id")
        email = auth_result.get("email")

        # Update client auth status
        client.authenticated = True
        client.user_id = user_id
        client.email = email

        # Parse order
        order_data = message.get("order", {})

        try:
            ticker = order_data.get("ticker", "").upper()
            side_str = order_data.get("type", "")  # "Buy" or "Sell"
            side = OrderSide.BUY if side_str == "Buy" else OrderSide.SELL
            quantity = int(order_data.get("quantity", 0))
            price = order_data.get("price")
            order_type_str = order_data.get("orderType", "LIMIT")
            order_type = OrderType.LIMIT if order_type_str == "LIMIT" else OrderType.MARKET

            if price is not None:
                price = float(price)

        except (ValueError, TypeError) as e:
            await client.send_error("INVALID_ORDER_FORMAT", f"Invalid order data: {e}")
            return

        # Validate ticker
        if ticker not in self.tickers:
            await client.send_error("INVALID_TICKER", f"Invalid ticker: {ticker}")
            return

        # Create RAW_ORDER payload
        payload = RawOrderPayload(
            ticker=ticker,
            side=side,
            order_type=order_type,
            quantity=quantity,
            price=price,
            user_id=user_id,
            email=email,
            websocket_id=client.client_id,
        )

        self._logger.info(
            f"Publishing RAW_ORDER: {side.value} {quantity} {ticker} @ {price} "
            f"from user {user_id}"
        )

        # Publish RAW_ORDER event
        await self.publish(EventType.RAW_ORDER, payload.to_dict())

    # =========================================================================
    # Event Handlers
    # =========================================================================

    async def _handle_order_rejected(self, event: Event) -> None:
        """
        Handle ORDER_REJECTED event - send error to client.
        """
        payload = OrderRejectedPayload.from_dict(event.payload)

        client = await self.get_client(payload.websocket_id)
        if client:
            await client.send_error(
                payload.rejection_code,
                payload.rejection_reason,
            )
            self._logger.info(
                f"Sent rejection to client {payload.websocket_id}: "
                f"{payload.rejection_code}"
            )

    async def _handle_order_persisted(self, event: Event) -> None:
        """
        Handle ORDER_PERSISTED event - send confirmation to client.
        """
        payload = OrderPersistedPayload.from_dict(event.payload)

        client = await self.get_client(payload.websocket_id)
        if client:
            await client.send_success(
                "order_success",
                {
                    "message": "Order placed successfully",
                    "order_id": payload.order_id,
                    "ticker": payload.ticker,
                    "status": payload.status.value,
                }
            )
            self._logger.info(
                f"Sent confirmation to client {payload.websocket_id}: "
                f"order {payload.order_id}"
            )

    # =========================================================================
    # Health Check
    # =========================================================================

    async def health_check(self) -> dict:
        """Return health metrics for the broadcasting service."""
        base = await super().health_check()

        async with self._clients_lock:
            client_count = len(self._clients)
            authenticated_count = sum(
                1 for c in self._clients.values() if c.authenticated
            )

        return {
            **base,
            "tickers": self.tickers,
            "connected_clients": client_count,
            "authenticated_clients": authenticated_count,
        }
