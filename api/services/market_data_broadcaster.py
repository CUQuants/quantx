"""
Market Data Broadcaster service for the QuantX trading system.

Responsibilities:
    - Subscribes to MARKET_DATA_UPDATE events from Matching Engine
    - Maintains WebSocket client subscriptions per ticker
    - Broadcasts market data snapshots to all subscribed clients
    - Handles client subscribe/unsubscribe requests

Does NOT:
    - Maintain order book state (MatchingEngine does this)
    - Process orders
    - Calculate market data snapshots
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
    MarketDataUpdatePayload,
)

if TYPE_CHECKING:
    from .broadcasting_service import WebSocketClient

logger = logging.getLogger(__name__)


class MarketDataBroadcaster(BaseService):
    """
    Market data broadcasting service.

    Receives market data updates from the Matching Engine and
    broadcasts them to all clients subscribed to that ticker.
    """

    def __init__(self, event_bus: EventBus, tickers: List[str]):
        """
        Initialize the market data broadcaster.

        Args:
            event_bus: Shared event bus
            tickers: List of supported ticker symbols
        """
        super().__init__(event_bus)

        self.tickers = [t.upper() for t in tickers]

        # Client subscriptions per ticker
        # ticker -> set of WebSocketClient objects
        self._subscriptions: Dict[str, Set["WebSocketClient"]] = {
            ticker: set() for ticker in self.tickers
        }

        # Lock for subscription management
        self._lock = asyncio.Lock()

        # Track last snapshot per ticker for new subscribers
        self._last_snapshots: Dict[str, MarketDataUpdatePayload] = {}

        # Statistics
        self._broadcasts_sent = 0
        self._messages_sent = 0

    @property
    def service_name(self) -> str:
        return "MarketDataBroadcaster"

    def _get_subscriptions(self) -> List[Tuple[EventType, EventHandler]]:
        return [
            (EventType.MARKET_DATA_UPDATE, self._handle_market_data_update),
        ]

    # =========================================================================
    # Subscription Management
    # =========================================================================

    async def subscribe(self, client: "WebSocketClient", ticker: str) -> bool:
        """
        Subscribe a client to market data for a ticker.

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
            self._subscriptions[ticker].add(client)
            client.subscribed_tickers.add(ticker)

        self._logger.debug(
            f"Client {client.client_id} subscribed to {ticker}"
        )

        # If the snapshot is available, it will send it to the client
        # If not, it will request it to load from the database, and perform a market data refresh
        if ticker in self._last_snapshots:
            await self._send_snapshot_to_client(
                client,
                self._last_snapshots[ticker]
            )
        else:
            await self.publish(EventType.REFRESH_MARKET_DATA, {"ticker": ticker})

        return True

    def _snapshot_empty(self, ticker: str):
        snapshot = self._last_snapshots[ticker]
        if len(snapshot.bids) == 0 and len(snapshot.asks) == 0:
            return True
        else:
            return False

    async def unsubscribe(self, client: "WebSocketClient", ticker: str) -> bool:
        ticker = ticker.upper()

        if ticker not in self.tickers:
            return False

        async with self._lock:
            self._subscriptions[ticker].discard(client)
            client.subscribed_tickers.discard(ticker)

        self._logger.debug(
            f"Client {client.client_id} unsubscribed from {ticker}"
        )

        return True

    async def unsubscribe_all(self, client: "WebSocketClient") -> None:
        """
        Unsubscribes a client from all tickers.
        """
        async with self._lock:
            for ticker in self.tickers:
                self._subscriptions[ticker].discard(client)
            client.subscribed_tickers.clear()

        self._logger.debug(
            f"Client {client.client_id} unsubscribed from all tickers"
        )

    async def get_subscriber_count(self, ticker: str) -> int:
        ticker = ticker.upper()
        if ticker not in self._subscriptions:
            return 0

        async with self._lock:
            return len(self._subscriptions[ticker])

    async def _handle_market_data_update(self, event: Event) -> None:
        """
        Handles MARKET_DATA_UPDATE events from the Matching Engine, and broadcasts to subscribed clients.
        """
        payload = MarketDataUpdatePayload.from_dict(event.payload)
        ticker = payload.ticker.upper()

        if ticker not in self.tickers:
            self._logger.warning(
                f"Received update for unknown ticker: {ticker}")
            return

        self._last_snapshots[ticker] = payload

        async with self._lock:
            clients = list(self._subscriptions[ticker])

        if not clients:
            return

        self._logger.debug(
            f"Broadcasting market data for {ticker} to {len(clients)} clients"
        )

        await self._broadcast_to_clients(clients, payload)

        self._broadcasts_sent += 1

    async def _broadcast_to_clients(
        self,
        clients: List["WebSocketClient"],
        payload: MarketDataUpdatePayload,
    ) -> None:
        message = self._format_market_data_message(payload)

        results = await asyncio.gather(
            *(self._send_to_client(client, message) for client in clients),
            return_exceptions=True,
        )

        # counts successful sends
        successful = sum(1 for r in results if r is True)
        self._messages_sent += successful

        for client, result in zip(clients, results):
            if isinstance(result, Exception):
                self._logger.error(
                    f"Failed to send to client {client.client_id}: {result}"
                )

    async def _send_to_client(
        self,
        client: "WebSocketClient",
        message: dict,
    ) -> bool:
        try:
            return await client.send(message)
        except Exception as e:
            self._logger.error(
                f"Error sending to client {client.client_id}: {e}"
            )
            return False

    async def _send_snapshot_to_client(
        self,
        client: "WebSocketClient",
        payload: MarketDataUpdatePayload,
    ) -> bool:
        message = self._format_market_data_message(payload)
        return await client.send(message)

    def _format_market_data_message(
        self,
        payload: MarketDataUpdatePayload,
    ) -> dict:
        """
        Format a market data payload into a client message.

        Matches the existing frontend format for compatibility.
        """
        # Calculate price estimate for frontend
        price_estimate = payload.mid_price
        if price_estimate is None:
            if payload.best_ask is not None:
                price_estimate = payload.best_ask
            elif payload.best_bid is not None:
                price_estimate = payload.best_bid
            else:
                price_estimate = 0.0

        message = {
            "type": "batch",
            "orders": {
                "bids": payload.bids,
                "asks": payload.asks,
                "total_bids": payload.total_bid_quantity,
                "total_asks": payload.total_ask_quantity,
                "price": price_estimate,
            },
        }

        # Include last trade if available
        if payload.last_trade_price is not None:
            message["last_trade"] = payload.last_trade_price

        return message

    async def send_initial_snapshot(
        self,
        client: "WebSocketClient",
        ticker: str,
    ) -> bool:
        """
        Sends the initial market data snapshot to a newly connected client.
        """
        ticker = ticker.upper()

        if ticker not in self._last_snapshots:
            # No data available yet - send empty snapshot
            empty_message = {
                "type": "batch",
                "orders": {
                    "bids": [],
                    "asks": [],
                    "total_bids": 0,
                    "total_asks": 0,
                    "price": 0.0,
                },
            }
            return await client.send(empty_message)

        return await self._send_snapshot_to_client(
            client,
            self._last_snapshots[ticker]
        )

    async def health_check(self) -> dict:
        """
        Return health metrics for the market data broadcaster.
        Custom metrics include:

        - Subscribers pe ticker
        - Total subscribers
        - Broadcasts sent
        - Messages sent

        """
        base = await super().health_check()

        subscriber_counts = {}
        async with self._lock:
            for ticker in self.tickers:
                subscriber_counts[ticker] = len(self._subscriptions[ticker])

        return {
            **base,
            "tickers": self.tickers,
            "subscribers_per_ticker": subscriber_counts,
            "total_subscribers": sum(subscriber_counts.values()),
            "broadcasts_sent": self._broadcasts_sent,
            "messages_sent": self._messages_sent,
        }
