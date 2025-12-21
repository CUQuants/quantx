"""
Synchronous wrapper for the QuantX client.

This provides a simpler interface for users who don't want to deal with async/await.
It runs the async client in a background thread with its own event loop.
"""

import asyncio
import threading
from typing import Callable, Any, Optional, Union

from .client import QuantXClient
from .constants import EventType, OrderSide, OrderType


class QuantXClientSync:
    """
    Synchronous WebSocket client for the QuantX trading platform.
    
    This is a wrapper around the async QuantXClient that handles
    the event loop management for you.
    
    Example usage:
        client = QuantXClientSync(api_key="qntx_live_...")
        
        client.on(EventType.TRADE, lambda trade: print(f"Trade: {trade}"))
        client.on(EventType.ORDER_FILLED, lambda order: print(f"Filled: {order}"))
        
        client.connect("QNTX")
        client.subscribe_private()
        client.send_order(OrderSide.BUY, quantity=10, price=50.00)
        
        # Block and process events until interrupted
        client.run_forever()
    """
    
    def __init__(self, api_key: str, base_url: str = "ws://localhost:8000"):
        """
        Initialize the synchronous QuantX client.
        
        Args:
            api_key: Your QuantX API key (e.g., "qntx_live_abc123...")
            base_url: WebSocket server URL (default: ws://localhost:8000)
        """
        self._api_key = api_key
        self._base_url = base_url
        
        self._client: Optional[QuantXClient] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._started = threading.Event()
    
    # =========================================================================
    # Connection Management
    # =========================================================================
    
    def connect(self, ticker: str) -> None:
        """
        Connect to the QuantX WebSocket server for a specific ticker.
        
        Args:
            ticker: The ticker symbol to connect to (e.g., "QNTX")
            
        Raises:
            ConnectionError: If connection fails
            AuthenticationError: If API key authentication fails
        """
        # Create a new event loop in a background thread
        self._loop = asyncio.new_event_loop()
        self._client = QuantXClient(self._api_key, self._base_url)
        
        # Start the background thread
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        
        # Wait for the loop to be running
        self._started.wait(timeout=5.0)
        
        # Connect (blocking)
        future = asyncio.run_coroutine_threadsafe(
            self._client.connect(ticker),
            self._loop
        )
        future.result(timeout=30.0)  # Wait for connection
    
    def disconnect(self) -> None:
        """Disconnect from the WebSocket server."""
        if self._client and self._loop:
            future = asyncio.run_coroutine_threadsafe(
                self._client.disconnect(),
                self._loop
            )
            try:
                future.result(timeout=5.0)
            except Exception:
                pass
        
        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)
        
        if self._thread:
            self._thread.join(timeout=2.0)
        
        self._client = None
        self._loop = None
        self._thread = None
    
    def run_forever(self) -> None:
        """
        Block until disconnect() is called or connection is lost.
        
        This keeps the main thread alive while processing events.
        Use Ctrl+C to interrupt.
        """
        if not self._client or not self._loop:
            raise RuntimeError("Not connected. Call connect() first.")
        
        try:
            future = asyncio.run_coroutine_threadsafe(
                self._client.run_forever(),
                self._loop
            )
            future.result()  # Block until done
        except KeyboardInterrupt:
            self.disconnect()
    
    @property
    def is_connected(self) -> bool:
        """Check if the client is connected and authenticated."""
        return self._client.is_connected if self._client else False
    
    @property
    def ticker(self) -> Optional[str]:
        """Get the currently connected ticker."""
        return self._client.ticker if self._client else None
    
    @property
    def account_id(self) -> Optional[int]:
        """Get the authenticated account ID."""
        return self._client.account_id if self._client else None
    
    # =========================================================================
    # Event Handling
    # =========================================================================
    
    def on(self, event_type: EventType, handler: Callable[[Any], Any]) -> None:
        """
        Register an event handler.
        
        Args:
            event_type: The type of event to listen for
            handler: Callback function (must be synchronous)
            
        Example:
            client.on(EventType.TRADE, lambda trade: print(trade))
        """
        if not self._client:
            # Store handlers to register after connect
            # For simplicity, require connect() first
            raise RuntimeError("Call connect() before registering handlers")
        
        self._client.on(event_type, handler)
    
    def off(self, event_type: EventType, handler: Callable[[Any], Any]) -> None:
        """
        Remove an event handler.
        
        Args:
            event_type: The type of event
            handler: The handler to remove
        """
        if self._client:
            self._client.off(event_type, handler)
    
    # =========================================================================
    # Subscriptions
    # =========================================================================
    
    def subscribe_public(self, ticker: Optional[str] = None) -> None:
        """
        Subscribe to public event stream for a ticker.
        
        Args:
            ticker: Ticker to subscribe to (defaults to connected ticker)
        """
        self._run_async(self._client.subscribe_public(ticker))
    
    def subscribe_private(self) -> None:
        """
        Subscribe to private event stream for your account.
        
        This enables receiving order updates (accepted, rejected, filled, cancelled).
        """
        self._run_async(self._client.subscribe_private())
    
    def unsubscribe_public(self, ticker: Optional[str] = None) -> None:
        """Unsubscribe from public event stream."""
        self._run_async(self._client.unsubscribe_public(ticker))
    
    def unsubscribe_private(self) -> None:
        """Unsubscribe from private event stream."""
        self._run_async(self._client.unsubscribe_private())
    
    # =========================================================================
    # Trading
    # =========================================================================
    
    def send_order(
        self,
        side: Union[OrderSide, str],
        quantity: int,
        price: float,
        order_type: Union[OrderType, str] = OrderType.LIMIT,
    ) -> None:
        """
        Submit a new order.
        
        Args:
            side: OrderSide.BUY or OrderSide.SELL
            quantity: Number of units to trade
            price: Price per unit
            order_type: OrderType.LIMIT or OrderType.MARKET (default: LIMIT)
            
        Note:
            Order confirmation/rejection will be received via events.
            Register handlers for ORDER_ACCEPTED and ORDER_REJECTED.
        """
        self._run_async(self._client.send_order(side, quantity, price, order_type))
    
    def cancel_order(self, order_id: str) -> None:
        """
        Cancel an existing order.
        
        Args:
            order_id: The ID of the order to cancel
        """
        self._run_async(self._client.cancel_order(order_id))
    
    # =========================================================================
    # Context Manager
    # =========================================================================
    
    def __enter__(self) -> "QuantXClientSync":
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()
    
    # =========================================================================
    # Private Methods
    # =========================================================================
    
    def _run_loop(self) -> None:
        """Run the event loop in a background thread."""
        asyncio.set_event_loop(self._loop)
        self._started.set()
        self._loop.run_forever()
    
    def _run_async(self, coro) -> Any:
        """Run an async coroutine from sync code."""
        if not self._client or not self._loop:
            raise RuntimeError("Not connected. Call connect() first.")
        
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result(timeout=30.0)

