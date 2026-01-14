"""
Async WebSocket client for the QuantX trading platform.

This is the main client class that handles WebSocket connections,
authentication, event handling, and order management.
"""

import asyncio
import json
import logging
from typing import Callable, Dict, List, Any, Optional, Union

import websockets
from websockets.client import WebSocketClientProtocol

from .constants import EventType, OrderSide, OrderType
from .exceptions import (
    AuthenticationError,
    ConnectionError,
    NotConnectedError,
    OrderError,
)
from .models import AuthInfo, MarketData, Trade, Order, ErrorInfo

# Configure logging
logger = logging.getLogger("quantx.client")

# Type alias for event handlers
EventHandler = Callable[[Any], Any]


class QuantXClient:
    """
    Async WebSocket client for the QuantX trading platform.
    
    Example usage:
        async def main():
            client = QuantXClient(api_key="qntx_live_...")
            
            client.on(EventType.TRADE, lambda trade: print(f"Trade: {trade}"))
            client.on(EventType.ORDER_FILLED, lambda order: print(f"Filled: {order}"))
            
            await client.connect("QNTX")
            await client.subscribe_private()
            await client.send_order(OrderSide.BUY, quantity=10, price=50.00)
            
            # Keep running
            await client.run_forever()
            
        asyncio.run(main())
    """
    
    def __init__(self, api_key: str, base_url: str = "ws://localhost:8000"):
        """
        Initialize the QuantX client.
        
        Args:
            api_key: Your QuantX API key (e.g., "qntx_live_abc123...")
            base_url: WebSocket server URL (default: ws://localhost:8000)
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        
        self._ws: Optional[WebSocketClientProtocol] = None
        self._ticker: Optional[str] = None
        self._auth_info: Optional[AuthInfo] = None
        self._is_authenticated = False
        self._is_running = False
        
        self._handlers: Dict[EventType, List[EventHandler]] = {}
        self._listen_task: Optional[asyncio.Task] = None
        
        # Event for signaling shutdown
        self._stop_event = asyncio.Event()
    
    # =========================================================================
    # Connection Management
    # =========================================================================
    
    async def connect(self, ticker: str) -> None:
        """
        Connect to the QuantX WebSocket server for a specific ticker.
        
        Args:
            ticker: The ticker symbol to connect to (e.g., "QNTX")
            
        Raises:
            ConnectionError: If connection fails
            AuthenticationError: If API key authentication fails
        """
        ticker = ticker.upper()
        self._ticker = ticker
        url = f"{self.base_url}/ws/api/{ticker}"
        
        logger.info(f"Connecting to {url}")
        
        try:
            self._ws = await websockets.connect(url)
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {url}: {e}")
        
        # Authenticate immediately after connecting
        await self._authenticate()
        
        # Start listening for messages
        self._is_running = True
        self._stop_event.clear()
        self._listen_task = asyncio.create_task(self._listen())
        
        await self._emit(EventType.CONNECTED, {"ticker": ticker})
        logger.info(f"Connected and authenticated for {ticker}")
    
    async def disconnect(self) -> None:
        """Disconnect from the WebSocket server."""
        self._is_running = False
        self._stop_event.set()
        
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None
        
        if self._ws:
            await self._ws.close()
            self._ws = None
        
        self._is_authenticated = False
        self._auth_info = None
        
        await self._emit(EventType.DISCONNECTED, {"ticker": self._ticker})
        logger.info("Disconnected")
    
    async def run_forever(self) -> None:
        """
        Block until disconnect() is called or connection is lost.
        
        Useful for keeping the client running to receive events.
        """
        await self._stop_event.wait()
    
    @property
    def is_connected(self) -> bool:
        """Check if the client is connected and authenticated."""
        return self._ws is not None and self._is_authenticated
    
    @property
    def ticker(self) -> Optional[str]:
        """Get the currently connected ticker."""
        return self._ticker
    
    @property
    def account_id(self) -> Optional[int]:
        """Get the authenticated account ID."""
        return self._auth_info.account_id if self._auth_info else None
    
    # =========================================================================
    # Event Handling
    # =========================================================================
    
    def on(self, event_type: EventType, handler: EventHandler) -> None:
        """
        Register an event handler.
        
        Args:
            event_type: The type of event to listen for
            handler: Callback function (can be sync or async)
            
        Example:
            client.on(EventType.TRADE, lambda trade: print(trade))
            client.on(EventType.ORDER_FILLED, handle_fill)
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)
    
    def off(self, event_type: EventType, handler: EventHandler) -> None:
        """
        Remove an event handler.
        
        Args:
            event_type: The type of event
            handler: The handler to remove
        """
        if event_type in self._handlers:
            try:
                self._handlers[event_type].remove(handler)
            except ValueError:
                pass
    
    # =========================================================================
    # Subscriptions
    # =========================================================================
    
    async def subscribe_public(self, ticker: Optional[str] = None) -> None:
        """
        Subscribe to public event stream for a ticker.
        
        Args:
            ticker: Ticker to subscribe to (defaults to connected ticker)
        """
        self._ensure_connected()
        ticker = (ticker or self._ticker).upper()
        
        await self._send({
            "type": "subscribe_public",
            "ticker": ticker,
        })
        logger.info(f"Subscribed to public stream for {ticker}")
    
    async def subscribe_private(self) -> None:
        """
        Subscribe to private event stream for your account.
        
        This enables receiving order updates (accepted, rejected, filled, cancelled).
        """
        self._ensure_connected()
        
        await self._send({"type": "subscribe_private"})
        logger.info("Subscribed to private stream")
    
    async def unsubscribe_public(self, ticker: Optional[str] = None) -> None:
        """Unsubscribe from public event stream."""
        self._ensure_connected()
        ticker = (ticker or self._ticker).upper()
        
        await self._send({
            "type": "unsubscribe_public",
            "ticker": ticker,
        })
    
    async def unsubscribe_private(self) -> None:
        """Unsubscribe from private event stream."""
        self._ensure_connected()
        await self._send({"type": "unsubscribe_private"})
    
    # =========================================================================
    # Trading
    # =========================================================================
    
    async def send_order(
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
            
        Raises:
            NotConnectedError: If not connected
            
        Note:
            Order confirmation/rejection will be received via events.
            Register handlers for ORDER_ACCEPTED and ORDER_REJECTED.
        """
        self._ensure_connected()
        
        # Normalize side and order_type
        if isinstance(side, str):
            side = OrderSide(side)
        if isinstance(order_type, str):
            order_type = OrderType(order_type)
        
        await self._send({
            "type": "order",
            "order": {
                "ticker": self._ticker,
                "type": side.value,
                "quantity": quantity,
                "price": price,
                "orderType": order_type.value,
            }
        })
        
        logger.info(f"Order sent: {side.value} {quantity} {self._ticker} @ {price}")
    
    async def cancel_order(self, order_id: str) -> None:
        """
        Cancel an existing order.
        
        Args:
            order_id: The ID of the order to cancel
            
        Note:
            Cancellation confirmation will be received via ORDER_CANCELLED event.
        """
        self._ensure_connected()
        
        await self._send({
            "type": "cancel_order",
            "order_id": order_id,
        })
        
        logger.info(f"Cancel request sent for order {order_id}")
    
    # =========================================================================
    # Context Manager
    # =========================================================================
    
    async def __aenter__(self) -> "QuantXClient":
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()
    
    # =========================================================================
    # Private Methods
    # =========================================================================
    
    def _ensure_connected(self) -> None:
        """Raise if not connected."""
        if not self.is_connected:
            raise NotConnectedError()
    
    async def _send(self, data: dict) -> None:
        """Send a JSON message to the server."""
        if not self._ws:
            raise NotConnectedError()
        await self._ws.send(json.dumps(data))
    
    async def _authenticate(self) -> None:
        """Authenticate with the API key."""
        if not self._ws:
            raise ConnectionError("WebSocket not connected")
        
        # Send auth message
        await self._ws.send(json.dumps({
            "type": "auth",
            "api_key": self.api_key,
        }))
        
        # Wait for auth response
        try:
            response = await asyncio.wait_for(self._ws.recv(), timeout=10.0)
            data = json.loads(response)
        except asyncio.TimeoutError:
            raise AuthenticationError("Authentication timed out")
        except Exception as e:
            raise AuthenticationError(f"Failed to receive auth response: {e}")
        
        # Check response
        msg_type = data.get("type")
        
        if msg_type == "error":
            error = ErrorInfo.from_dict(data)
            raise AuthenticationError(error.error_message)
        
        if msg_type == "authenticated":
            self._auth_info = AuthInfo.from_dict(data.get("data", {}))
            self._is_authenticated = True
            logger.info(f"Authenticated with account {self._auth_info.account_id}")
            await self._emit(EventType.AUTHENTICATED, self._auth_info)
        else:
            raise AuthenticationError(f"Unexpected auth response: {data}")
    
    async def _listen(self) -> None:
        """Listen for incoming messages and dispatch to handlers."""
        try:
            async for message in self._ws:
                if not self._is_running:
                    break
                
                try:
                    data = json.loads(message)
                    await self._handle_message(data)
                except json.JSONDecodeError:
                    logger.warning(f"Received invalid JSON: {message[:100]}")
                except Exception as e:
                    logger.error(f"Error handling message: {e}")
        
        except websockets.ConnectionClosed:
            logger.info("Connection closed by server")
            self._is_running = False
            self._stop_event.set()
            await self._emit(EventType.DISCONNECTED, {"reason": "connection_closed"})
        
        except Exception as e:
            logger.error(f"Listen loop error: {e}")
            self._is_running = False
            self._stop_event.set()
    
    async def _handle_message(self, data: dict) -> None:
        """Route incoming message to appropriate handler."""
        msg_type = data.get("type", "")
        
        # Map server message types to events
        if msg_type == "market_data":
            market_data = MarketData.from_dict(data.get("data", data))
            await self._emit(EventType.MARKET_DATA, market_data)
        
        elif msg_type == "trade":
            trade = Trade.from_dict(data.get("data", data))
            await self._emit(EventType.TRADE, trade)
        
        elif msg_type == "order_accepted":
            order = Order.from_dict(data.get("data", data))
            await self._emit(EventType.ORDER_ACCEPTED, order)
        
        elif msg_type == "order_rejected":
            error = ErrorInfo.from_dict(data.get("data", data))
            await self._emit(EventType.ORDER_REJECTED, error)
        
        elif msg_type == "order_filled":
            order = Order.from_dict(data.get("data", data))
            await self._emit(EventType.ORDER_FILLED, order)
        
        elif msg_type == "order_cancelled":
            order = Order.from_dict(data.get("data", data))
            await self._emit(EventType.ORDER_CANCELLED, order)
        
        elif msg_type == "success":
            action = data.get("action", "")
            if action == "subscribed":
                await self._emit(EventType.SUBSCRIBED, data.get("data", {}))
        
        elif msg_type == "error":
            error = ErrorInfo.from_dict(data)
            await self._emit(EventType.ERROR, error)
            logger.warning(f"Server error: {error.error_type} - {error.error_message}")
        
        else:
            # Unknown message type - log but don't fail
            logger.debug(f"Unknown message type: {msg_type}")
    
    async def _emit(self, event_type: EventType, data: Any) -> None:
        """Emit an event to all registered handlers."""
        handlers = self._handlers.get(event_type, [])
        
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(data)
                else:
                    handler(data)
            except Exception as e:
                logger.error(f"Error in {event_type.value} handler: {e}")

