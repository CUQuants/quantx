"""
QuantX Trading API

This is the main entry point for the refactored trading system.
All services communicate via an asyncio.Queue-based event bus.

To run:
    uvicorn api.main_v2:app --reload --port 8000

Architecture:
    - BroadcastingService: WebSocket handler, auth, client management
    - RiskEngine: Order validation
    - MatchingEngine: Order book and matching
    - PersistenceService: Database operations
    - MarketDataBroadcaster: Market data distribution (order book snapshots)
    - EventStreamBroadcaster: Real-time event streaming (trades, orders, cancellations)
"""

import logging
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from api.db import init_models, SessionFactory
from api.routes.main_router import all_routes
from api.routes.v1.api_keys.routes import set_api_key_service
from api.security.firebase import init_firebase
from api.services import ServiceContainer
from api.services.api_key_service import ApiKeyService
from engine_server.auth.firebase_auth_service import FirebaseAuth

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

_service_container: Optional[ServiceContainer] = None
_api_key_service: Optional[ApiKeyService] = None


def get_service_container() -> ServiceContainer:
    if _service_container is None:
        raise RuntimeError("Service container not initialized")
    return _service_container


def get_api_key_service() -> ApiKeyService:
    if _api_key_service is None:
        raise RuntimeError("API key service not initialized")
    return _api_key_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context manager.

    Initializes database, Firebase, and all services on startup.
    Shuts down services gracefully on shutdown.
    """
    global _service_container, _api_key_service

    logger.info("Starting trading API...")

    await init_models()
    logger.info("Database initialized")

    init_firebase()
    logger.info("Firebase initialized")

    _service_container = ServiceContainer(
        session_factory=SessionFactory,
        auth_service=FirebaseAuth(),
        tickers=["QNTX", "NVDA"],  # Add more tickers as needed
    )

    await _service_container.start()
    logger.info("All services started")

    # Initialize API key service
    _api_key_service = ApiKeyService(session_factory=SessionFactory)
    set_api_key_service(_api_key_service)
    logger.info("API key service initialized")

    yield

    logger.info("Shutting down trading API...")
    await _service_container.stop()
    logger.info("All services stopped")


app = FastAPI(
    title="QuantX Trading API",
    description="Service-oriented trading platform with real-time WebSocket support",
    version="2.0.0",
    lifespan=lifespan,
    redirect_slashes=False,  # Prevent 307 redirects for trailing slash mismatches
)

app.include_router(all_routes, prefix="/api")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.websocket("/ws/{ticker}")
async def websocket_endpoint(websocket: WebSocket, ticker: str):
    """
    WebSocket endpoint for real-time trading.

    Clients connect to /ws/{ticker} to:
        - Receive market data updates for that ticker
        - Submit orders (with authentication)
        - Receive order confirmations and rejections
        - Subscribe to event streams (public/private)

    Message formats:
        Order submission:
            {
                "type": "order",
                "token": "<firebase_id_token>",
                "order": {
                    "ticker": "QNTX",
                    "type": "Buy" | "Sell",
                    "quantity": 100,
                    "price": 50.00,
                    "orderType": "LIMIT" | "MARKET"
                }
            }

        Subscribe to public event stream:
            {
                "type": "subscribe_public",
                "ticker": "QNTX"
            }

        Unsubscribe from public event stream:
            {
                "type": "unsubscribe_public",
                "ticker": "QNTX"
            }

        Subscribe to private event stream (requires auth):
            {
                "type": "subscribe_private",
                "token": "<firebase_id_token>"
            }

        Unsubscribe from private event stream:
            {
                "type": "unsubscribe_private"
            }
    """
    ticker = ticker.upper()
    container = get_service_container()

    if ticker not in container.tickers:
        await websocket.close(code=4000, reason="Invalid ticker")
        return

    await websocket.accept()

    # When a user subscribes to a ticker, it must be synced with the database
    # This call will lazily load orders from the database into the matching engine if needed
    await container.matching_engine.ensure_hydrated(ticker)

    client = await container.broadcasting_service.add_client(websocket, ticker)

    if not client:
        await websocket.close(code=4001, reason="Failed to register client")
        return

    await container.market_data_broadcaster.subscribe(client, ticker)

    await container.market_data_broadcaster.send_initial_snapshot(client, ticker)

    try:
        while True:
            data = await websocket.receive_json()
            await _handle_websocket_message(container, client, data)

    except WebSocketDisconnect:
        logger.info(f"Client {client.client_id} disconnected")

    except Exception as e:
        logger.error(f"WebSocket error for client {client.client_id}: {e}")

    finally:
        # Cleanup all subscriptions
        await container.market_data_broadcaster.unsubscribe_all(client)
        await container.event_stream_broadcaster.unsubscribe_all(client)
        await container.broadcasting_service.remove_client(client)


async def _handle_websocket_message(
    container: ServiceContainer,
    client,
    data: dict,
) -> None:
    """
    Route incoming WebSocket messages to the appropriate handler.

    Handles:
        - order, cancel_order: Routed to BroadcastingService
        - subscribe_public, unsubscribe_public: Routed to EventStreamBroadcaster
        - subscribe_private, unsubscribe_private: Routed to EventStreamBroadcaster
    """
    message_type = data.get("type")

    if not message_type:
        await client.send_error("MISSING_TYPE", "Message type is required")
        return

    # Order-related messages go to BroadcastingService
    if message_type in ("order", "cancel_order"):
        await container.broadcasting_service.handle_message(client, data)
        return

    # Event stream subscription messages
    if message_type == "subscribe_public":
        ticker = data.get("ticker", "").upper()
        if not ticker:
            await client.send_error("MISSING_TICKER", "Ticker is required for public subscription")
            return
        await container.event_stream_broadcaster.subscribe_public(client, ticker)
        return

    if message_type == "unsubscribe_public":
        ticker = data.get("ticker", "").upper()
        if not ticker:
            await client.send_error("MISSING_TICKER", "Ticker is required for unsubscription")
            return
        await container.event_stream_broadcaster.unsubscribe_public(client, ticker)
        return

    if message_type == "subscribe_private":
        token = data.get("token")
        if not token:
            await client.send_error("MISSING_TOKEN", "Token is required for private subscription")
            return
        await container.event_stream_broadcaster.subscribe_private(client, token)
        return

    if message_type == "unsubscribe_private":
        await container.event_stream_broadcaster.unsubscribe_private(client)
        return

    # Unknown message type
    await client.send_error(
        "INVALID_MESSAGE_TYPE",
        f"Unknown message type: {message_type}"
    )


# =============================================================================
# API WebSocket Endpoint (API Key Authentication)
# =============================================================================

@app.websocket("/ws/api/{ticker}")
async def api_websocket_endpoint(websocket: WebSocket, ticker: str):
    """
    WebSocket endpoint for programmatic API clients.

    Clients authenticate ONCE at connection time using an API key.
    After authentication, all messages use the cached user identity.

    Connection flow:
        1. Connect to /ws/api/{ticker}
        2. Server waits for auth message
        3. Client sends: {"type": "auth", "api_key": "qntx_live_..."}
        4. Server validates and responds with success/error
        5. On success, client can send orders without per-message auth

    Message formats after authentication:
        Order submission (no token needed):
            {
                "type": "order",
                "order": {
                    "ticker": "QNTX",
                    "type": "Buy" | "Sell",
                    "quantity": 100,
                    "price": 50.00,
                    "orderType": "LIMIT" | "MARKET"
                }
            }

        Cancel order:
            {
                "type": "cancel_order",
                "order_id": "<order_id>"
            }

        Subscribe to public event stream:
            {"type": "subscribe_public", "ticker": "QNTX"}

        Subscribe to private event stream (auto-authenticated):
            {"type": "subscribe_private"}
    """
    ticker = ticker.upper()
    container = get_service_container()
    api_key_service = get_api_key_service()

    if ticker not in container.tickers:
        await websocket.close(code=4000, reason="Invalid ticker")
        return

    await websocket.accept()

    # Wait for authentication message
    try:
        auth_data = await websocket.receive_json()
    except Exception as e:
        logger.error(f"Failed to receive auth message: {e}")
        await websocket.close(code=4001, reason="Failed to receive auth message")
        return

    # Validate auth message format
    if auth_data.get("type") != "auth" or not auth_data.get("api_key"):
        await websocket.send_json({
            "type": "error",
            "error_type": "AUTH_REQUIRED",
            "error_message": "First message must be: {\"type\": \"auth\", \"api_key\": \"...\"}"
        })
        await websocket.close(code=4002, reason="Authentication required")
        return

    # Validate the API key
    api_key_info = await api_key_service.validate_key(auth_data["api_key"])

    if not api_key_info:
        await websocket.send_json({
            "type": "error",
            "error_type": "AUTH_FAILED",
            "error_message": "Invalid or expired API key"
        })
        await websocket.close(code=4003, reason="Invalid API key")
        return

    # Authentication successful - set up the client
    await container.matching_engine.ensure_hydrated(ticker)

    client = await container.broadcasting_service.add_client(websocket, ticker)

    if not client:
        await websocket.close(code=4004, reason="Failed to register client")
        return

    # Mark client as authenticated with the API key info
    client.authenticated = True
    client.user_id = api_key_info.user_id
    client.account_id = api_key_info.account_id

    # Send auth success response
    await client.send_success("authenticated", {
        "account_id": api_key_info.account_id,
        "username": api_key_info.username,
    })

    logger.info(
        f"API client {client.client_id} authenticated for account {api_key_info.account_id}"
    )

    # Subscribe to market data
    await container.market_data_broadcaster.subscribe(client, ticker)
    await container.market_data_broadcaster.send_initial_snapshot(client, ticker)

    try:
        while True:
            data = await websocket.receive_json()
            await _handle_api_websocket_message(container, client, api_key_info, data)

    except WebSocketDisconnect:
        logger.info(f"API client {client.client_id} disconnected")

    except Exception as e:
        logger.error(f"WebSocket error for API client {client.client_id}: {e}")

    finally:
        # Cleanup all subscriptions
        await container.market_data_broadcaster.unsubscribe_all(client)
        await container.event_stream_broadcaster.unsubscribe_all(client)
        await container.broadcasting_service.remove_client(client)


async def _handle_api_websocket_message(
    container: ServiceContainer,
    client,
    api_key_info,
    data: dict,
) -> None:
    """
    Handle messages from API-authenticated clients.

    Unlike the regular endpoint, orders don't need per-message auth tokens
    since the client is already authenticated via API key.
    """
    message_type = data.get("type")

    if not message_type:
        await client.send_error("MISSING_TYPE", "Message type is required")
        return

    # Order submission - inject the authenticated user info
    if message_type == "order":
        # Create a synthetic auth result for the broadcasting service
        order_data = data.get("order", {})
        
        # Add the ticker to the order if not present
        if "ticker" not in order_data:
            order_data["ticker"] = client.subscribed_tickers.copy().pop() if client.subscribed_tickers else ""
        
        # Route to broadcasting service with pre-authenticated info
        await _handle_api_order(container, client, api_key_info, order_data)
        return

    # Cancel order - inject the authenticated user info
    if message_type == "cancel_order":
        order_id = data.get("order_id")
        if not order_id:
            await client.send_error("MISSING_ORDER_ID", "Order ID is required")
            return
        await _handle_api_cancel_order(container, client, api_key_info, order_id)
        return

    # Event stream subscriptions
    if message_type == "subscribe_public":
        ticker = data.get("ticker", "").upper()
        if not ticker:
            await client.send_error("MISSING_TICKER", "Ticker is required")
            return
        await container.event_stream_broadcaster.subscribe_public(client, ticker)
        return

    if message_type == "unsubscribe_public":
        ticker = data.get("ticker", "").upper()
        if not ticker:
            await client.send_error("MISSING_TICKER", "Ticker is required")
            return
        await container.event_stream_broadcaster.unsubscribe_public(client, ticker)
        return

    # Private subscription - already authenticated, no token needed
    if message_type == "subscribe_private":
        # Subscribe using the pre-authenticated account_id
        await _subscribe_private_api(container, client, api_key_info)
        return

    if message_type == "unsubscribe_private":
        await container.event_stream_broadcaster.unsubscribe_private(client)
        return

    # Unknown message type
    await client.send_error(
        "INVALID_MESSAGE_TYPE",
        f"Unknown message type: {message_type}"
    )


async def _handle_api_order(
    container: ServiceContainer,
    client,
    api_key_info,
    order_data: dict,
) -> None:
    """Handle order submission from an API-authenticated client."""
    from models import OrderSide, OrderType
    from api.services.events import EventType, RawOrderPayload

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

    if ticker not in container.tickers:
        await client.send_error("INVALID_TICKER", f"Invalid ticker: {ticker}")
        return

    payload = RawOrderPayload(
        ticker=ticker,
        side=side,
        order_type=order_type,
        quantity=quantity,
        price=price,
        user_id=api_key_info.user_id,
        email=f"api_user_{api_key_info.account_id}@quantx.api",  # Synthetic email for API users
        websocket_id=client.client_id,
    )

    logger.info(
        f"API order: {side.value} {quantity} {ticker} @ {price} "
        f"from account {api_key_info.account_id}"
    )

    await container.broadcasting_service.publish(EventType.RAW_ORDER, payload.to_dict())


async def _handle_api_cancel_order(
    container: ServiceContainer,
    client,
    api_key_info,
    order_id: str,
) -> None:
    """Handle order cancellation from an API-authenticated client."""
    from api.services.events import EventType, CancelOrderPayload

    payload = CancelOrderPayload(
        order_id=order_id,
        user_id=api_key_info.user_id,
        websocket_id=client.client_id,
    )

    logger.info(
        f"API cancel: order_id={order_id} from account {api_key_info.account_id}"
    )

    await container.broadcasting_service.publish(EventType.CANCEL_ORDER, payload.to_dict())


async def _subscribe_private_api(
    container: ServiceContainer,
    client,
    api_key_info,
) -> None:
    """Subscribe an API client to their private event stream."""
    # Directly add to private subscriptions using account_id
    async with container.event_stream_broadcaster._lock:
        account_id = api_key_info.account_id
        
        if account_id not in container.event_stream_broadcaster._private_subscriptions:
            container.event_stream_broadcaster._private_subscriptions[account_id] = set()
        container.event_stream_broadcaster._private_subscriptions[account_id].add(client)
        container.event_stream_broadcaster._client_to_account[client.client_id] = account_id

    logger.info(
        f"API client {client.client_id} subscribed to private stream for account {account_id}"
    )

    await client.send_success("subscribed", {"stream": "private", "account_id": account_id})


# =============================================================================
# Health Check Endpoint
# =============================================================================

@app.get("/health")
async def health_check():
    """
    Health check endpoint.

    Returns the status of all services.
    """
    container = get_service_container()
    return await container.health_check()


@app.get("/health/simple")
async def simple_health_check():
    """Simple health check for load balancers."""
    return {"status": "ok"}


# =============================================================================
# Market Data Endpoint (REST fallback)
# =============================================================================

@app.get("/api/v1/market-data/{ticker}")
async def get_market_data(ticker: str):
    """
    REST endpoint to get current market data snapshot.

    Useful for initial page load before WebSocket connects.
    """
    ticker = ticker.upper()
    container = get_service_container()

    if ticker not in container.tickers:
        return {"error": "Invalid ticker"}

    # Ensure order book is hydrated before returning market data
    await container.matching_engine.ensure_hydrated(ticker)

    snapshot = container.matching_engine.get_market_data_snapshot(ticker)

    if snapshot:
        return snapshot.to_dict()

    return {
        "ticker": ticker,
        "bids": [],
        "asks": [],
        "total_bid_quantity": 0,
        "total_ask_quantity": 0,
        "best_bid": None,
        "best_ask": None,
        "mid_price": None,
        "last_trade_price": None,
        "last_trade_quantity": None,
    }
