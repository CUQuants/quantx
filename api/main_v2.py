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
from api.security.firebase import init_firebase
from api.services import ServiceContainer
from engine_server.auth.firebase_auth_service import FirebaseAuth

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

_service_container: Optional[ServiceContainer] = None


def get_service_container() -> ServiceContainer:
    if _service_container is None:
        raise RuntimeError("Service container not initialized")
    return _service_container


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI lifespan context manager.

    Initializes database, Firebase, and all services on startup.
    Shuts down services gracefully on shutdown.
    """
    global _service_container

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

    yield

    logger.info("Shutting down trading API...")
    await _service_container.stop()
    logger.info("All services stopped")


app = FastAPI(
    title="QuantX Trading API",
    description="Service-oriented trading platform with real-time WebSocket support",
    version="2.0.0",
    lifespan=lifespan,
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
