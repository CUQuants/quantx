"""
QuantX Trading API - Service-Oriented Architecture

This is the main entry point for the refactored trading system.
All services communicate via an asyncio.Queue-based event bus.

To run:
    uvicorn api.main_v2:app --reload --port 8000

Architecture:
    - BroadcastingService: WebSocket handler, auth, client management
    - RiskEngine: Order validation
    - MatchingEngine: Order book and matching
    - PersistenceService: Database operations
    - MarketDataBroadcaster: Market data distribution
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Global service container
_service_container: Optional[ServiceContainer] = None


def get_service_container() -> ServiceContainer:
    """Get the global service container."""
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
    
    logger.info("Starting QuantX Trading API...")
    
    # Initialize database
    await init_models()
    logger.info("Database initialized")
    
    # Initialize Firebase
    init_firebase()
    logger.info("Firebase initialized")
    
    # Create and start service container
    _service_container = ServiceContainer(
        session_factory=SessionFactory,
        auth_service=FirebaseAuth(),
        tickers=["QNTX"],  # Add more tickers as needed
    )
    
    await _service_container.start()
    logger.info("All services started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down QuantX Trading API...")
    await _service_container.stop()
    logger.info("All services stopped")


# Create FastAPI app
app = FastAPI(
    title="QuantX Trading API",
    description="Service-oriented trading platform with real-time WebSocket support",
    version="2.0.0",
    lifespan=lifespan,
)

# Include REST API routes
app.include_router(all_routes, prefix="/api")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# WebSocket Endpoint
# =============================================================================

@app.websocket("/ws/{ticker}")
async def websocket_endpoint(websocket: WebSocket, ticker: str):
    """
    WebSocket endpoint for real-time trading.
    
    Clients connect to /ws/{ticker} to:
        - Receive market data updates for that ticker
        - Submit orders (with authentication)
        - Receive order confirmations and rejections
    
    Message format for orders:
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
    """
    ticker = ticker.upper()
    container = get_service_container()
    
    # Validate ticker
    if ticker not in container.tickers:
        await websocket.close(code=4000, reason="Invalid ticker")
        return
    
    # Accept connection
    await websocket.accept()
    
    # Ensure order book is hydrated before client gets market data
    # This handles edge cases where hydration failed on startup
    # or if a new ticker was added dynamically
    await container.matching_engine.ensure_hydrated(ticker)
    
    # Register client with broadcasting service
    client = await container.broadcasting_service.add_client(websocket, ticker)
    
    if not client:
        await websocket.close(code=4001, reason="Failed to register client")
        return
    
    # Subscribe to market data
    await container.market_data_broadcaster.subscribe(client, ticker)
    
    # Send initial market data snapshot
    await container.market_data_broadcaster.send_initial_snapshot(client, ticker)
    
    try:
        # Main message loop
        while True:
            data = await websocket.receive_json()
            await container.broadcasting_service.handle_message(client, data)
            
    except WebSocketDisconnect:
        logger.info(f"Client {client.client_id} disconnected")
        
    except Exception as e:
        logger.error(f"WebSocket error for client {client.client_id}: {e}")
        
    finally:
        # Cleanup
        await container.market_data_broadcaster.unsubscribe_all(client)
        await container.broadcasting_service.remove_client(client)


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
