from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from api.db import init_models, SessionFactory
from api.routes.main_router import all_routes
from api.security.firebase import init_firebase
from api.socket_service.event_bus import EventBus, EventType
from engine import MatchingEngine
from engine_server.auth.firebase_auth_service import FirebaseAuth
from api.socket_service.engine_broadcaster import OrderBroadcaster
from api.socket_service.adapters import ServerConnectionAdapter


"""
To run:
uvicorn api.main:app --reload --port 8000
"""


class UnifiedService:
    def __init__(self):
        self.event_bus = EventBus()
        self.MatchingEngine = MatchingEngine(self.event_bus)
        self.auth_service = FirebaseAuth()
        self.tickers = ["QNTX"]
        self.broadcasting_service = OrderBroadcaster(
            auth_service=self.auth_service, tickers=self.tickers, bus=self.event_bus)


unified_service = UnifiedService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_models()
    init_firebase()

    async def order_handler(payload: dict):
        order = payload["order"]
        async with SessionFactory() as session:
            async with session.begin():
                await unified_service.MatchingEngine.add_order(order, session)

    async def trade_handler(payload: dict):
        await unified_service.broadcasting_service.on_trade(payload)

    unified_service.event_bus.subscribe_event(EventType.ORDER, order_handler)
    unified_service.event_bus.subscribe_event(EventType.TRADE, trade_handler)
    yield

app = FastAPI(
    name="QuantX API",
    version="v1",
    lifespan=lifespan
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
    ticker = ticker.upper()

    print(ticker)
    if ticker not in unified_service.broadcasting_service.tickers:
        await websocket.close(code=4000, reason="Invalid ticker")
        return

    await websocket.accept()

    ws = ServerConnectionAdapter(websocket)

    try:
        await unified_service.broadcasting_service.add_subscription(ws, ticker)

    except Exception as e:
        print("Unable to add websocket connection", e)

    try:
        while True:
            data = await websocket.receive_json()
            await unified_service.broadcasting_service.on_message(data, ws)
    except WebSocketDisconnect:
        print("Websocket disconnected")
        await unified_service.broadcasting_service.remove_client(ws)
    except Exception as e:
        print(f"WebSocket error: {e}")
        await unified_service.broadcasting_service.remove_client(ws)
