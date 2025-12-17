from fastapi import APIRouter

from api.routes.v1.accounts.routes import router as accounts_router
from api.routes.v1.trades.routes import router as trades_router
from api.routes.v1.orders.routes import router as orders_router

all_routes = APIRouter()

all_routes.include_router(accounts_router)
all_routes.include_router(trades_router)
all_routes.include_router(orders_router)
