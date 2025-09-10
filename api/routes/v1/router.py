from fastapi import APIRouter

from api.routes.v1.accounts.routes import router as accounts_router
from api.routes.v1.instruments.routes import router as instruments_router

all_routes = APIRouter()

all_routes.include_router(accounts_router)
all_routes.include_router(instruments_router)