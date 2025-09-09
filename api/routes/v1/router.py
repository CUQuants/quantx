from fastapi import APIRouter

from api.routes.v1.accounts.routes import router as accounts_router

all_routes = APIRouter()

all_routes.include_router(accounts_router)