from fastapi import APIRouter

from api.routes.v1.router import all_routes as v1

all_routes = APIRouter()

all_routes.include_router(v1, prefix="/v1")