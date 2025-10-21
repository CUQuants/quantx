from fastapi import APIRouter
from .v1.router import router_v1

main_router = APIRouter()
main_router.include_router(router_v1)
