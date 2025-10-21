from fastapi import APIRouter
from .manual.routes import router as manual_router

router_v1 = APIRouter(prefix="/v1")
router_v1.include_router(manual_router)
