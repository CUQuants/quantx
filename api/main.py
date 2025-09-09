from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.db import init_models
from api.routes.main_router import all_routes
from api.security.firebase import init_firebase


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_models()
    init_firebase()

    yield

app = FastAPI(
    name="QuantX API",
    version="v1",
    lifespan=lifespan
)

app.include_router(all_routes, prefix="/api")
