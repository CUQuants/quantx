from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="QuantX API", version="v1")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True,
)

ANON_ROUTES = (
    "/v1/health",
    "/docs",
    "/redoc",
    "/v1/accounts",
    "/v1/auth/login"
)