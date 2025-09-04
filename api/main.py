from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from middleware.auth import AuthMiddleware

app = FastAPI(title="QuantX API", version="v1")

# Anonymous routes that don't require authentication
ANON_ROUTES = (
    "/v1/health",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/v1/auth/login",
    "/v1/auth/register"
)

# Add CORS middleware first
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"], 
    allow_headers=["*"], 
    allow_credentials=True,
)

# Add Firebase authentication middleware
app.add_middleware(AuthMiddleware, allowed_paths=ANON_ROUTES)