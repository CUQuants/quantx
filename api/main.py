from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.middleware.cors import CORSMiddleware
import firebase_admin
from firebase_admin import credentials, auth
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from api.middleware.auth import FirebaseAuthMiddleware


app = FastAPI(title="QuantX API", version="v1")

ANON_ROUTES = (
    "/v1/health",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/v1/auth/login",
    "/v1/auth/register"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080", "*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)


app.add_middleware(FirebaseAuthMiddleware,
                   allowed_paths=[],
                   path_prefixes=[]
                   )


def initialize_firebase():
    if not firebase_admin._apps:

        cred = credentials.Certificate("serviceAccount.json")

        firebase_admin.initialize_app(cred)


initialize_firebase()

security = HTTPBearer()


@app.get("/")
async def read_route(request: Request):

    uid = request.state.firebase_uid
    email = request.state.firebase_email

    return {
        "uid": uid,
        "email": email
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
