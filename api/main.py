from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
import firebase_admin
from firebase_admin import credentials, auth
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials


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


def initialize_firebase():
    if not firebase_admin._apps:

        cred = credentials.Certificate("serviceAccount.json")

        firebase_admin.initialize_app(cred)


initialize_firebase()

security = HTTPBearer()


async def verify_token():
    pass


@app.get("/")
async def read_route(credentials: HTTPAuthorizationCredentials = Depends(security)):

    try:
        id_token = credentials.credentials
        decoded_token = auth.verify_id_token(id_token)

        uid = decoded_token['uid']
        email = decoded_token.get('email', '')

        return {"email": email, "uid": uid}

    except auth.InvalidIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid ID token"
        )
    except auth.ExpiredIdTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Authentication failed: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
