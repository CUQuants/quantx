from typing import Iterable, Optional, Dict, Any
import firebase_admin
from firebase_admin import auth, credentials
from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import json


class FirebaseAuthMiddleware(BaseHTTPMiddleware):
    def __init__(
        self,
        app,
        allowed_paths: Iterable[str] = None,
        path_prefixes: Iterable[str] = None
    ):
        super().__init__(app)
        self.allowed_paths = set(allowed_paths or [])
        self.path_prefixes = list(path_prefixes or [])

    def _path_allowed(self, path: str) -> bool:
        if path in self.allowed_paths:
            return True

        for prefix in self.path_prefixes:
            if path == prefix or path.startswith(prefix.rstrip("/") + "/"):
                return True

        return False

    def _extract_token(self, request: Request) -> Optional[str]:
        authorization = request.headers.get("Authorization")
        if not authorization:
            return None

        try:
            scheme, token = authorization.split(" ", 1)
            if scheme.lower() != "bearer":
                return None
            return token
        except ValueError:
            return None

    def _decode_token(self, id_token: str) -> Dict[str, Any]:
        try:
            decoded_token = auth.verify_id_token(id_token)

            uid = decoded_token['uid']
            email = decoded_token.get('email', '')
            email_verified = decoded_token.get('email_verified', False)

            return {
                "success": True,
                "uid": uid,
                "email": email,
                "email_verified": email_verified,
                "decoded_token": decoded_token
            }

        except auth.InvalidIdTokenError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid ID token",
                headers={"WWW-Authenticate": "Bearer"}
            )
        except auth.ExpiredIdTokenError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token has expired",
                headers={"WWW-Authenticate": "Bearer"}
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Authentication failed: {str(e)}",
                headers={"WWW-Authenticate": "Bearer"}
            )

    async def dispatch(self, request: Request, call_next) -> Response:
        try:
            if self._path_allowed(request.url.path):
                return await call_next(request)

            token = self._extract_token(request)
            if not token:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Missing authentication token",
                    headers={"WWW-Authenticate": "Bearer"}
                )

            user_info = self._decode_token(token)

            request.state.user = user_info
            request.state.firebase_uid = user_info["uid"]
            request.state.firebase_email = user_info["email"]

            response = await call_next(request)
            return response

        except HTTPException as e:
            return Response(
                content=json.dumps({"detail": e.detail}),
                status_code=e.status_code,
                media_type="application/json",
                headers=e.headers
            )
        except Exception as e:
            print(e)
            return Response(
                content=json.dumps({"detail": "Internal server error"}),
                status_code=500,
                media_type="application/json"
            )
