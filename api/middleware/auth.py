from typing import Iterable
import firebase_admin
from firebase_admin import auth, credentials

from starlette.middleware.base import BaseHTTPMiddleware


def _path_allowed(path: str, allowed: Iterable[str]) -> bool:
    for p in allowed:
        if path == p or path.startswith(p.rstrip("/") + "/"):
            return True

    return False


class FirebaseAuthMiddleware:
    """
    Firebase Authentication Middleware
    Validates Firebase ID tokens and maps them to Account objects
    """

    def __init__(self, auth_routes=None, protected_routes=None, admin_routes=None):
        self.auth_routes = auth_routes if auth_routes else []
        self.protected_routes = protected_routes if protected_routes else []
        self.admin_routes = admin_routes if admin_routes else []

        if not firebase_admin._apps:
            try:
                cred = credentials.ApplicationDefault()
                firebase_admin.initialize_app(cred)
            except Exception as e:
                raise

    async def check_route(self, route, token=None):

        if route in self.protected_routes:
            if not token:
                return False
            elif not self._validate_token(token):
                return False
            else:
                return True

        elif route in self.admin_routes:
            r

        else:
            return True

    async def _validate_token(self, token=None):
        if not token:
            return False

        try:
            auth.verify_id_token(token)
            return True

        except auth.InvalidIdTokenError:
            return False
        except auth.ExpiredIdTokenError:
            return False
        except auth.RevokedIdTokenError:
            return False
        except Exception as e:
            return False

    async def get_user_by_token(self, token):
        if not token:
            return None

        try:

            decoded_token = await auth.verify_id_token(token)
            return {
                'uid': decoded_token.get('uid'),
                'email': decoded_token.get('email'),
                'email_verified': decoded_token.get('email_verified', False),
                'name': decoded_token.get('name'),
                'picture': decoded_token.get('picture'),
                'firebase_claims': decoded_token
            }
        except Exception as e:
            return None
