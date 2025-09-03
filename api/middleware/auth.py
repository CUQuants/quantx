from typing import Iterable

from starlette.middleware.base import BaseHTTPMiddleware

def _path_allowed(path: str, allowed: Iterable[str]) -> bool:
    for p in allowed:
        if path == p or path.startswith(p.rstrip("/") + "/"):
            return True

    return False

class AuthMiddleware(BaseHTTPMiddleware):
    """
    Leaving this for you to figure out, Alex
    """
    def __init__(self):
        pass