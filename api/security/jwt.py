from datetime import datetime, timezone
from typing import Optional, Dict, Any

from jose import jwt, JWTError

from .config import SECRET_KEY, ALGORITHM, ACCESS_TOKEN_EXPIRE

def create_access_token(sub: str | int, extra: Optional[Dict[str, Any]] = None) -> str:
    now = datetime.now(timezone.utc)
    payload: Dict[str, Any] = {
        "sub": str(sub),
        "iat": int(now.timestamp())
    }

    if ACCESS_TOKEN_EXPIRE:
        payload["exp"] = int((now + ACCESS_TOKEN_EXPIRE).timestamp())
    if extra:
        payload.update(extra)

    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError as e:
        raise ValueError(f"Invalid token: {e}")