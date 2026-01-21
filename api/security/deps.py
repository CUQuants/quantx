import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Any, Callable

from fastapi import HTTPException, Header, Depends, Request
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import roles

from api.db import get_session
from models import Account, AccountRole

from firebase_admin import auth as fb_auth


@dataclass(frozen=True)
class AuthContext:
    uid: str
    email: str
    account: Account

    @property
    def account_id(self) -> int: return self.account.id

    @property
    def username(self) -> str: return self.account.username

    @property
    def role(self) -> AccountRole: return self.account.role


async def _get_account_by_uid(session: AsyncSession, uid: str) -> Optional[Account]:
    res = await session.execute(select(Account).where(Account.firebase_uid == uid))
    return res.scalar_one_or_none()


async def _get_account_by_username(session: AsyncSession, username: str) -> Optional[Account]:
    res = await session.execute(select(Account).where(Account.username == username))
    return res.scalar_one_or_none()


async def _unique_username(session: AsyncSession, base: str) -> str:
    candidate, n = base, 1
    while await _get_account_by_username(session, candidate) is not None:
        candidate = f"{base}-{n}"
        n += 1
    return candidate


async def _get_or_create_account(
    session: AsyncSession,
    *,
    firebase_uid: str,
    email: str,
    default_role: AccountRole = AccountRole.USER,
    display_name: Optional[str] = None,
) -> Account:
    acct = await _get_account_by_uid(session, firebase_uid)
    if acct:
        return acct

    base = (email.split("@", 1)[0] if email else None)
    if not base and display_name:
        base = "-".join(display_name.lower().split())
        if not base:
            base = None
    seed = base or f"user-{firebase_uid[:8]}"
    username = await _unique_username(session, seed)

    acct = Account(
        username=username,
        firebase_uid=firebase_uid,
        # CHANGE LATER TO USER, this is just for testing purposes
        role=AccountRole.ADMIN,
        created_at=datetime.now(timezone.utc),
        last_login_at=None,
    )
    session.add(acct)
    await session.flush()

    # Run blocking Firebase call in thread pool
    await asyncio.to_thread(
        fb_auth.set_custom_user_claims,
        firebase_uid,
        {'role': 'admin', 'db_id': acct.id}
    )

    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        username = await _unique_username(session, f"{seed}-{int(datetime.now().timestamp())}")
        acct.username = username
        session.add(acct)
        await session.commit()

    await session.refresh(acct)
    return acct


async def _touch_last_login(session: AsyncSession, account: Account) -> None:
    """
    Update last_login_at timestamp, but only if it's been more than 5 minutes.
    
    This avoids a DB commit on every single request while still tracking
    user activity at a reasonable granularity.
    """
    now = datetime.now(timezone.utc)
    
    # Only update if last login was more than 5 minutes ago (or never)
    should_update = account.last_login_at is None
    if not should_update and account.last_login_at is not None:
        # Handle both timezone-aware and naive datetimes from DB
        last_login = account.last_login_at
        if last_login.tzinfo is None:
            last_login = last_login.replace(tzinfo=timezone.utc)
        should_update = (now - last_login).total_seconds() > 300
    
    if should_update:
        account.last_login_at = now
        session.add(account)
        await session.commit()


def _extract_bearer(auth_header: Optional[str]) -> Optional[str]:
    if not auth_header:
        return None
    try:
        scheme, token = auth_header.split(" ", 1)
    except ValueError:
        return None
    return token if scheme.lower() == "bearer" and token else None


async def _verify_token_or_401(id_token: str) -> dict[str, Any]:
    """
    Verify Firebase ID token asynchronously.
    
    Uses asyncio.to_thread() to run the blocking Firebase SDK call
    in a thread pool, preventing it from blocking the async event loop.
    """
    try:
        # Run blocking Firebase call in thread pool
        return await asyncio.to_thread(fb_auth.verify_id_token, id_token)
    except fb_auth.ExpiredIdTokenError:
        raise HTTPException(
            status_code=401,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"}
        )
    except fb_auth.InvalidIdTokenError:
        raise HTTPException(
            status_code=401,
            detail="Invalid ID token",
            headers={"WWW-Authenticate": "Bearer"}
        )
    except Exception as e:
        raise HTTPException(
            status_code=401,
            detail=f"Authentication failed: {e}",
            headers={"WWW-Authenticate": "Bearer"}
        )


async def optional_auth_context(
    authorization: Optional[str] = Header(None, alias="Authorization"),
    session: AsyncSession = Depends(get_session),
    require_google_provider: bool = False,   # default off
) -> Optional[AuthContext]:
    token = _extract_bearer(authorization)
    if not token:
        return None

    decoded = await _verify_token_or_401(token)

    # Optional provider check for Google OAuth only
    if require_google_provider:
        provider = (decoded.get("firebase") or {}).get("sign_in_provider")
        if provider != "google.com":
            raise HTTPException(
                status_code=401, detail="Google sign-in required")

    uid = decoded["uid"]
    email = decoded.get("email") or ""
    display_name = decoded.get("name") or decoded.get(
        "displayName")  # Google usually sets "name"

    acct = await _get_or_create_account(
        session, firebase_uid=uid, email=email, display_name=display_name
    )
    await _touch_last_login(session, acct)

    return AuthContext(uid=uid, email=email, account=acct)


async def current_auth(
    authorization: Optional[str] = Header(None, alias="Authorization"),
    session: AsyncSession = Depends(get_session),
    # default ON since you're using Google OAuth
    require_google_provider: bool = True,
) -> AuthContext:
    token = _extract_bearer(authorization)
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Missing authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    decoded = await _verify_token_or_401(token)

    if require_google_provider:
        provider = (decoded.get("firebase") or {}).get("sign_in_provider")
        if provider != "google.com":
            raise HTTPException(
                status_code=401, detail="Google sign-in required")

    uid = decoded["uid"]
    email = decoded.get("email") or ""
    display_name = decoded.get("name") or decoded.get("displayName")

    acct = await _get_or_create_account(
        session, firebase_uid=uid, email=email, display_name=display_name
    )
    await _touch_last_login(session, acct)

    return AuthContext(uid=uid, email=email, account=acct)


def require_roles(*roles: AccountRole) -> Callable[[AuthContext], AuthContext]:

    def _dep(auth: AuthContext = Depends(current_auth)) -> AuthContext:
        if auth.role not in roles:
            raise HTTPException(
                status_code=403, detail="Insufficient permissions")
        return auth
    return _dep


admin = require_roles(AccountRole.ADMIN)
moderator = require_roles(AccountRole.MODERATOR)
user = require_roles(
    AccountRole.USER, AccountRole.MODERATOR, AccountRole.ADMIN)


def owner_or_role(param_name: str = "account_id", *roles: AccountRole):
    def _dep(request: Request, auth: AuthContext = Depends(current_auth)) -> AuthContext:
        raw = request.path_params.get(
            param_name) or request.query_params.get(param_name)
        try:
            target_id = int(raw)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400, detail=f"Invalid {param_name}")
        if auth.role not in roles and auth.account_id != target_id:
            raise HTTPException(
                status_code=403, detail="Not authorized for this resource")
        return auth
    return _dep


def owner_or_admin(param_name="account_id"):
    return owner_or_role(param_name, AccountRole.ADMIN)


def owner_or_mod(param_name="account_id"):
    return owner_or_role(param_name, AccountRole.ADMIN, AccountRole.MODERATOR)
