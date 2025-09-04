from fastapi import Request, HTTPException, Depends

from models import Account, AccountRole

import firebase_admin
from firebase_admin import auth
import os

cred = firebase_admin.credentials.Certificate(os.getenv("FIREBASE_ADMIN_CREDENTIALS"))
firebase_admin.initialize_app(cred)


async def current_user(request: Request) -> Account:
    token = request.headers.get("Authorization")
    if not token:
        raise HTTPException(status_code=401, detail="unauthorized")

    decoded_token = auth.verify_id_token(token)
    uid = decoded_token["uid"]

    user = await auth.get_user(uid)
    if not user:
        raise HTTPException(status_code=401, detail="unauthorized")

    return user

    # user = getattr(request.state, "user", None)

    # if not user:
    #     raise HTTPException(status_code=401, detail="unauthorized")

    # return user

def require_roles(*allowed: AccountRole):
    allowed_set = set(allowed)

    async def _dep(user: Account = Depends(current_user)) -> Account:
        if user.role not in allowed_set:
            raise HTTPException(status_code=403, detail="forbidden")
        return user

    return _dep

require_admin = require_roles(AccountRole.ADMIN)
require_mod = require_roles(AccountRole.ADMIN, AccountRole.MODERATOR)
require_user = require_roles(AccountRole.ADMIN, AccountRole.MODERATOR, AccountRole.USER)