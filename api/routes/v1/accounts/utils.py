from models import AccountRole
from fastapi.exceptions import HTTPException


class InvalidRoleUpdate(Exception):
    msg: str


role_hierarchy = [AccountRole.OWNER, AccountRole.ADMIN,
                  AccountRole.MODERATOR, AccountRole.USER]


def validate_role_update(updater_role: AccountRole, updated_role: AccountRole):
    if role_hierarchy.index(updater_role) >= role_hierarchy.index(updated_role):
        raise HTTPException(
            status_code=401, detail="Insufficient permissions to update role")
