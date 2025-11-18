from models import AccountRole


class InvalidRoleUpdate(Exception):
    msg: str


role_hierarchy = [AccountRole.OWNER, AccountRole.ADMIN,
                  AccountRole.MODERATOR, AccountRole.USER]


def validate_role_update(updater_role: AccountRole, updated_role: AccountRole):
    if role_hierarchy.index(updater_role) <= role_hierarchy.index(updated_role):
        raise InvalidRoleUpdate(
            "User does not have permissions to update user")
