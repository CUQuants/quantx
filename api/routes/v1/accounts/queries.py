from typing import Callable

from sqlalchemy import select, Select, or_
from sqlalchemy.orm import selectinload

from fastapi.exceptions import HTTPException

from models import Account, Trade, Order, Position
from models import AccountRole


get_account_by_firebase_id: Callable[[str], Select] = \
    lambda firebase_id: select(Account).where(
        Account.firebase_uid == firebase_id)

get_account_by_id: Callable[[int], Select] = \
    lambda account_id: select(Account).where(Account.id == account_id)

get_full_account_by_id: Callable[[int], Select] = \
    lambda account_id: (
        select(Account)
        .where(Account.id == account_id)
        .options(
            selectinload(Account.orders),
            selectinload(Account.buy_trades),
            selectinload(Account.sell_trades),
            selectinload(Account.positions),
        )
)

get_orders_by_account_id: Callable[[int], Select] = \
    lambda account_id: select(Order).where(Order.account_id == account_id)

get_trades_by_account_id: Callable[[int], Select] = \
    lambda account_id: select(Trade).where(
        or_(Trade.buy_account_id == account_id, Trade.sell_account_id == account_id))

get_positions_by_account_id: Callable[[int], Select] = \
    lambda account_id: select(Position).where(
        Position.account_id == account_id)

get_all_accounts: Callable[[], Select] = select(Account)


def role_dto(role: str):
    role = role.upper()

    if role == "ADMIN":
        return AccountRole.ADMIN
    elif role == "AGENT":
        return AccountRole.AGENT
    elif role == "MODERATOR":
        return AccountRole.MODERATOR
    elif role == "USER":
        return AccountRole.USER
    elif role == "OWNER":
        return AccountRole.OWNER
    else:
        raise HTTPException(status_code=400, detail=f"Role {role} is invalid")
