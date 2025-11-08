from typing import Optional, Callable

from sqlalchemy import select, Select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from models import Account, Trade, Order, Position


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
            selectinload(Account.trades),
            selectinload(Account.positions),
        )
)

get_orders_by_account_id: Callable[[int], Select] = \
    lambda account_id: select(Order).where(Account.id == account_id)

get_trades_by_account_id: Callable[[int], Select] = \
    lambda account_id: select(Trade).where(Trade.account_id == account_id)

get_positions_by_account_id: Callable[[int], Select] = \
    lambda account_id: select(Position).where(Account.id == account_id)
