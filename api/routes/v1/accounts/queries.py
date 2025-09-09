from typing import Optional, Callable

from sqlalchemy import select, Select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Account

get_account_by_id: Callable[[int], Select] = \
    lambda account_id: select(Account).where(Account.id == account_id)