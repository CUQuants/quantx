from typing import Optional, Callable

from sqlalchemy import select, Select, insert, Insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload, Mapped, mapped_column, DeclarativeBase
import uuid
from datetime import datetime
from sqlalchemy.sql.schema import ForeignKey
from api.routes.v1.trades.dto import TradeRequest


from models import Account, Trade, Order, Position


get_trade_by_id: Callable[[int], Select] = \
    lambda trade_id: select(Trade).where(Trade.id == trade_id)


def get_trades():
    return select(Trade)
