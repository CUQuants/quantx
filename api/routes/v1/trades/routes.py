from typing import List

from fastapi import APIRouter, HTTPException
from fastapi.params import Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.db import get_session
from api.routes.v1.accounts.dto import AccountDTO, AccountUpdateBalanceRequest, \
    OrdersResult, TradesResult, OrdersFilters, TradesFilters, PositionsResult, PositionsFilters
from api.routes.v1.accounts.queries import get_account_by_id, get_orders_by_account_id, get_trades_by_account_id, \
    get_positions_by_account_id
from api.routes.v1.trades.queries import get_trades, get_trade_by_id
from api.routes.v1.trades.dto import OrderDTO, TradeDTO, PositionDTO
from api.security.deps import current_auth, AuthContext, moderator, admin, owner_or_admin, owner_or_mod
from api.util.pagination import apply_time_symbol_filters, where_if, paginate
from models import Account, Order, OrderStatus, Trade, Position, AccountRole
from typing import Optional
from datetime import datetime


router = APIRouter(prefix="/trades", tags=["trades"])


@router.get(
    "/{trade_id}",
    response_model=TradeDTO,
    dependencies=[Depends(current_auth)],
)
async def get_trade(trade_id: int, session: AsyncSession = Depends(get_session), user: AuthContext = Depends(current_auth)):

    stmt = get_trade_by_id(trade_id)

    resp = await session.execute(stmt)
    trade = resp.scalar_one_or_none()

    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")

    authorized_roles = [AccountRole.ADMIN, AccountRole.MODERATOR]

    if user.role not in authorized_roles and user.account_id != trade.account_id:
        raise HTTPException(status_code=401, detail="Unauthorized access!")

    return TradeDTO.model_validate(trade)


@router.get(
    "/",
    response_model=List[TradeDTO],
    dependencies=[Depends(owner_or_admin)],
)
async def query_trades(session: AsyncSession = Depends(get_session), filters: TradesFilters = Depends()):

    stmt = get_trades()

    stmt = apply_time_symbol_filters(
        stmt,
        ts_col=Order.created_at,
        after=filters.after,
        before=filters.before,
        symbol_col=Order.symbol,
        symbol=filters.symbol,
    )

    stmt = where_if(stmt, filters.status, Order.status == filters.status)
    stmt = paginate(stmt, filters.page, filters.page_size)

    resp = await session.execute(stmt)

    trades = resp.scalars().all()

    return [TradeDTO.model_validate(t) for t in trades]
