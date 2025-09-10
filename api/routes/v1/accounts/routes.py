from typing import List

from fastapi import APIRouter, HTTPException
from fastapi.params import Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.db import get_session
from api.routes.v1.accounts.dto import AccountDTO, AccountUpdateBalanceRequest, \
    OrdersResult, TradesResult, OrdersFilters, TradesFilters, PositionsResult, PositionsFilters
from api.routes.v1.accounts.queries import get_account_by_id, get_orders_by_account_id, get_trades_by_account_id, \
    get_positions_by_account_id
from api.routes.v1.trades.dto import OrderDTO, TradeDTO, PositionDTO
from api.security.deps import current_auth, AuthContext, moderator, admin, owner_or_admin, owner_or_mod
from api.util.pagination import apply_time_symbol_filters, where_if, paginate
from models import Account, Order, OrderStatus, Trade, Position

router = APIRouter(prefix="/accounts", tags=["accounts"])

@router.get(
    "/me",
    response_model=AccountDTO,
)
async def get_me(auth: AuthContext = Depends(current_auth)):
    return AccountDTO.model_validate(auth.account)

@router.get(
    "/{account_id}",
    response_model=AccountDTO,
    dependencies=[Depends(moderator)],
)
async def get_account(account_id: int, session: AsyncSession = Depends(get_session)):
    resp = await session.execute(get_account_by_id(account_id))
    account = resp.scalar_one_or_none()

    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    return AccountDTO.model_validate(account)

@router.get(
    "/{account_id}/orders",
    response_model=OrdersResult,
    dependencies=[Depends(owner_or_mod)],
)
async def get_orders(
        account_id: int,
        session: AsyncSession = Depends(get_session),
        filters: OrdersFilters = Depends()
):
    stmt = get_orders_by_account_id(account_id)

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
    orders = resp.scalars().all()

    return OrdersResult(
        page=filters.page,
        page_size=filters.page_size,
        orders=[OrderDTO.model_validate(o) for o in orders],
    )

@router.get(
    "/{account_id}/trades",
    response_model=TradesResult,
)
async def get_trades(
        account_id: int,
        session: AsyncSession = Depends(get_session),
        filters: TradesFilters = Depends()
):
    stmt = get_trades_by_account_id(account_id)

    stmt = apply_time_symbol_filters(
        stmt,
        ts_col=Trade.created_at,
        after=filters.after,
        before=filters.before,
        symbol_col=Trade.symbol,
        symbol=filters.symbol,
    )

    stmt = where_if(stmt, filters.quantity, Trade.quantity >= filters.quantity)

    stmt = paginate(stmt, filters.page, filters.page_size)

    resp = await session.execute(stmt)
    trades = resp.scalars().all()

    return TradesResult(
        page=filters.page,
        page_size=filters.page_size,
        trades=[TradeDTO.model_validate(t) for t in trades],
    )

@router.get(
    "/{account_id}/positions",
    response_model=PositionsResult,
)
async def get_positions(
        account_id: int,
        session: AsyncSession = Depends(get_session),
        filters: PositionsFilters = Depends()
):
    stmt = get_positions_by_account_id(account_id)

    stmt = apply_time_symbol_filters(
        stmt,
        ts_col=Position.updated_at,
        after=filters.after,
        before=filters.before,
        symbol_col=Position.symbol,
        symbol=filters.symbol,
    )

    stmt = where_if(stmt, filters.quantity, Position.quantity >= filters.quantity)

    stmt = paginate(stmt, filters.page, filters.page_size)

    resp = await session.execute(stmt)
    positions = resp.scalars().all()

    return PositionsResult(
        page=filters.page,
        page_size=filters.page_size,
        positions=[PositionDTO.model_validate(p) for p in positions],
    )

@router.put(
    "/{account_id}/balance",
    response_model=AccountDTO,
    dependencies=[Depends(admin)],
)
async def update_account_balance(
        account_id: int,
        dto: AccountUpdateBalanceRequest,
        session: AsyncSession = Depends(get_session)
):
    resp = await session.execute(get_account_by_id(account_id))
    account = resp.scalar_one_or_none()

    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    account.balance = dto.balance

    await session.commit()
    await session.refresh(account)

    return AccountDTO.model_validate(account)