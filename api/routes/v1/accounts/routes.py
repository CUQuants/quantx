from typing import List

from fastapi import APIRouter, HTTPException, Response, status
from fastapi.params import Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from api.db import get_session
from api.routes.v1.accounts.dto import AccountDTO, AccountUpdateBalanceRequest, AccountAdjustBalanceRequest, CreateAccountRequest, \
    OrdersResult, TradesResult, OrdersFilters, TradesFilters, PositionsResult, PositionsFilters, AccountFilters, AccountUpdateRoleRequest
from api.routes.v1.accounts.queries import get_account_by_id, get_orders_by_account_id, get_trades_by_account_id, \
    get_positions_by_account_id, get_all_accounts, role_dto
from api.routes.v1.trades.dto import OrderDTO, TradeDTO, PositionDTO
from api.security.deps import current_auth, AuthContext, moderator, admin, owner_or_admin, owner_or_mod
from api.util.pagination import apply_time_symbol_filters, where_if, paginate
from models import Account, Order, Trade, Position, AccountRole, OrderStatus
from api.routes.v1.accounts.utils import validate_role_update

router = APIRouter(prefix="/accounts", tags=["accounts"])


@router.get(
    "/me",
    response_model=AccountDTO,
)
async def get_me(auth: AuthContext = Depends(current_auth)):
    print(auth)
    return AccountDTO.model_validate(auth.account)


@router.get("", response_model=List[AccountDTO])
async def get_accounts(dependencies=[Depends(current_auth)], session: AsyncSession = Depends(get_session), filters: AccountFilters = Depends()):

    query = get_all_accounts

    if filters.role:
        try:
            role = role_dto(filters.role)

        except Exception as e:
            raise HTTPException(
                status_code=400, detail="An error occurred getting the account role")

        query = query.where(Account.role == role)

    resp = await session.execute(query)

    accounts = resp.scalars().all()
    response_model = [AccountDTO.model_validate(
        account) for account in accounts]

    return response_model


@router.get(
    "/{account_id}",
    response_model=AccountDTO,
    dependencies=[Depends(current_auth)],
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

    # Convert string status to enum (case-insensitive)
    status_enum = None
    if filters.status:
        try:
            status_enum = OrderStatus(filters.status.lower())
        except ValueError:
            pass  # Invalid status, ignore filter
    
    stmt = where_if(stmt, status_enum, Order.status == status_enum)

    # Get total count before pagination
    count_stmt = select(func.count()).select_from(stmt.subquery())
    count_result = await session.execute(count_stmt)
    total_count = count_result.scalar() or 0

    stmt = paginate(stmt, filters.page, filters.page_size)

    resp = await session.execute(stmt)
    orders = resp.scalars().all()

    return OrdersResult(
        page=filters.page,
        page_size=filters.page_size,
        total_count=total_count,
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

    if filters.quantity:
        stmt = where_if(stmt, filters.quantity,
                        Trade.quantity >= filters.quantity)

    # Get total count before pagination
    count_stmt = select(func.count()).select_from(stmt.subquery())
    count_result = await session.execute(count_stmt)
    total_count = count_result.scalar() or 0

    if filters.page_size:
        stmt = paginate(stmt, filters.page, filters.page_size)

    resp = await session.execute(stmt)
    trades = resp.scalars().all()

    return TradesResult(
        page=filters.page,
        page_size=filters.page_size,
        total_count=total_count,
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

    if filters.quantity:
        stmt = where_if(stmt, filters.quantity,
                        Position.quantity >= filters.quantity)

    # Get total count before pagination
    count_stmt = select(func.count()).select_from(stmt.subquery())
    count_result = await session.execute(count_stmt)
    total_count = count_result.scalar() or 0

    if filters.page_size:
        stmt = paginate(stmt, int(filters.page), int(filters.page_size))

    resp = await session.execute(stmt)
    positions = resp.scalars().all()

    return PositionsResult(
        page=int(filters.page),
        page_size=int(filters.page_size),
        total_count=total_count,
        positions=[PositionDTO.model_validate(p) for p in positions],
    )


@router.patch("/{account_id}/role", response_model=AccountDTO, dependencies=[Depends(admin)])
async def update_account_role(account_id: int, dto: AccountUpdateRoleRequest, session: AsyncSession = Depends(get_session), auth: AuthContext = Depends(current_auth)):
    resp = await session.execute(get_account_by_id(account_id))
    account: Account = resp.scalar_one_or_none()

    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    updater_role = auth.role
    print(updater_role)

    dto_role = role_dto(dto.role)

    # Will check the role hierarchy to see if a user of a specific role can update another user's role, else throw an exception
    validate_role_update(updater_role, dto_role)

    account.role = dto_role

    await session.commit()
    await session.refresh(account)

    return AccountDTO.model_validate(account)


@router.patch(
    "/{account_id}/balance",
    response_model=AccountDTO,
    dependencies=[Depends(admin)],
)
async def adjust_account_balance(
        account_id: int,
        dto: AccountAdjustBalanceRequest,
        session: AsyncSession = Depends(get_session),
):
    """
    Adjust account balance by an amount.
    Positive amount adds credits, negative amount removes credits.
    Both balance and available_cash are updated together.
    """
    resp = await session.execute(get_account_by_id(account_id))
    account: Account = resp.scalar_one_or_none()

    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    # Update both balance and available_cash by the same amount
    account.balance += dto.amount
    account.available_cash += dto.amount

    await session.commit()
    await session.refresh(account)

    return AccountDTO.model_validate(account)
