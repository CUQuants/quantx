from fastapi import APIRouter, HTTPException
from fastapi.params import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api.db import get_session
from api.routes.v1.accounts.dto import AccountDTO, AccountDetailsDTO
from api.routes.v1.accounts.queries import get_account_by_id
from api.routes.v1.trades.dto import OrderDTO, TradeDTO, PositionDTO
from api.security.deps import current_auth, AuthContext, moderator

router = APIRouter(prefix="/accounts", tags=["accounts"])

@router.get(
    "/me",
    response_model=AccountDTO,
)
async def get_me(auth: AuthContext = Depends(current_auth)):
    return AccountDTO.from_orm(auth.account)

@router.get(
    "/me/details",
    response_model=AccountDetailsDTO,
)
async def get_me_details(auth: AuthContext = Depends(current_auth)):
    return AccountDetailsDTO(
        account = AccountDTO.model_validate(auth.account),
        orders = [OrderDTO.model_validate(o) for o in auth.account.orders],
        trades_affecting = [TradeDTO.model_validate(t) for t in auth.account.trades],
        positions = [PositionDTO.model_validate(p) for p in auth.account.positions],
    )

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
    "/{account_id}/details",
    response_model=AccountDetailsDTO,
    dependencies=[Depends(moderator)],
)
async def get_account_details(account_id: int, session: AsyncSession = Depends(get_session)):
    resp = await session.execute(get_account_by_id(account_id))
    account = resp.scalar_one_or_none()

    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    return AccountDetailsDTO(
        account = AccountDTO.model_validate(account),
        orders=[OrderDTO.model_validate(o) for o in account.orders],
        trades_affecting=[TradeDTO.model_validate(t) for t in account.trades],
        positions=[PositionDTO.model_validate(p) for p in account.positions],
    )

