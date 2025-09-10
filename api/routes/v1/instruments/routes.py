from fastapi import APIRouter, HTTPException
from fastapi.params import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from api.db import get_session
from api.routes.v1.instruments.dto import InstrumentsFilters, InstrumentsResult, InstrumentDTO, EquityDTO, FutureDTO, \
    OptionDTO, InstrumentUnion
from api.routes.v1.instruments.queries import get_instruments, get_instrument_by_symbol
from api.security.deps import admin
from api.util.pagination import paginate, where_if
from models import Instrument, InstrumentType

router = APIRouter(prefix="/instruments", tags=["instruments"])

def map_to_dto(instrument: Instrument) -> InstrumentUnion:
    if instrument.type == InstrumentType.EQUITY:
        return EquityDTO.model_validate(instrument)
    elif instrument.type == InstrumentType.FUTURE:
        return FutureDTO.model_validate(instrument)
    elif instrument.type == InstrumentType.OPTION:
        return OptionDTO.model_validate(instrument)

@router.get(
    "",
    response_model=InstrumentsResult,
    dependencies=[Depends(admin)]
)
async def search_instruments(
        session: AsyncSession = Depends(get_session),
        filters: InstrumentsFilters = Depends(),
):
    stmt = get_instruments()

    if filters.search:
        stmt = stmt.where(Instrument.symbol.ilike(f"{filters.search}%"))

    stmt = where_if(stmt, filters.active, Instrument.is_active == filters.active)

    stmt = where_if(stmt, filters.type, Instrument.type == filters.type)

    stmt = paginate(stmt, filters.page, filters.page_size)

    stmt = stmt.options(
        selectinload(Instrument.underlying_id)
    )

    resp = await session.execute(stmt)
    instruments = resp.scalars().all()

    return InstrumentsResult(
        page=filters.page,
        page_size=filters.page_size,
        instruments=[map_to_dto(i) for i in instruments],
    )

@router.get(
    "/{symbol}",
    response_model=InstrumentUnion,
)
async def get_instrument(symbol: str, session: AsyncSession = Depends(get_session)):
    stmt = get_instrument_by_symbol(symbol)
    resp = await session.execute(stmt)
    instrument = resp.scalar_one_or_none()

    if not instrument:
        raise HTTPException(404)

    return map_to_dto(instrument)


