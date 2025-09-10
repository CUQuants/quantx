from fastapi import APIRouter
from fastapi.params import Depends

from api.routes.v1.instruments.dto import InstrumentsFilters
from api.routes.v1.instruments.queries import get_instruments
from api.security.deps import admin
from api.util.pagination import paginate

router = APIRouter(prefix="/instruments", tags=["instruments"])

@router.get(
    "",
    dependencies=[Depends(admin)]
)
async def search(
      filters: InstrumentsFilters = Depends(),
):
    stmt = get_instruments()

    stmt = stmt.where()

    stmt = paginate(stmt, filters.page, filters.page_size)

