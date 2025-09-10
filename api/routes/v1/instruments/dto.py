from datetime import datetime
from typing import Optional, List, Literal, Annotated, Union

from pydantic import BaseModel, Field

from api.util.pagination import PaginatedFilters, PaginatedResult
from models import InstrumentType, OptionType

class InstrumentDTO(BaseModel):
    id: int
    symbol: str
    type: InstrumentType
    active: bool

class EquityDTO(InstrumentDTO):
    type: Literal[InstrumentType.EQUITY] = InstrumentType.EQUITY
    name: str
    spot_price: float
    dividend_yield: float
    sector: Optional[str]

class OptionDTO(InstrumentDTO):
    type: Literal[InstrumentType.OPTION] = InstrumentType.OPTION
    underlying: InstrumentDTO
    strike: float
    expiry: Optional[datetime]
    option_type: OptionType
    multiplier: float
    implied_volatility: Optional[float]
    open_interest: Optional[float]
    volume: int

class FutureDTO(InstrumentDTO):
    type: Literal[InstrumentType.FUTURE] = InstrumentType.FUTURE
    underlying: InstrumentDTO
    expiry: Optional[datetime]
    multiplier: float
    initial_margin_rate: float
    maintenance_margin_rate: float

InstrumentUnion = Annotated[
    Union[EquityDTO, OptionDTO, FutureDTO],
    Field(discriminator="type")
]

#Query DTO
class InstrumentsResult(PaginatedResult):
    instruments: List[InstrumentUnion]

class InstrumentsFilters(PaginatedFilters):
    active: Optional[bool] = None
    type: Optional[InstrumentType] = None
    search: Optional[str] = None
