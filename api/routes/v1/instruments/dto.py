from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel

from api.util.pagination import PaginatedFilters, PaginatedResult
from models import InstrumentType, OptionType

class InstrumentDTO(BaseModel):
    id: int
    symbol: str
    type: InstrumentType
    active: bool

class EquityDTO(InstrumentDTO):
    name: str
    spot_price: float
    dividend_yield: float
    sector: Optional[str]

class OptionDTO(InstrumentDTO):
    underlying: InstrumentDTO
    strike: float
    expiry: Optional[datetime]
    type: OptionType
    multiplier: float
    implied_volatility: Optional[float]
    open_interest: Optional[float]
    volume: int

class FutureDTO(InstrumentDTO):
    underlying: InstrumentDTO
    expiry: Optional[datetime]
    multiplier: float
    initial_margin_rate: float
    maintenance_margin_rate: float

#Query DTO
class InstrumentsResult(PaginatedResult):
    instruments: List[InstrumentDTO]

class InstrumentsFilters(PaginatedFilters):
    active: Optional[bool] = None
    type: Optional[InstrumentType] = None
    search: Optional[str] = None
