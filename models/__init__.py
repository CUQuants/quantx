from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import MetaData

convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=convention)

from .instruments import Instrument, InstrumentType, Equity, Option, OptionType, Future
from .trading import Order, OrderStatus, OrderType, OrderSide, Position, Trade
from .account import AccountRole, Account
from .settlement import SettlementStatus, SettlementRun, AttendanceEvent, Payout
from .api_key import ApiKey

__all__ = [
    "Account",
    "AccountRole",
    "ApiKey",
    "AttendanceEvent",
    "Base",
    "Equity",
    "Future",
    "Instrument",
    "InstrumentType",
    "Option",
    "OptionType",
    "Order",
    "OrderStatus",
    "OrderType",
    "OrderSide",
    "Position",
    "Payout",
    "SettlementStatus",
    "SettlementRun",
    "Trade"
]