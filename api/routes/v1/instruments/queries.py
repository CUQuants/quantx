from typing import Callable

from sqlalchemy import Select, select

from models import Instrument


def get_instruments() -> Select:
    return select(Instrument)


def get_instrument_by_id(id: int) -> Select:
    return get_instruments().where(Instrument.id == id)

def get_instrument_by_symbol(symbol: str) -> Select:
    return get_instruments().where(Instrument.symbol == symbol)