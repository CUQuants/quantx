from typing import Any, Optional, Sequence

from sqlalchemy import ColumnElement
from sqlalchemy.sql import Select

def where_if(stmt: Select, cond: Any, criterion: ColumnElement[bool]) -> Select:
    """Apply .where(...) only if cond is not None / truthy."""
    if cond is None:
        return stmt
    if isinstance(cond, (str, Sequence)) and not cond:  # empty string/list
        return stmt
    return stmt.where(criterion)

def apply_time_symbol_filters(
    stmt: Select,
    *,
    ts_col: ColumnElement,           # e.g., Order.created_at
    after: Optional[Any] = None,
    before: Optional[Any] = None,
    symbol_col: Optional[ColumnElement] = None,
    symbol: Optional[str] = None,
) -> Select:
    stmt = where_if(stmt, after,  ts_col >= after)
    stmt = where_if(stmt, before, ts_col <  before)
    if symbol_col is not None:
        stmt = where_if(stmt, symbol, symbol_col == symbol)
    return stmt

def paginate(stmt: Select, page: int, page_size: int) -> Select:
    offset = page * page_size
    return stmt.offset(offset).limit(page_size)