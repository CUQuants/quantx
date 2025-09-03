import pytest

import asyncio
import sqlalchemy as sa
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker

from models import Base, Future, Option, Equity, Instrument


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"

@pytest.fixture(scope="session")
def event_loop():
    """pytest-asyncio needs a session-scoped loop when using async engine fixtures."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="module")
async def engine(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("db") / "test.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False, future=True)

    async with engine.begin() as conn:
        # Enforce FKs on SQLite
        await conn.exec_driver_sql("PRAGMA foreign_keys=ON;")
        await conn.run_sync(Base.metadata.create_all)

    try:
        yield engine
    finally:
        await engine.dispose()

@pytest.fixture
async def session(engine):
    """Fresh DB state per test (function scope)."""
    Session = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with Session() as s:
        # Clean tables in dependency-safe order
        for tbl in (Future.__table__, Option.__table__, Equity.__table__, Instrument.__table__):
            await s.execute(sa.delete(tbl))
        await s.commit()
        yield s