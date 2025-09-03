from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from models import Account, AccountRole

@pytest.mark.anyio
async def test_create_account_minimal(session: AsyncSession):
    now = datetime.now(timezone.utc)

    a = Account(
        username="alice",
        role=AccountRole.USER,
        # balance uses default
        # created_at uses default
        # last_login_at stays None
    )
    session.add(a)
    await session.commit()

    got = await session.get(Account, a.id)
    assert got is not None
    assert got.username == "alice"
    assert got.role == AccountRole.USER
    assert got.balance == pytest.approx(1000.0)

    # created_at defaulted; allow a few seconds of slack
    assert got.created_at is not None
    assert (got.created_at.replace(tzinfo=timezone.utc) - now) < timedelta(seconds=5)

    # last_login_at default is None
    assert got.last_login_at is None


@pytest.mark.anyio
async def test_username_uniqueness(session: AsyncSession):
    a1 = Account(username="dupe", role=AccountRole.USER)
    a2 = Account(username="dupe", role=AccountRole.ADMIN)
    session.add_all([a1, a2])

    with pytest.raises(IntegrityError):
        await session.commit()


@pytest.mark.anyio
async def test_update_last_login(session: AsyncSession):
    a = Account(username="charlie", role=AccountRole.USER)
    session.add(a)
    await session.commit()

    when = datetime.now(timezone.utc)
    a.last_login_at = when
    await session.commit()

    fresh = await session.get(Account, a.id)
    assert fresh.last_login_at is not None
    # compare naive values (SQLite may drop tzinfo on roundtrip)
    assert fresh.last_login_at.replace(tzinfo=None) == when.replace(tzinfo=None)


@pytest.mark.anyio
async def test_index_and_lookup_by_username(session: AsyncSession):
    session.add_all([
        Account(username="dana", role=AccountRole.USER),
        Account(username="eric", role=AccountRole.ADMIN),
    ])
    await session.commit()

    res = await session.execute(select(Account).where(Account.username == "eric"))
    eric = res.scalar_one()
    assert eric.role == AccountRole.ADMIN