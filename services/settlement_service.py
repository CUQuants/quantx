from datetime import datetime, timezone
from typing import Optional, List

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Instrument, Option, OptionType, Future, SettlementRun, Position, Payout, InstrumentType, Account, \
    SettlementStatus


async def _fetch_instrument(db: AsyncSession, symbol: str) -> Optional[Instrument]:
    res = await db.execute(select(Instrument).where(Instrument.symbol == symbol))
    return res.scalar_one_or_none()

def _option_payoff(settle: float, opt: Option) -> float:
    if opt.option_type == OptionType.CALL:
        return max(0.0, settle - opt.strike) * opt.multiplier
    else:
        return max(0.0, opt.strike - settle) * opt.multiplier

def _futures_pnl_per_contract(settle: float, avg_price: float, fut: Optional[Future]) -> float:
    mult = fut.multiplier if (fut and fut.multiplier) else 1.0
    return (settle - avg_price) * mult

async def perform_settlement(
        db: AsyncSession,
        symbol: str,
        settlement_price: float,
        effective_at: Optional[datetime] = None
) -> SettlementRun:
    effective_at = effective_at or datetime.now(timezone.utc)

    run = SettlementRun(
        symbol=symbol,
        effective_at=effective_at,
        settlement_price=settlement_price,
    )
    db.add(run)
    await db.flush()

    inst = await _fetch_instrument(db, symbol)

    pos_res = await db.execute(select(Position).where(Position.symbol == symbol))
    positions = list(pos_res.scalars())

    payouts: List[Payout] = []

    for pos in positions:
        # really shit way of doing this, but it's fine for now
        if pos.quantity == 0: continue

        qty = pos.quantity
        avg = pos.average_price or 0.0

        realized = 0.0

        if inst and inst.type == InstrumentType.OPTION:
            opt_res = await db.execute(select(Option).where(Option.id == inst.id))
            opt = opt_res.scalar_one_or_none()

            if opt is None:
                #safety: treat as zero payoff
                payoff = 0.0
            else:
                payoff = _option_payoff(settlement_price, opt)

            realized = (payoff - avg) * qty
        else:
            fut: Optional[Future] = None

            if inst and inst.type == InstrumentType.FUTURE:
                fr = await db.execute(select(Future).where(Future.id == inst.id))
                fut = fr.scalar_one_or_none()

            realized = _futures_pnl_per_contract(settlement_price, avg, fut) * qty

        acct = await db.get(Account, pos.account_id)
        if acct:
            acct.balance = (acct.balance or 0.0) + realized

        pos.realized_pnl = (pos.realized_pnl or 0.0) + realized
        pos.unrealized_pnl = 0.0
        pos.quantity = 0 # really shit way as mentioned before
        pos.average_price = 0.0

        payout = Payout(
            run_id=run.id,
            account_id=pos.account_id,
            symbol=symbol,
            quantity=qty,
            settlement_price=settlement_price,
            realized_pnl=realized,
        )
        db.add(payout)
        payouts.append(payout)

    run.status = SettlementStatus.COMPLETED
    await db.commit()
    await db.refresh(run)
    return run