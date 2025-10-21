from typing import Optional
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from manual_agent.models.order import ManualOrder, OrderStatus, Side  # manual_agent.models.order

async def create_order(
    db: AsyncSession, *, account_id: str, symbol: str, side: str, quantity: float, limit_price: Optional[float]
):
    order = ManualOrder(
        account_id=account_id,
        symbol=symbol,
        side=Side(side),
        quantity=quantity,
        limit_price=limit_price,
    )
    db.add(order)
    await db.flush()
    await db.refresh(order)
    return order

async def list_orders(db: AsyncSession, *, limit: int, offset: int):
    stmt = select(ManualOrder).order_by(ManualOrder.id.desc()).limit(limit).offset(offset)
    res = await db.execute(stmt)
    return list(res.scalars())

async def cancel_order(db: AsyncSession, *, order_id: int):
    stmt = (
        update(ManualOrder)
        .where(ManualOrder.id == order_id, ManualOrder.status == OrderStatus.NEW)
        .values(status=OrderStatus.CANCELLED)
        .execution_options(synchronize_session="fetch")
    )
    res = await db.execute(stmt)
    return res.rowcount  # 1 if updated
