from typing import List
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

# go up to manual_agent.api.db
from typing import List
from manual_agent.api.db import get_db, Base, engine
from manual_agent.api.util.pagination import Page
from .dto import OrderCreate, OrderOut
from . import queries

router = APIRouter(prefix="/manual", tags=["manual"])

@router.on_event("startup")
async def _migrate():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@router.post("/orders", response_model=OrderOut, status_code=status.HTTP_201_CREATED)
async def create_order(payload: OrderCreate, db: AsyncSession = Depends(get_db)):
    # simple guardrail
    if payload.quantity > 1e9:
        raise HTTPException(status_code=400, detail="Quantity too large.")
    order = await queries.create_order(
        db,
        account_id=payload.account_id,
        symbol=payload.symbol.upper(),
        side=payload.side,
        quantity=payload.quantity,
        limit_price=payload.limit_price,
    )
    await db.commit()
    return order

@router.get("/orders", response_model=List[OrderOut])
async def get_orders(page: Page = Depends(), db: AsyncSession = Depends(get_db)):
    return await queries.list_orders(db, limit=page.limit, offset=page.offset)

@router.post("/orders/{order_id}/cancel")
async def cancel(order_id: int, db: AsyncSession = Depends(get_db)):
    updated = await queries.cancel_order(db, order_id=order_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Order not found or not cancellable.")
    await db.commit()
    return {"ok": True, "order_id": order_id}
