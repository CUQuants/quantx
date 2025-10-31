
from models import Order, Trade, Account, Position
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete


async def add_db_order(order: Order, session: AsyncSession):
    session.add(order)
    await session.flush()
    await session.refresh(order)
    return order


async def handle_trade(trade: Trade, session: AsyncSession):
    session.add(trade)

    await update_account_balances(trade, session)
    await update_positions(trade, session)


async def update_account_balances(trade: Trade, session: AsyncSession):
    buyer_account: Account = trade.buy_order.account
    seller_account: Account = trade.sell_order.account

    buyer_account.balance -= trade.price
    seller_account.balance += trade.price


async def update_positions(trade: Trade, session: AsyncSession):
    buyer_account: Account = trade.buy_order.account
    seller_account: Account = trade.sell_order.account

    seller_pos_result = await session.execute(select(Position).where(Position.account_id == seller_account.id).where(Position.symbol == trade.symbol))
    seller_position = seller_pos_result.scalar_one_or_none()

    buyer_pos_result = await session.execute(select(Position).where(Position.account_id == buyer_account.id).where(Position.symbol == trade.symbol))
    buyer_position = seller_pos_result.scalar_one_or_none()

    if not buyer_position:
        new_position = Position()

    if seller_position:
        seller_position.quantity -= trade.quantity

        if seller_position.quantity <= 0.0:
            await session.execute(delete(Position).where(Position.id == seller_position.id))
