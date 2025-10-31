from engine_server.db_session import SessionFactory
from models import Order, Trade
from sqlalchemy.exc import SQLAlchemyError


async def add_db_order(order: Order):

    async with SessionFactory() as session:
        try:
            async with session.begin():
                session.add(order)
                await session.flush()
                await session.refresh(order)
            return order
        except SQLAlchemyError:
            await session.rollback()
            raise


async def add_trade(trade: Trade):
    async with SessionFactory() as session:
        try:
            async with session.begin():
                session.add(trade)
                await session.flush()
                await session.refresh(trade)
            return trade
        except SQLAlchemyError:
            await session.rollback()
            raise
