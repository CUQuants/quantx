
from models import Order, Trade, Account, Position
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from models import OrderSide


class OrderValidationError(Exception):
    pass


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

    buyer_account.balance -= trade.price*trade.quantity
    seller_account.balance += trade.price*trade.quantity


async def update_positions(trade: Trade, session: AsyncSession):
    buyer_account: Account = trade.buy_order.account
    seller_account: Account = trade.sell_order.account

    seller_position = await get_position(seller_account.id, trade.symbol, session)
    buyer_position = await get_position(buyer_account.id, trade.symbol, session)

    if not buyer_position:
        # fill in pnl later
        new_position = Position(
            symbol=trade.symbol, quantity=trade.quantity, account_id=buyer_account.id, average_price=trade.price)
        await session.add(new_position)

    else:
        new_average = ((buyer_position.quantity * buyer_position.average_price) +
                       (trade.quantity * trade.price)) / (buyer_position.quantity + trade.quantity)
        buyer_position.quantity += trade.quantity
        buyer_position.average_price = new_average

    if seller_position:
        seller_position.quantity -= trade.quantity

        if seller_position.quantity <= 0.0:
            await session.execute(delete(Position).where(Position.id == seller_position.id))


async def get_position(account_id: int, symbol: str, session: AsyncSession, side: OrderSide) -> Position:
    pos_result = await session.execute(select(Position).where(Position.account_id == account_id).where(Position.symbol == symbol))
    position = pos_result.scalar_one_or_none()
    return position


async def get_account(session: AsyncSession, account_id: int) -> Account:
    account_result = await session.execute(select(Account).where(Account.id == account_id))
    account = account_result.scalar_one_or_none()
    return account


async def validate_order(session: AsyncSession, price: float, side: OrderSide, symbol: str, quantity: int, account_id) -> dict:
    """
    For the functionality of this engine, we will choose to validate the order in the following ways.

    The order will get rejected if:
    - The price or quantity is less than or equal to 0
    - For a BUY order, if a user doesn't have enough in their account to fulfill the order when matched
    - For a SELL order, if a user doesn't have a position, or the position size is less than the quantity of the order
    - If a user has an order that could possibly match with the incoming order
    """
    if quantity <= 0:
        raise OrderValidationError("Quantity must be positive")

    if price is None or price <= 0:
        raise OrderValidationError("Orders require positive price")

    if side == OrderSide.BUY:
        account = await get_account(session, account_id)
        if account.balance < price*quantity:
            raise OrderValidationError(
                "Not enough account balance to execute order!")

    else:
        position = await get_position(account_id, symbol, session)
        if position and position.quantity < quantity:
            raise OrderValidationError(
                "Position size is not enough to execute sell order")

    # Check to see if another order will match
