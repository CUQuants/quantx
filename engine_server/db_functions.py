
from models import Order, Trade, Account, Position
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from models import OrderSide


class OrderValidationError(Exception):
    pass


async def add_db_order(order: Order, email: str, firebase_uid: str, session: AsyncSession):
    account = await _get_account_firebase_uid(session, firebase_uid, email)

    await validate_order(session, order, account)
    order.account_id = account.id
    session.add(order)
    await session.flush()
    await session.refresh(order)
    return order


async def handle_trade(trade: Trade, session: AsyncSession) -> None:
    session.add(trade)

    buyer_account: Account = await session.get(Account, trade.buy_account_id)
    seller_account: Account = await session.get(Account, trade.sell_account_id)
    await update_account_balances(trade, session, buyer_account, seller_account)
    await update_positions(trade, session, buyer_account, seller_account)

    await session.flush()


async def update_account_balances(trade: Trade, session: AsyncSession, buyer_account: Account, seller_account: Account):

    buyer_account.balance -= trade.price*trade.quantity
    seller_account.balance += trade.price*trade.quantity


async def update_positions(trade: Trade, session: AsyncSession, buyer_account: Account, seller_account: Account):

    seller_position = await get_position(seller_account.id, trade.symbol, session)
    buyer_position = await get_position(buyer_account.id, trade.symbol, session)

    if not buyer_position:
        # fill in pnl later
        new_position = Position(
            symbol=trade.symbol, quantity=trade.quantity, account_id=buyer_account.id, average_price=trade.price)
        session.add(new_position)

    else:
        new_average = ((buyer_position.quantity * buyer_position.average_price) +
                       (trade.quantity * trade.price)) / (buyer_position.quantity + trade.quantity)
        buyer_position.quantity += trade.quantity
        buyer_position.average_price = new_average

    if seller_position:
        seller_position.quantity -= trade.quantity

        if seller_position.quantity <= 0.0:
            await session.execute(delete(Position).where(Position.id == seller_position.id))


async def get_position(account_id: int, symbol: str, session: AsyncSession) -> Position:
    pos_result = await session.execute(select(Position).where(Position.account_id == account_id).where(Position.symbol == symbol))
    position = pos_result.scalar_one_or_none()
    return position


async def get_account(session: AsyncSession, account_id: int) -> Account:
    account_result = await session.execute(select(Account).where(Account.id == account_id))
    account = account_result.scalar_one_or_none()

    return account


async def _get_account_firebase_uid(session: AsyncSession, firebase_uid, email) -> Account:
    account_result = await session.execute(select(Account).where(Account.firebase_uid == firebase_uid))
    account = account_result.scalar_one_or_none()

    if account:
        return account
    else:
        new_account = await _create_db_account(firebase_uid, email, session)
        return new_account


async def _create_db_account(firebase_uid: str, email: str, session: AsyncSession) -> Account:

    new_account = Account(firebase_uid=firebase_uid, username=email)

    session.add(new_account)

    await session.flush()

    await session.refresh(new_account)

    return new_account


async def validate_order(session: AsyncSession, order: Order, account: Account) -> None:
    """
    For the functionality of this engine, we will choose to validate the order in the following ways.

    The order will get rejected if:
    - The price or quantity is less than or equal to 0
    - For a BUY order, if a user doesn't have enough in their account to fulfill the order when matched
    - For a SELL order, if a user doesn't have a position, or the position size is less than the quantity of the order
    - If a user has an order that could possibly match with the incoming order
    """
    if order.quantity <= 0:
        raise OrderValidationError("Quantity must be positive")

    if order.price is None or order.price <= 0:
        raise OrderValidationError("Orders require positive price")

    if order.side == OrderSide.BUY:

        if account.balance < order.price*order.quantity:
            raise OrderValidationError(
                "Not enough account balance to execute order!")

    else:
        position = await get_position(account.id, order.symbol, session)
        # if not position:
        #     raise OrderValidationError(
        #         "Invalid position! Unable to execute this sell order")
        if position and position.quantity < order.quantity:
            raise OrderValidationError(
                "Position size is not enough to execute sell order")
