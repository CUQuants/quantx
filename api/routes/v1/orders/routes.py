"""
Orders routes for the QuantX trading API.

Provides endpoints for order management including cancellation.
"""

import logging
from typing import Union

from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.db import get_session
from api.routes.v1.orders.dto import CancelOrderResponse, CancelOrderError
from api.security.deps import current_auth, AuthContext
from api.services.events import EventType, OrderCancelledPayload
from models import Order, Account, Position, OrderStatus, OrderSide

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/orders", tags=["orders"])


def _get_service_container():
    """
    Get the service container from the main app.
    
    This is a lazy import to avoid circular dependencies.
    """
    from api.main_v2 import get_service_container
    return get_service_container()


async def _get_position(
    session: AsyncSession,
    account_id: int,
    ticker: str,
) -> Position | None:
    """Get a position by account and ticker."""
    result = await session.execute(
        select(Position)
        .where(Position.account_id == account_id)
        .where(Position.symbol == ticker.upper())
    )
    return result.scalar_one_or_none()


@router.post(
    "/{order_id}/cancel",
    response_model=CancelOrderResponse,
    responses={
        400: {"model": CancelOrderError, "description": "Order cannot be cancelled"},
        403: {"model": CancelOrderError, "description": "Not authorized to cancel this order"},
        404: {"model": CancelOrderError, "description": "Order not found"},
    },
)
async def cancel_order(
    order_id: str,
    session: AsyncSession = Depends(get_session),
    auth: AuthContext = Depends(current_auth),
):
    logger.info(f"Cancel request for order {order_id} from user {auth.uid}")
    
    # Fetch the order
    order = await session.get(Order, order_id)
    
    if not order:
        logger.warning(f"Order {order_id} not found")
        raise HTTPException(
            status_code=404,
            detail={
                "success": False,
                "error_code": "ORDER_NOT_FOUND",
                "error_message": f"Order {order_id} not found",
                "order_id": order_id,
            },
        )
    
    # Validate ownership - check if the order belongs to the authenticated user
    if order.account_id != auth.account_id:
        logger.warning(
            f"User {auth.uid} (account {auth.account_id}) attempted to cancel "
            f"order {order_id} belonging to account {order.account_id}"
        )
        raise HTTPException(
            status_code=403,
            detail={
                "success": False,
                "error_code": "NOT_OWNER",
                "error_message": "You do not own this order",
                "order_id": order_id,
            },
        )
    
    # Check if order is cancellable
    if order.status not in (OrderStatus.PENDING, OrderStatus.PARTIAL):
        logger.warning(
            f"Order {order_id} cannot be cancelled (status: {order.status.value})"
        )
        raise HTTPException(
            status_code=400,
            detail={
                "success": False,
                "error_code": "NOT_CANCELABLE",
                "error_message": f"Order cannot be cancelled (status: {order.status.value})",
                "order_id": order_id,
            },
        )
    
    # Calculate remaining quantity
    remaining_quantity = order.quantity - order.filled_quantity
    
    # Get the account for balance restoration
    account = await session.get(Account, order.account_id)
    
    if not account:
        logger.error(f"Account {order.account_id} not found for order {order_id}")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error_code": "INTERNAL_ERROR",
                "error_message": "Account not found",
                "order_id": order_id,
            },
        )
    
    # Restore funds/shares based on order side
    if order.side == OrderSide.BUY:
        # Restore available cash
        refund_amount = remaining_quantity * order.price
        account.available_cash += refund_amount
        logger.info(f"Restored ${refund_amount:.2f} to account {account.id}")
    else:
        # Restore reserved shares
        position = await _get_position(session, order.account_id, order.symbol)
        if position:
            position.reserved_shares -= remaining_quantity
            logger.info(
                f"Restored {remaining_quantity} reserved shares for "
                f"{order.symbol} to account {account.id}"
            )
    
    # Update order status
    order.status = OrderStatus.CANCELED
    
    # Store values for event publishing before commit
    ticker = order.symbol
    side = order.side
    price = order.price
    account_id = order.account_id
    
    # Commit the changes
    await session.commit()
    
    logger.info(f"Order {order_id} cancelled successfully")
    
    # Publish ORDER_CANCELLED event so the MatchingEngine removes it from the book
    # and broadcasts market data update
    try:
        container = _get_service_container()
        
        cancelled_payload = OrderCancelledPayload(
            order_id=order_id,
            ticker=ticker,
            side=side,
            price=price,
            remaining_quantity=remaining_quantity,
            account_id=account_id,
            websocket_id="",  # No websocket for REST API calls
        )
        
        await container.event_bus.publish(
            EventType.ORDER_CANCELLED,
            cancelled_payload.to_dict(),
        )
        
        logger.info(f"Published ORDER_CANCELLED event for order {order_id}")
        
    except Exception as e:
        # Log but don't fail the request - the DB is already updated
        # The order book will sync on next hydration
        logger.error(f"Failed to publish ORDER_CANCELLED event: {e}")
    
    return CancelOrderResponse(
        success=True,
        message="Order cancelled successfully",
        order_id=order_id,
        ticker=ticker,
        status=OrderStatus.CANCELED,
    )
