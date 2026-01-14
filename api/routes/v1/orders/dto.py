"""DTOs for order-related endpoints."""

from pydantic import BaseModel

from models import OrderStatus


class CancelOrderResponse(BaseModel):
    """Response for successful order cancellation."""
    
    success: bool
    message: str
    order_id: str
    ticker: str
    status: OrderStatus


class CancelOrderError(BaseModel):
    """Response for failed order cancellation."""
    
    success: bool = False
    error_code: str
    error_message: str
    order_id: str
