"""
QuantX Client SDK

A Python client library for connecting to the QuantX trading platform.

Example (async):
    from quantx.client_api import QuantXClient, EventType, OrderSide
    
    async def main():
        client = QuantXClient(api_key="qntx_live_...")
        
        client.on(EventType.TRADE, lambda t: print(f"Trade: {t.price}"))
        client.on(EventType.ORDER_FILLED, lambda o: print(f"Filled: {o.id}"))
        
        await client.connect("QNTX")
        await client.subscribe_private()
        await client.send_order(OrderSide.BUY, quantity=10, price=50.00)
        
        await client.run_forever()
    
    asyncio.run(main())

Example (sync):
    from quantx.client_api import QuantXClientSync, EventType, OrderSide
    
    client = QuantXClientSync(api_key="qntx_live_...")
    
    client.connect("QNTX")
    client.on(EventType.TRADE, lambda t: print(f"Trade: {t.price}"))
    client.subscribe_private()
    client.send_order(OrderSide.BUY, quantity=10, price=50.00)
    
    client.run_forever()
"""

# Main clients
from .client import QuantXClient
from .sync_client import QuantXClientSync

# Constants and enums
from .constants import (
    EventType,
    OrderSide,
    OrderType,
    OrderStatus,
)

# Data models
from .models import (
    MarketData,
    OrderBookLevel,
    Trade,
    Order,
    AuthInfo,
    ErrorInfo,
)

# Exceptions
from .exceptions import (
    QuantXError,
    AuthenticationError,
    ConnectionError,
    NotConnectedError,
    OrderError,
)

__all__ = [
    # Clients
    "QuantXClient",
    "QuantXClientSync",
    
    # Enums
    "EventType",
    "OrderSide",
    "OrderType",
    "OrderStatus",
    
    # Models
    "MarketData",
    "OrderBookLevel",
    "Trade",
    "Order",
    "AuthInfo",
    "ErrorInfo",
    
    # Exceptions
    "QuantXError",
    "AuthenticationError",
    "ConnectionError",
    "NotConnectedError",
    "OrderError",
]

