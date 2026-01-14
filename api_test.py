from client_api import QuantXClient, EventType, OrderSide
import asyncio

API_KEY = "qntx_live_16dea16dc50e2fc24b9580e9fae86d4a"

async def main():
    client = QuantXClient(api_key=API_KEY)
    
    client.on(EventType.TRADE, lambda t: print(f"Trade: {t.quantity} @ {t.price}"))
    client.on(EventType.ORDER_FILLED, lambda o: print(f"Order {o.id} filled!"))
    
    await client.connect("NVDA")
    await client.subscribe_public()
    # await client.send_order(OrderSide.BUY, quantity=10, price=50.00)
    
    await client.run_forever()

asyncio.run(main())