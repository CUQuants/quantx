import pytest
from httpx import AsyncClient, ASGITransport
from manual_agent.api.main import app

@pytest.mark.asyncio
async def test_order_lifecycle():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # health
        r = await ac.get("/healthz")
        assert r.status_code == 200
        assert r.json() == {"ok": True}

        # create
        payload = {
            "account_id": "ACC-TST",
            "symbol": "MSFT",
            "side": "BUY",
            "quantity": 10,
            "limit_price": 420.0,
        }
        r = await ac.post("/v1/manual/orders", json=payload)
        assert r.status_code == 201
        order = r.json()
        assert order["account_id"] == "ACC-TST"
        assert order["symbol"] == "MSFT"
        order_id = order["id"]

        # list
        r = await ac.get("/v1/manual/orders")
        assert r.status_code == 200
        all_orders = r.json()
        assert any(o["id"] == order_id for o in all_orders)

        # cancel
        r = await ac.post(f"/v1/manual/orders/{order_id}/cancel")
        assert r.status_code == 200
        assert r.json()["ok"] is True

        # verify cancelled
        r = await ac.get("/v1/manual/orders")
        cancelled = next(o for o in r.json() if o["id"] == order_id)
        assert cancelled["status"] == "CANCELLED"
