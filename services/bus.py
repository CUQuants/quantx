from typing import Dict, Set
from fastapi import WebSocket

class ConnectionManager:
    def __init__(self):
        self.market_rooms: Dict[str, Set[WebSocket]] = {}
        self.account_rooms: Dict[int, Set[WebSocket]] = {}

    async def join_market(self, symbol: str, ws: WebSocket):
        await ws.accept()
        self.market_rooms.setdefault(symbol, set()).add(ws)

    async def leave_market(self, symbol: str, ws: WebSocket):
        self.market_rooms.get(symbol, set()).discard(ws)

    async def join_account(self, account_id: int, ws: WebSocket):
        await ws.accept()
        self.account_rooms.setdefault(account_id, set()).add(ws)

    async def leave_account(self, account_id: int, ws: WebSocket):
        self.account_rooms.get(account_id, set()).discard(ws)

    async def send_market(self, symbol: str, message: dict):
        for ws in self.market_rooms.get(symbol, set()):
            try:
                await ws.send_json(message)
            except Exception:
                await self.leave_market(symbol, ws)

    async def send_account(self, account_id: int, message: dict):
        for ws in self.account_rooms.get(account_id, set()):
            try:
                await ws.send_json(message)
            except Exception:
                await self.leave_account(account_id, ws)

manager = ConnectionManager()