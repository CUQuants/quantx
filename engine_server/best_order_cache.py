from typing import Dict


class OrderCache:
    """
    This class is intended to serve as a cache for each user's best order.

    It will include for each user and ticker:
    - Maximum buy order 
    - Minimum sell order


    When validating an order, we will be able to get O(1) lookups to see if a user's incoming order will match with one of their own.
    """

    def __init__(self):
        self.best_bids: Dict[int, Dict[str, float]] = {}
        self.best_asks: Dict[int, Dict[str, float]] = {}

    def get_best_ask(self, account_id: int, symbol: str) -> float | None:
        acc = self.best_asks.get(account_id)
        if not acc or symbol not in acc:
            return None
        else:
            return acc.get(symbol)

    def get_best_bid(self, account_id: int, symbol: str) -> float | None:
        acc = self.best_bids.get(account_id)
        if not acc or symbol not in acc:
            return None
        else:
            return acc.get(symbol)

    def update_best_bid(self, account_id: int, symbol: str, price: float) -> None:
        if account_id not in self.best_bids:
            self.best_bids[account_id] = {}

        self.best_bids[account_id][symbol] = max(
            self.best_bids[account_id].get(symbol, -1), price)

    def update_best_ask(self, account_id: int, symbol: str, price: float) -> None:
        if account_id not in self.best_asks:
            self.best_asks[account_id] = {}

        self.best_asks[account_id][symbol] = min(
            self.best_asks[account_id].get(symbol, float('inf')), price)
