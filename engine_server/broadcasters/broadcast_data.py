from models.trading import OrderSide
from sortedcontainers import SortedDict
from itertools import islice


class MarketDataSnapshot:
    """
    This class is intended to hold an a data structure that holds the data streamed to the frontend.

    When the page is initially opened, a user will request batch data which is stored in this class.

    As orders are placed, trades are executed, the datastructure will efficiently update to match the current state of the matching engine
    and fulfilled orders.
    """

    def __init__(self, ticker: str, snapshot_length: int):
        self.snapshot_length = snapshot_length
        self.ticker = ticker
        self.bids: SortedDict = SortedDict(lambda x: -x)
        self.asks: SortedDict = SortedDict()

    def remove_order(self, price: float, amount_fulfilled: int, side: OrderSide) -> None:

        book = self.bids if side == OrderSide.BUY else self.asks

        if price not in book:
            raise ValueError(f"Price {price} not found in {side.value} side")

        if book[price] < amount_fulfilled:
            raise ValueError(
                f"Insufficient quantity at {price}: {book[price]} < {amount_fulfilled}")

        book[price] -= amount_fulfilled
        if book[price] <= 0:
            book.pop(price)

    def add_order(self, price: float, quantity: int, side: OrderSide) -> None:

        book = self.bids if side == OrderSide.BUY else self.asks
        if price not in book:
            book[price] = 0
        book[price] += quantity

    def get_best_bid(self) -> None | tuple[float, int]:
        if not self.bids:
            return None
        price = self.bids.keys()[0]
        return price, self.bids[price]

    def get_best_ask(self) -> None | tuple[float, int]:
        if not self.asks:
            return None
        price = self.asks.keys()[0]
        return price, self.asks[price]

    def get_top_book(self, side: OrderSide) -> list[tuple[float, int]]:
        book = self.asks if side == OrderSide.SELL else self.bids

        return list(islice(book.items(), self.snapshot_length))

    def get_snapshot(self):
        return {
            "bids": self.get_top_book(OrderSide.BUY),
            "asks": self.get_top_book(OrderSide.SELL)
        }
