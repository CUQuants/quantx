from typing import Optional

from engine import MatchingEngine

_engine: Optional[MatchingEngine] = None

class DummyEngine:
    class Snap:
        def __init__(self):
            self.best_bid = 100.0
            self.best_ask = 100.5
            self.midprice = 100.25
            self.depth_bid_L1 = 60
            self.depth_ask_L1 = 55
    def get_mark_snapshot(self, symbol: str):
        return DummyEngine.Snap()
    
def engine_singleton() -> MatchingEngine:
    global _engine
    if _engine is None:
        _engine = MatchingEngine()
    return _engine

