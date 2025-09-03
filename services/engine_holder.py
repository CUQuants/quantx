from typing import Optional

from engine import MatchingEngine

_engine: Optional[MatchingEngine] = None

def engine_singleton() -> MatchingEngine:
    global _engine
    if _engine is None:
        _engine = MatchingEngine()
    return _engine