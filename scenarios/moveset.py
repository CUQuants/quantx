import time, random
from dataclasses import dataclass

@dataclass
class MoveConfig:
    name: str
    intensity: float = 1.0
    duration_s: float = 3.0

class Move:
    def __init__(self, cfg: MoveConfig):
        self.cfg = cfg
        self.start_ts = None

    def start(self):
        self.start_ts = time.time()

    def expired(self):
        if self.start_ts is None:
            return True
        return (time.time() - self.start_ts) > self.cfg.duration_s

    def apply(self, engine, agent, snap):
        raise NotImplementedError

# ----- concrete moves -----
class Flood(Move):
    def apply(self, engine, agent, snap):
        for _ in range(int(30 * self.cfg.intensity)):
            side = random.choice(["BUY", "SELL"])
            agent.place_near_touch(side, price_offset_ticks=random.randint(-2,2), qty=random.randint(1,5))

class FlashCrash(Move):
    def apply(self, engine, agent, snap):
        agent.cancel_all(side="BUY")
        for _ in range(int(5 * self.cfg.intensity)):
            agent.market_sell(qty=100)

class Earthquake(Move):
    def apply(self, engine, agent, snap):
        for i in range(int(5 * self.cfg.intensity)):
            if i % 2 == 0:
                agent.market_buy(qty=50)
            else:
                agent.market_sell(qty=50)

class Recover(Move):
    def apply(self, engine, agent, snap):
        return  # do nothing to test resiliency

MOVE_REGISTRY = {
    "Flood": Flood,
    "FlashCrash": FlashCrash,
    "Earthquake": Earthquake,
    "Recover": Recover,
}
