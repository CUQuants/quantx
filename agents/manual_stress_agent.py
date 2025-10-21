import random
try:
    from quantx.scenarios.moveset import MOVE_REGISTRY, MoveConfig
    from quantx.utils.csv_logger import CSVLogger
except ModuleNotFoundError:
    from scenarios.moveset import MOVE_REGISTRY, MoveConfig
    from utils.csv_logger import CSVLogger
    
import time

class ManualStressAgent:
    def __init__(self, engine, cfg, run_dir="runs/manual_moves"):
        self.engine = engine
        self.cfg = cfg
        self.log = CSVLogger(run_dir)
        self.moves = []
        self.current_move = None
        self.inventory = 0
        self.cash = 0

    def load_moves(self, moves_cfg):
        self.moves = []
        for mc in moves_cfg:
            mcls = MOVE_REGISTRY.get(mc["name"])
            if mcls:
                self.moves.append(mcls(MoveConfig(**mc)))

    # basic placeholder helpers (hook to engine)
    def place_near_touch(self, side, price_offset_ticks, qty):
        self.log.write(self.log.aa_path, [CSVLogger.now_ms(),"ADD_ORDER",side,qty])

    def market_buy(self, qty):
        self.log.write(self.log.aa_path, [CSVLogger.now_ms(),"MARKET_ORDER","BUY",qty])

    def market_sell(self, qty):
        self.log.write(self.log.aa_path, [CSVLogger.now_ms(),"MARKET_ORDER","SELL",qty])

    def cancel_all(self, side=None):
        self.log.write(self.log.aa_path, [CSVLogger.now_ms(),"CANCEL_ALL",side or "BOTH",0])

    def _snapshot(self):
        """Grab a live snapshot from the engine instead of random values."""
        snap = self.engine.get_mark_snapshot(self.cfg.symbol)

        # Protect against missing attributes in the snapshot object
        best_bid = getattr(snap, "best_bid", None)
        best_ask = getattr(snap, "best_ask", None)
        spread = None
        if best_bid is not None and best_ask is not None:
            spread = best_ask - best_bid

        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "midprice": getattr(snap, "midprice", None),
            "spread": spread,
            "depth_bid_L1": getattr(snap, "depth_bid_L1", 0),
            "depth_ask_L1": getattr(snap, "depth_ask_L1", 0),
        }
    def _log_market(self, ts, snap):
        """Record the current market snapshot."""
        self.log.write(self.log.ms_path, [
            ts,
            snap.get("best_bid"),
            snap.get("best_ask"),
            snap.get("midprice"),
            snap.get("spread"),
        ])

    def _log_agent_state(self, ts):
        """Record agent cash/inventory."""
        self.log.write(self.log.as_path, [
            ts,
            getattr(self, "inventory", 0),
            getattr(self, "cash", 0.0),
        ])

    def _log_action(self, ts, a_type, a_side, qty):
        """Record a trading action."""
        self.log.write(self.log.aa_path, [ts, a_type, a_side, qty])

    def _log_outcome_basic(self, ts, snap_after):
        """Record simple after-move outcomes."""
        spread_after = snap_after.get("spread")
        depth_after = (snap_after.get("depth_bid_L1") or 0) + (snap_after.get("depth_ask_L1") or 0)
        self.log.write(self.log.om_path, [ts, spread_after, depth_after])
        
    def tick(self):
        ts = CSVLogger.now_ms()
        snap_before = self._snapshot()

        # record market + agent state BEFORE doing anything
        self._log_market(ts, snap_before)
        self._log_agent_state(ts)

        # existing move logic
        if not self.current_move or self.current_move.expired():
            self._mi = getattr(self, "_mi", -1) + 1
            if self.moves:
                self.current_move = self.moves[self._mi % len(self.moves)]
                self.current_move.start()
                self.log.log_move(
                    ts,
                    self.current_move.cfg.name,
                    self.current_move.cfg.intensity,
                    self.current_move.cfg.duration_s,
                    snap_before.get("spread"),
                    None,
                    (snap_before.get("depth_bid_L1") or 0) + (snap_before.get("depth_ask_L1") or 0),
                    None,
                )

        if self.current_move:
            self.current_move.apply(self.engine, self, snap_before)

        snap_after = self._snapshot()

        # log final before/after for this tick
        self.log.log_move(
            ts,
            self.current_move.cfg.name,
            self.current_move.cfg.intensity,
            self.current_move.cfg.duration_s,
            snap_before.get("spread"),
            snap_after.get("spread"),
            (snap_before.get("depth_bid_L1") or 0) + (snap_before.get("depth_ask_L1") or 0),
            (snap_after.get("depth_bid_L1") or 0) + (snap_after.get("depth_ask_L1") or 0),
        )

        # record outcome AFTER move
        self._log_outcome_basic(ts, snap_after)    


    def run(self, ticks=1000, sleep_s=0.05):
        for _ in range(ticks):
            self.tick()
            time.sleep(sleep_s)