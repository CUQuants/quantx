import csv, os, time
from pathlib import Path
import time

class CSVLogger:
    @staticmethod
    def now_ns():
        return time.time_ns() 

class CSVLogger:
    def __init__(self, run_dir: str):
        Path(run_dir).mkdir(parents=True, exist_ok=True)
        self.ms_path = os.path.join(run_dir, "market_state.csv")
        self.as_path = os.path.join(run_dir, "agent_state.csv")
        self.aa_path = os.path.join(run_dir, "agent_actions.csv")
        self.om_path = os.path.join(run_dir, "outcome_metrics.csv")
        self.mv_path = os.path.join(run_dir, "moves_log.csv")

        self._init(self.ms_path, ["timestamp","best_bid","best_ask","midprice","spread"])
        self._init(self.as_path, ["timestamp","agent_inventory","agent_cash"])
        self._init(self.aa_path, ["timestamp","action_type","action_side","qty"])
        self._init(self.om_path, ["timestamp","spread_after","depth_after"])
        self._init(self.mv_path, ["timestamp","move_name","intensity","duration_s",
                                  "spread_before","spread_after","depth_before","depth_after"])

    def _init(self, path, header):
        if not os.path.exists(path):
            with open(path, "w", newline="") as f:
                csv.writer(f).writerow(header)

    @staticmethod
    def now_ms():
        return int(time.time() * 1000)

    def write(self, path, row):
        with open(path, "a", newline="") as f:
            csv.writer(f).writerow(row)

    def log_move(self, ts, move_name, intensity, duration_s, spread_before, spread_after, depth_before, depth_after):
        self.write(self.mv_path, [ts, move_name, intensity, duration_s, spread_before, spread_after, depth_before, depth_after])
