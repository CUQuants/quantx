import yaml
from pathlib import Path

# Use package-absolute imports (you’re running with: python -m quantx.scripts.run_manual_agent)
from quantx.configs.manual_agent_config import ManualAgentConfig
from quantx.agents.manual_stress_agent import ManualStressAgent

# ---- Minimal dummy engine so _snapshot() works without pulling services/engine_holder.py ----
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
# -----------------------------------------------------------------------

def main():
    cfg = ManualAgentConfig()

    # Choose engine
    engine = DummyEngine()
    # When you’re ready for the real engine:
    # from quantx.services.engine_holder import engine_singleton
    # engine = engine_singleton()

    # Always write under quantx/runs/manual_moves_001
    RUN_DIR = Path(__file__).resolve().parents[1] / "runs" / "manual_moves_001"
    agent = ManualStressAgent(engine, cfg, run_dir=str(RUN_DIR))

    # Load moves.yaml from quantx/configs/moves.yaml
    moves_cfg_path = Path(__file__).resolve().parents[1] / "configs" / "moves.yaml"
    with open(moves_cfg_path, "r") as f:
        moves_cfg = yaml.safe_load(f)["moves"]
    agent.load_moves(moves_cfg)

    # Pace the loop so move durations actually elapse
    agent.run(ticks=200, sleep_s=0.05)

    print(f"Run complete. Check {RUN_DIR} for CSV logs.")

if __name__ == "__main__":
    main()