from dataclasses import dataclass

@dataclass
class ManualAgentConfig:
    enabled: bool = True
    seed: int = 42
    symbol: str = "XYZ"
    scenario: str = "moveset"  # default: uses Pokémon-style moves
    side: str = "BUY"
    simulation_only: bool = True