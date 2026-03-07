from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SharedState:
    # token_id (YES or NO) -> (asset, horizon) — populated by market discovery
    token_meta: dict[str, tuple[str, str]] = field(default_factory=dict)
    # asset -> latest mid price USD — populated by Binance feed
    binance_mids: dict[str, float] = field(default_factory=dict)


shared_state = SharedState()
