from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PolymarketMarket:
    market_id: str           # conditionId
    token_id_yes: str
    token_id_no: str
    slug: str
    question: str
    asset: str               # "BTC" | "ETH" | "SOL" | "XRP"
    horizon: str             # "daily" | "hourly" | "15m" | "5m"
    strike: float | None
    end_ts: int
    active: bool


@dataclass(frozen=True)
class BinanceTick:
    ts_ns: int
    asset: str
    bid1: float
    bid1_size: float
    ask1: float
    ask1_size: float
    mid: float
