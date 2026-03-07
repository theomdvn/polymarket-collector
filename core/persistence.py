from __future__ import annotations

import time
from typing import Any

import structlog

from core.models import BinanceTick
from core.orderbook_stream import TickEvent
from config import settings
from storage.db import binance_prices, get_engine, orderbook_ticks

log = structlog.get_logger()


class PersistenceWriter:
    def __init__(self) -> None:
        self._engine = get_engine(settings.DB_PATH)
        self._tick_buffer: list[dict[str, Any]] = []
        self._binance_buffer: list[dict[str, Any]] = []
        self._last_flush = time.monotonic()
        self._last_binance_flush = time.monotonic()

    # ── Orderbook ticks ───────────────────────────────────────────────────────

    def _should_flush(self) -> bool:
        return (
            len(self._tick_buffer) >= settings.TICK_BATCH_SIZE
            or time.monotonic() - self._last_flush >= settings.TICK_FLUSH_INTERVAL
        )

    def flush(self) -> None:
        if not self._tick_buffer:
            return
        with self._engine.begin() as conn:
            conn.execute(orderbook_ticks.insert(), self._tick_buffer)
        log.debug("persistence.flush_ticks", count=len(self._tick_buffer))
        self._tick_buffer.clear()
        self._last_flush = time.monotonic()

    def write_tick(self, event: TickEvent) -> None:
        sorted_bids = sorted(event.bids.keys(), reverse=True)
        sorted_asks = sorted(event.asks.keys())

        bid1 = sorted_bids[0] if sorted_bids else None
        bid2 = sorted_bids[1] if len(sorted_bids) > 1 else None
        ask1 = sorted_asks[0] if sorted_asks else None
        ask2 = sorted_asks[1] if len(sorted_asks) > 1 else None
        mid = (bid1 + ask1) / 2.0 if bid1 is not None and ask1 is not None else None

        self._tick_buffer.append({
            "ts_ns":      event.timestamp_ns,
            "token_id":   event.market_id,
            "asset":      event.asset,
            "horizon":    event.horizon,
            "bid1":       bid1,
            "bid1_size":  event.bids.get(bid1) if bid1 is not None else None,
            "bid2":       bid2,
            "bid2_size":  event.bids.get(bid2) if bid2 is not None else None,
            "ask1":       ask1,
            "ask1_size":  event.asks.get(ask1) if ask1 is not None else None,
            "ask2":       ask2,
            "ask2_size":  event.asks.get(ask2) if ask2 is not None else None,
            "mid":        mid,
            "last_price": event.last_price,
        })
        if self._should_flush():
            self.flush()

    # ── Binance prices ────────────────────────────────────────────────────────

    def _should_flush_binance(self) -> bool:
        return (
            len(self._binance_buffer) >= settings.BINANCE_FLUSH_BATCH
            or time.monotonic() - self._last_binance_flush >= settings.BINANCE_FLUSH_INTERVAL
        )

    def flush_binance(self) -> None:
        if not self._binance_buffer:
            return
        with self._engine.begin() as conn:
            conn.execute(binance_prices.insert(), self._binance_buffer)
        log.debug("persistence.flush_binance", count=len(self._binance_buffer))
        self._binance_buffer.clear()
        self._last_binance_flush = time.monotonic()

    def write_binance_tick(self, tick: BinanceTick) -> None:
        self._binance_buffer.append({
            "ts_ns":     tick.ts_ns,
            "asset":     tick.asset,
            "bid1":      tick.bid1,
            "bid1_size": tick.bid1_size,
            "ask1":      tick.ask1,
            "ask1_size": tick.ask1_size,
            "mid":       tick.mid,
        })
        if self._should_flush_binance():
            self.flush_binance()
