from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any

import structlog
import websockets
import websockets.exceptions

from config import settings
from state import shared_state

log = structlog.get_logger()

MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_BASE_DELAY = 1.0


@dataclass
class TickEvent:
    timestamp_ns: int
    market_id: str        # token_id (YES or NO)
    bids: dict[float, float]
    asks: dict[float, float]
    last_price: float | None
    asset: str = ""
    horizon: str = ""


class _OrderBook:
    __slots__ = ("bids", "asks", "last_price")

    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.last_price: float | None = None

    def apply_book(self, bids_raw: list[Any], asks_raw: list[Any]) -> None:
        self.bids = {float(b["price"]): float(b["size"]) for b in bids_raw}
        self.asks = {float(a["price"]): float(a["size"]) for a in asks_raw}

    def apply_delta(self, changes: list[Any]) -> None:
        for c in changes:
            price = float(c["price"])
            size  = float(c["size"])
            side  = str(c.get("side", "")).upper()
            book  = self.bids if side in ("BUY", "BID") else self.asks if side in ("SELL", "ASK") else None
            if book is None:
                continue
            if size == 0:
                book.pop(price, None)
            else:
                book[price] = size


def _handle_message(
    msg: dict[str, Any],
    books: dict[str, _OrderBook],
) -> TickEvent | None:
    event_type = msg.get("event_type", "")
    asset_id: str = msg.get("asset_id", "")
    if not asset_id:
        return None

    if asset_id not in books:
        books[asset_id] = _OrderBook()
    book = books[asset_id]

    if event_type == "book":
        book.apply_book(msg.get("bids", []), msg.get("asks", []))
    elif event_type == "price_change":
        book.apply_delta(msg.get("changes", []))
    elif event_type == "last_trade_price":
        price_str = msg.get("price")
        if price_str:
            book.last_price = float(price_str)
        return None
    else:
        return None

    meta = shared_state.token_meta.get(asset_id, ("", ""))
    return TickEvent(
        timestamp_ns=time.time_ns(),
        market_id=asset_id,
        bids=dict(book.bids),
        asks=dict(book.asks),
        last_price=book.last_price,
        asset=meta[0],
        horizon=meta[1],
    )


async def _subscribe(ws: Any, token_ids: list[str]) -> None:
    msg = json.dumps({"assets_ids": token_ids, "type": "Market"})
    await ws.send(msg)
    log.info("orderbook.subscribed", count=len(token_ids))


async def stream_markets(tick_queue: asyncio.Queue[TickEvent]) -> None:
    """Connect to Polymarket CLOB WS and stream orderbook ticks.

    Subscribes to all token_ids present in shared_state.token_meta.
    Checks every 30 s for newly discovered tokens and extends the subscription.
    """
    books: dict[str, _OrderBook] = {}
    attempt = 0

    while True:
        token_ids = list(shared_state.token_meta.keys())
        if not token_ids:
            log.warning("orderbook.no_tokens_yet — waiting for market discovery")
            await asyncio.sleep(10)
            continue

        try:
            log.info("orderbook.connecting", tokens=len(token_ids), attempt=attempt)
            async with websockets.connect(
                settings.CLOB_WS_URL,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                await _subscribe(ws, token_ids)
                subscribed: set[str] = set(token_ids)
                attempt = 0
                log.info("orderbook.connected")

                async def _watch_new() -> None:
                    while True:
                        await asyncio.sleep(30)
                        new = [t for t in shared_state.token_meta if t not in subscribed]
                        if new:
                            await _subscribe(ws, new)
                            subscribed.update(new)

                watch_task = asyncio.create_task(_watch_new())
                try:
                    async for raw_msg in ws:
                        try:
                            data = json.loads(raw_msg)
                            msgs = data if isinstance(data, list) else [data]
                            for msg in msgs:
                                event = _handle_message(msg, books)
                                if event is not None:
                                    await tick_queue.put(event)
                        except (json.JSONDecodeError, KeyError, ValueError) as exc:
                            log.warning("orderbook.parse_error", error=str(exc))
                finally:
                    watch_task.cancel()

        except websockets.exceptions.ConnectionClosedError as exc:
            log.warning("orderbook.connection_closed", error=str(exc))
        except websockets.exceptions.WebSocketException as exc:
            log.error("orderbook.ws_error", error=str(exc))
        except Exception as exc:
            log.error("orderbook.unexpected_error", error=str(exc))

        attempt += 1
        if attempt >= MAX_RECONNECT_ATTEMPTS:
            attempt = 0

        delay = RECONNECT_BASE_DELAY * (2 ** min(attempt, 5))
        log.info("orderbook.reconnecting", delay=delay)
        await asyncio.sleep(delay)
