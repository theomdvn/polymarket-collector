from __future__ import annotations

import asyncio
import json
import time

import structlog
import websockets
import websockets.exceptions

from core.models import BinanceTick

log = structlog.get_logger()

_ASSET_TO_STREAM: dict[str, str] = {
    "BTC": "btcusdt",
    "ETH": "ethusdt",
    "SOL": "solusdt",
    "XRP": "xrpusdt",
}
_STREAM_TO_ASSET: dict[str, str] = {v: k for k, v in _ASSET_TO_STREAM.items()}

MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_BASE_DELAY = 1.0


def _build_ws_url(base_url: str, assets: list[str]) -> str:
    streams = "/".join(
        f"{_ASSET_TO_STREAM[a]}@bookTicker"
        for a in assets
        if a in _ASSET_TO_STREAM
    )
    return f"{base_url}?streams={streams}"


def _parse_message(raw: str) -> BinanceTick | None:
    try:
        msg = json.loads(raw)
        stream: str = msg.get("stream", "")
        symbol = stream.split("@")[0]
        asset = _STREAM_TO_ASSET.get(symbol)
        if asset is None:
            return None

        data = msg.get("data", {})
        # bookTicker fields: b=best bid price, B=best bid qty, a=best ask price, A=best ask qty
        bid1 = float(data["b"])
        bid1_size = float(data["B"])
        ask1 = float(data["a"])
        ask1_size = float(data["A"])
        mid = (bid1 + ask1) / 2.0

        return BinanceTick(
            ts_ns=time.time_ns(),
            asset=asset,
            bid1=bid1,
            bid1_size=bid1_size,
            ask1=ask1,
            ask1_size=ask1_size,
            mid=mid,
        )
    except (json.JSONDecodeError, KeyError, ValueError) as exc:
        log.warning("binance_feed.parse_error", error=str(exc))
        return None


async def start_binance_feed(
    assets: list[str],
    queue: asyncio.Queue[BinanceTick],
    ws_base_url: str,
) -> None:
    valid_assets = [a for a in assets if a in _ASSET_TO_STREAM]
    if not valid_assets:
        log.warning("binance_feed.no_valid_assets")
        return

    url = _build_ws_url(ws_base_url, valid_assets)
    attempt = 0

    while True:
        try:
            log.info("binance_feed.connecting", url=url, attempt=attempt)
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=10,
            ) as ws:
                attempt = 0
                log.info("binance_feed.connected", assets=valid_assets)
                async for raw_msg in ws:
                    tick = _parse_message(raw_msg)
                    if tick is not None:
                        await queue.put(tick)

        except websockets.exceptions.ConnectionClosedError as exc:
            log.warning("binance_feed.connection_closed", error=str(exc))
        except websockets.exceptions.WebSocketException as exc:
            log.error("binance_feed.websocket_error", error=str(exc))
        except Exception as exc:
            log.error("binance_feed.unexpected_error", error=str(exc))

        attempt += 1
        if attempt >= MAX_RECONNECT_ATTEMPTS:
            log.warning("binance_feed.max_reconnects_resetting", attempts=attempt)
            attempt = 0

        delay = RECONNECT_BASE_DELAY * (2 ** min(attempt, 5))
        log.info("binance_feed.reconnecting", delay=delay)
        await asyncio.sleep(delay)
