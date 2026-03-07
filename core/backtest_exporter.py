"""Exports collector SQLite data to the Parquet format expected by polymarket-backtest."""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sqlalchemy as sa
import structlog

from storage.db import binance_prices, markets, orderbook_ticks

log = structlog.get_logger()

# Backtest orderbook schema (matches backtest/backtest/loader.py expectations)
SCHEMA_OB = pa.schema([
    pa.field("ts_ns",      pa.int64()),
    pa.field("market_id",  pa.string()),   # conditionId
    pa.field("crypto",     pa.string()),
    pa.field("timeframe",  pa.string()),
    pa.field("strike",     pa.float64()),
    pa.field("expiry",     pa.string()),
    pa.field("bid_yes",    pa.float64()),
    pa.field("ask_yes",    pa.float64()),
    pa.field("bid_no",     pa.float64()),
    pa.field("ask_no",     pa.float64()),
    pa.field("mid_yes",    pa.float64()),
    pa.field("mid_no",     pa.float64()),
])

SCHEMA_BN = pa.schema([
    pa.field("ts_ns",  pa.int64()),
    pa.field("crypto", pa.string()),
    pa.field("close",  pa.float64()),
])

# Collector uses "hourly", backtest config uses "hourly" too after this change.
# No mapping needed — horizon is used as-is for timeframe.


def _load_market_meta(engine: sa.Engine) -> dict[str, dict[str, Any]]:
    """Returns {token_id_yes: {market_id, token_id_no, asset, horizon, strike, expiry}}."""
    with engine.connect() as conn:
        rows = conn.execute(markets.select()).mappings().all()

    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        tid_yes = row.get("token_id_yes", "")
        if not tid_yes:
            continue
        end_ts = row.get("end_ts") or 0
        expiry = (
            datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat()
            if end_ts else ""
        )
        result[tid_yes] = {
            "token_id_no": row.get("token_id_no", "") or "",
            "market_id": row.get("market_id", "") or "",
            "asset": row.get("asset", "") or "",
            "horizon": row.get("horizon", "") or "",
            "strike": float(row.get("strike") or 0.0),
            "expiry": expiry,
        }
    return result


def _query_ob_since(engine: sa.Engine, since_ns: int) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            orderbook_ticks.select()
            .where(orderbook_ticks.c.ts_ns > since_ns)
            .order_by(orderbook_ticks.c.ts_ns)
        ).mappings().all()
    return [dict(r) for r in rows]


def _query_binance_since(engine: sa.Engine, since_ns: int) -> list[dict[str, Any]]:
    with engine.connect() as conn:
        rows = conn.execute(
            binance_prices.select()
            .where(binance_prices.c.ts_ns > since_ns)
            .order_by(binance_prices.c.ts_ns)
        ).mappings().all()
    return [dict(r) for r in rows]


def _append_parquet(path: Path, df_new: pd.DataFrame, schema: pa.Schema) -> None:
    """Merge df_new into existing Parquet file, deduplicate on ts_ns.

    Uses a temp file + atomic rename to prevent corruption on crash.
    Falls back to df_new only if the existing file is unreadable.
    """
    df_old: pd.DataFrame | None = None
    if path.exists():
        try:
            df_old = pq.read_table(str(path)).to_pandas()
        except Exception as exc:
            log.warning("backtest_export.corrupt_parquet",
                        path=str(path), error=str(exc), action="overwriting")
            path.unlink(missing_ok=True)

    if df_old is not None and not df_old.empty:
        df = (
            pd.concat([df_old, df_new], ignore_index=True)
            .drop_duplicates(subset=["ts_ns"])
            .sort_values("ts_ns")
            .reset_index(drop=True)
        )
    else:
        df = df_new.sort_values("ts_ns").reset_index(drop=True)

    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

    # Write to temp file first, then atomic rename
    tmp = path.with_suffix(".tmp.parquet")
    try:
        pq.write_table(table, str(tmp), compression="snappy")
        tmp.replace(path)
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def _export_ob(
    data_dir: Path,
    ob_rows: list[dict[str, Any]],
    market_meta: dict[str, dict[str, Any]],
) -> None:
    # Reverse lookup: token_id_no → meta
    no_to_meta: dict[str, dict[str, Any]] = {
        m["token_id_no"]: m
        for m in market_meta.values()
        if m.get("token_id_no")
    }

    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}

    for row in ob_rows:
        token_id = row.get("token_id", "")

        if token_id in market_meta:
            meta = market_meta[token_id]
            is_yes = True
        elif token_id in no_to_meta:
            meta = no_to_meta[token_id]
            is_yes = False
        else:
            continue

        asset = meta["asset"]
        horizon = meta["horizon"]
        if not asset or not horizon:
            continue

        day = datetime.fromtimestamp(row["ts_ns"] / 1e9, tz=timezone.utc).strftime("%Y-%m-%d")
        key = (asset, horizon, day)

        bid1 = float(row.get("bid1") or 0.0)
        ask1 = float(row.get("ask1") or 0.0)
        mid = float(row.get("mid") or 0.0)

        if is_yes:
            bid_yes, ask_yes, mid_yes = bid1, ask1, mid
            bid_no  = 1.0 - ask1 if ask1 else 0.0
            ask_no  = 1.0 - bid1 if bid1 else 0.0
            mid_no  = 1.0 - mid if mid else 0.0
        else:
            bid_no, ask_no, mid_no = bid1, ask1, mid
            bid_yes = 1.0 - ask1 if ask1 else 0.0
            ask_yes = 1.0 - bid1 if bid1 else 0.0
            mid_yes = 1.0 - mid if mid else 0.0

        groups.setdefault(key, []).append({
            "ts_ns":     row["ts_ns"],
            "market_id": meta["market_id"],
            "crypto":    asset,
            "timeframe": horizon,
            "strike":    meta["strike"],
            "expiry":    meta["expiry"],
            "bid_yes":   bid_yes,
            "ask_yes":   ask_yes,
            "bid_no":    bid_no,
            "ask_no":    ask_no,
            "mid_yes":   mid_yes,
            "mid_no":    mid_no,
        })

    for (asset, horizon, day), rows in groups.items():
        path = data_dir / "orderbooks" / asset / horizon / f"{day}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df_new = pd.DataFrame(rows)
        for col in ("strike", "bid_yes", "ask_yes", "bid_no", "ask_no", "mid_yes", "mid_no"):
            df_new[col] = pd.to_numeric(df_new[col], errors="coerce").fillna(0.0)
        _append_parquet(path, df_new, SCHEMA_OB)
        log.debug("backtest_export.ob", asset=asset, horizon=horizon, day=day, rows=len(rows))


def _export_binance(data_dir: Path, bn_rows: list[dict[str, Any]]) -> None:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in bn_rows:
        asset = row.get("asset", "")
        if not asset:
            continue
        day = datetime.fromtimestamp(row["ts_ns"] / 1e9, tz=timezone.utc).strftime("%Y-%m-%d")
        groups.setdefault((asset, day), []).append({
            "ts_ns":  row["ts_ns"],
            "crypto": asset,
            "close":  float(row.get("mid") or 0.0),
        })

    for (asset, day), rows in groups.items():
        path = data_dir / "binance" / asset / f"{day}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df_new = pd.DataFrame(rows)
        df_new["close"] = pd.to_numeric(df_new["close"], errors="coerce").fillna(0.0)
        _append_parquet(path, df_new, SCHEMA_BN)
        log.debug("backtest_export.binance", asset=asset, day=day, rows=len(rows))


def _write_metadata(data_dir: Path, market_meta: dict[str, dict[str, Any]]) -> None:
    markets_out: dict[str, dict[str, Any]] = {
        tid_yes: {
            "market_id":    meta["market_id"],
            "token_id_yes": tid_yes,
            "token_id_no":  meta.get("token_id_no", ""),
            "crypto":       meta["asset"],
            "timeframe":    meta["horizon"],
            "strike":       meta["strike"],
            "expiry":       meta["expiry"],
        }
        for tid_yes, meta in market_meta.items()
    }
    path = data_dir / "metadata.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump({"markets": markets_out, "updated_at": int(time.time())}, f, indent=2)


async def run_backtest_exporter(
    backtest_data_dir: str,
    engine: sa.Engine,
    interval: float = 60.0,
    lookback_seconds: int = 3600,
) -> None:
    """Periodically export collector SQLite data to polymarket-backtest Parquet format.

    Runs every `interval` seconds. On first run, exports the last `lookback_seconds`
    of history; subsequent runs export only new rows (incremental).
    """
    data_dir = Path(backtest_data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    log.info("backtest_exporter.starting", data_dir=str(data_dir), interval=interval)

    last_export_ns: int = (int(time.time()) - lookback_seconds) * 1_000_000_000

    while True:
        try:
            market_meta = _load_market_meta(engine)
            ob_rows = _query_ob_since(engine, last_export_ns)
            bn_rows = _query_binance_since(engine, last_export_ns)

            if ob_rows or bn_rows:
                _export_ob(data_dir, ob_rows, market_meta)
                _export_binance(data_dir, bn_rows)
                _write_metadata(data_dir, market_meta)

                all_ts = [r["ts_ns"] for r in ob_rows + bn_rows]
                last_export_ns = max(all_ts)
                log.info(
                    "backtest_exporter.done",
                    ob_rows=len(ob_rows),
                    bn_rows=len(bn_rows),
                    markets=len(market_meta),
                )

        except Exception as exc:
            log.error("backtest_exporter.error", error=str(exc))

        await asyncio.sleep(interval)
