from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
import structlog

from core.models import PolymarketMarket
from state import shared_state

log = structlog.get_logger()

GAMMA_API_URL = "https://gamma-api.polymarket.com"
_PAGE_LIMIT = 100

# Slug prefix patterns per asset and horizon
_SLUG_PATTERNS: dict[str, dict[str, re.Pattern[str]]] = {
    "BTC": {
        "daily":  re.compile(r"will-btc-close-higher-on-", re.IGNORECASE),
        "hourly": re.compile(r"btc-up-or-down-next-hour", re.IGNORECASE),
        "15m":    re.compile(r"btc-up-or-down-next-15-min", re.IGNORECASE),
        "5m":     re.compile(r"btc-up-or-down-next-5-min", re.IGNORECASE),
    },
    "ETH": {
        "daily":  re.compile(r"will-eth-close-higher-on-", re.IGNORECASE),
        "hourly": re.compile(r"eth-up-or-down-next-hour", re.IGNORECASE),
        "15m":    re.compile(r"eth-up-or-down-next-15-min", re.IGNORECASE),
        "5m":     re.compile(r"eth-up-or-down-next-5-min", re.IGNORECASE),
    },
    "SOL": {
        "daily":  re.compile(r"will-sol-close-higher-on-", re.IGNORECASE),
        "hourly": re.compile(r"sol-up-or-down-next-hour", re.IGNORECASE),
        "15m":    re.compile(r"sol-up-or-down-next-15-min", re.IGNORECASE),
        "5m":     re.compile(r"sol-up-or-down-next-5-min", re.IGNORECASE),
    },
    "XRP": {
        "daily":  re.compile(r"will-xrp-close-higher-on-", re.IGNORECASE),
        "hourly": re.compile(r"xrp-up-or-down-next-hour", re.IGNORECASE),
        "15m":    re.compile(r"xrp-up-or-down-next-15-min", re.IGNORECASE),
        "5m":     re.compile(r"xrp-up-or-down-next-5-min", re.IGNORECASE),
    },
}

_STRIKE_RE = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)")
_UPDOWN_SLUG_RE = re.compile(
    r"^(?P<asset>btc|eth|sol|xrp)-updown-(?P<horizon>[a-z0-9-]+)-(?P<ts>\d{8,})",
    re.IGNORECASE,
)
_HORIZON_ALIASES: dict[str, set[str]] = {
    "daily": {"daily", "1d", "24h"},
    "hourly": {"hourly", "1h", "60m", "1hr"},
    "15m": {"15m", "15min", "15-min"},
    "5m": {"5m", "5min", "5-min"},
}


def _normalize_horizon(token: str) -> str | None:
    t = token.lower().strip()
    for canonical, aliases in _HORIZON_ALIASES.items():
        if t in aliases:
            return canonical
    return None


def _match_slug(slug: str, assets: list[str], horizons: list[str]) -> tuple[str, str] | None:
    slug_clean = slug.strip().rstrip("/").split("/")[-1]
    wanted_assets = {a.upper() for a in assets}
    wanted_horizons = {h.lower() for h in horizons}

    # New polymarket slug family, e.g. "btc-updown-15m-1772825400"
    m = _UPDOWN_SLUG_RE.search(slug_clean)
    if m:
        asset = m.group("asset").upper()
        horizon = _normalize_horizon(m.group("horizon"))
        if asset in wanted_assets and horizon and horizon in wanted_horizons:
            return asset, horizon

    # Legacy slug family fallback.
    for asset in assets:
        asset_patterns = _SLUG_PATTERNS.get(asset, {})
        for horizon in horizons:
            pat = asset_patterns.get(horizon)
            if pat and pat.search(slug_clean):
                return asset, horizon
    return None


def _extract_strike(question: str) -> float | None:
    m = _STRIKE_RE.search(question)
    if m:
        return float(m.group(1).replace(",", ""))
    return None


def _parse_end_ts(raw: dict[str, Any]) -> int:
    for key in ("endDate", "end_date_iso", "endDateIso"):
        val = raw.get(key)
        if val:
            try:
                dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
                return int(dt.timestamp())
            except (ValueError, AttributeError):
                continue
    return 0


def _extract_tokens(raw: dict[str, Any]) -> tuple[str, str]:
    """Return (token_id_yes, token_id_no) from a Gamma API market object."""
    yes_id = ""
    no_id = ""
    tokens = raw.get("tokens") or []
    for tok in tokens:
        outcome = str(tok.get("outcome", "")).lower()
        tid = str(tok.get("token_id", "") or tok.get("tokenId", ""))
        if outcome == "yes":
            yes_id = tid
        elif outcome == "no":
            no_id = tid
    # fallback: clob_token_ids list
    if not yes_id:
        ids = raw.get("clobTokenIds") or raw.get("clob_token_ids") or []
        if isinstance(ids, str):
            try:
                parsed = json.loads(ids)
                ids = parsed if isinstance(parsed, list) else []
            except json.JSONDecodeError:
                ids = [x.strip() for x in ids.split(",") if x.strip()]
        if len(ids) >= 2:
            yes_id, no_id = str(ids[0]), str(ids[1])
        elif len(ids) == 1:
            yes_id = str(ids[0])
    return yes_id, no_id


def _try_parse_gamma_market(
    raw: dict[str, Any],
    assets: list[str],
    horizons: list[str],
) -> PolymarketMarket | None:
    slug: str = raw.get("slug", "") or ""
    if not slug:
        return None

    match = _match_slug(slug, assets, horizons)
    if match is None:
        return None
    asset, horizon = match

    question: str = raw.get("question", "") or ""
    condition_id: str = raw.get("conditionId", "") or raw.get("condition_id", "") or ""
    if not condition_id:
        return None

    yes_id, no_id = _extract_tokens(raw)
    if not yes_id:
        return None

    strike = _extract_strike(question)
    end_ts = _parse_end_ts(raw)
    if end_ts > 0 and end_ts <= int(time.time()):
        return None  # already expired

    active = bool(raw.get("active", False)) and not bool(raw.get("closed", True))

    return PolymarketMarket(
        market_id=condition_id,
        token_id_yes=yes_id,
        token_id_no=no_id,
        slug=slug,
        question=question,
        asset=asset,
        horizon=horizon,
        strike=strike,
        end_ts=end_ts,
        active=active,
    )


async def discover_markets(
    assets: list[str],
    horizons: list[str],
) -> list[PolymarketMarket]:
    results: list[PolymarketMarket] = []
    headers = {"Accept": "application/json"}
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        offset = 0
        while True:
            params: dict[str, Any] = {
                "active": "true",
                "closed": "false",
                "limit": _PAGE_LIMIT,
                "offset": offset,
            }
            try:
                async with session.get(f"{GAMMA_API_URL}/markets", params=params) as resp:
                    resp.raise_for_status()
                    page: list[dict[str, Any]] = await resp.json()
            except Exception as exc:
                log.warning("market_discovery.fetch_error", offset=offset, error=str(exc))
                break

            if not page:
                break

            for raw in page:
                market = _try_parse_gamma_market(raw, assets, horizons)
                if market is not None:
                    results.append(market)

            if len(page) < _PAGE_LIMIT:
                break
            offset += _PAGE_LIMIT

    log.info("market_discovery.scan_done", found=len(results))
    return results


async def run_market_discovery(
    assets: list[str],
    horizons: list[str],
    engine: Any,
    interval: int = 300,
) -> None:
    from storage.db import upsert_market  # noqa: PLC0415

    while True:
        try:
            found = await discover_markets(assets, horizons)
            for market in found:
                upsert_market(engine, market)
                shared_state.token_meta[market.token_id_yes] = (market.asset, market.horizon)
                if market.token_id_no:
                    shared_state.token_meta[market.token_id_no] = (market.asset, market.horizon)
            log.info("market_discovery.persisted", count=len(found))
        except Exception as exc:
            log.error("market_discovery.error", error=str(exc))
        await asyncio.sleep(interval)
