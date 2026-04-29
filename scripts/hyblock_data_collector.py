"""
Hyblock Capital — Daily Time-Series Data Collector

Fetches all relevant 1h time-series endpoints and appends new bars to
data_cache/hyblock/{endpoint}_1h.json.

First run:  fetches limit=1000 (maximum history available, ~41 days)
Subsequent: fetches limit=50 (covers 2 days, safe overlap) and deduplicates

Run manually:  python scripts/hyblock_data_collector.py
Run via cron:  systemd timer hyblock-collector.timer (daily 00:05 UTC)
"""
import asyncio
import base64
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

# Load .env before anything else so os.getenv() picks up the values
_env_file = Path(__file__).parent.parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text(encoding="utf-8", errors="replace").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())

import httpx
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL   = os.getenv("HYBLOCK_BASE_URL", "https://api.hyblockcapital.com/v2")
API_KEY    = os.getenv("HYBLOCK_API_KEY", "")
ACCESS_ID  = os.getenv("HYBLOCK_ACCESS_KEY_ID", "")
SECRET     = os.getenv("HYBLOCK_API_SECRET", "")

CACHE_DIR  = Path(__file__).parent.parent / "data_cache" / "hyblock"
TIMEFRAME  = "1h"
FIRST_RUN_LIMIT = 1000   # max history on first run
DAILY_LIMIT     = 50     # safe overlap for daily top-up

# Endpoints using binance_perp_stable
BINANCE_ENDPOINTS = [
    "marketImbalanceIndex",
    "topTraderPositions",
    "topTraderAccounts",
    "whaleRetailDelta",
    "volumeDelta",
    "fundingRate",
    "openInterest",
    "bidAsk",
    "bidsIncreaseDecrease",
    "asksIncreaseDecrease",
]

# Endpoints using a different exchange
OKX_ENDPOINTS = [
    "averageLeverageUsed",
]


# ── OAuth2 token ──────────────────────────────────────────────────────────────

async def get_token(client: httpx.AsyncClient) -> Optional[str]:
    if not ACCESS_ID or not SECRET:
        logger.error("HYBLOCK_ACCESS_KEY_ID or HYBLOCK_API_SECRET not set")
        return None
    basic = base64.b64encode(f"{ACCESS_ID}:{SECRET}".encode()).decode()
    try:
        resp = await client.post(
            f"{BASE_URL}/oauth2/token",
            data="grant_type=client_credentials",
            headers={
                "Authorization": f"Basic {basic}",
                "Content-Type": "application/x-www-form-urlencoded",
                "x-api-key": API_KEY,
            },
            timeout=15.0,
        )
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if token:
            logger.info("Hyblock OAuth2 token OK")
        return token
    except Exception as e:
        logger.error(f"Token fetch failed: {e}")
        return None


# ── Fetch one endpoint ────────────────────────────────────────────────────────

async def fetch_endpoint(
    client: httpx.AsyncClient,
    token: str,
    endpoint: str,
    exchange: str,
    limit: int,
) -> List[Dict]:
    headers = {"x-api-key": API_KEY, "Authorization": f"Bearer {token}"}
    params  = {"coin": "BTC", "exchange": exchange, "timeframe": TIMEFRAME, "limit": limit}
    try:
        resp = await client.get(
            f"{BASE_URL}/{endpoint}",
            params=params,
            headers=headers,
            timeout=20.0,
        )
        resp.raise_for_status()
        body = resp.json()
        data = body.get("data", body)
        if isinstance(data, list):
            return data
        logger.warning(f"{endpoint}: unexpected response shape: {str(body)[:200]}")
        return []
    except Exception as e:
        logger.warning(f"{endpoint}: fetch error: {e}")
        return []


# ── Cache helpers ─────────────────────────────────────────────────────────────

def cache_path(endpoint: str) -> Path:
    return CACHE_DIR / f"{endpoint}_{TIMEFRAME}.json"


def load_cache(endpoint: str) -> List[Dict]:
    path = cache_path(endpoint)
    if not path.exists():
        return []
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load cache {path}: {e}")
        return []


def save_cache(endpoint: str, bars: List[Dict]):
    path = cache_path(endpoint)
    with open(path, "w") as f:
        json.dump(bars, f)


def get_open_date(bar: Dict) -> int:
    """Extract openDate (seconds) from a bar — handles field name variants."""
    for key in ("openDate", "timestamp", "time", "t", "openTime"):
        v = bar.get(key)
        if v is not None:
            return int(v)
    return 0


def merge_bars(existing: List[Dict], new_bars: List[Dict]) -> tuple[List[Dict], int]:
    """
    Merge new_bars into existing, deduplicating by openDate.
    Returns (merged_sorted_list, count_added).
    """
    existing_dates = {get_open_date(b) for b in existing}
    added = [b for b in new_bars if get_open_date(b) not in existing_dates]
    merged = existing + added
    merged.sort(key=get_open_date)
    return merged, len(added)


# ── Main ──────────────────────────────────────────────────────────────────────

async def run():
    if not API_KEY:
        logger.error("HYBLOCK_API_KEY not set — aborting")
        sys.exit(1)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    async with httpx.AsyncClient() as client:
        token = await get_token(client)
        if not token:
            logger.error("Could not obtain Hyblock token — aborting")
            sys.exit(1)

        total_added = 0

        all_endpoints = (
            [(ep, "binance_perp_stable") for ep in BINANCE_ENDPOINTS] +
            [(ep, "okx_perp_coin")       for ep in OKX_ENDPOINTS]
        )

        for endpoint, exchange in all_endpoints:
            existing = load_cache(endpoint)

            # First run: fetch maximum history; subsequent: fetch overlap window
            limit = DAILY_LIMIT if existing else FIRST_RUN_LIMIT

            new_bars = await fetch_endpoint(client, token, endpoint, exchange, limit)
            if not new_bars:
                logger.warning(f"{endpoint}: no data returned")
                continue

            merged, added = merge_bars(existing, new_bars)
            save_cache(endpoint, merged)
            total_added += added

            # Human-readable date range
            if merged:
                oldest = datetime.fromtimestamp(get_open_date(merged[0]),  tz=timezone.utc).strftime("%Y-%m-%d")
                newest = datetime.fromtimestamp(get_open_date(merged[-1]), tz=timezone.utc).strftime("%Y-%m-%d")
                logger.info(f"{endpoint}: {len(merged)} bars total | {oldest} to {newest} | +{added} new")
            else:
                logger.info(f"{endpoint}: 0 bars (empty after merge)")

            # Brief pause to stay within rate limits
            await asyncio.sleep(0.5)

    logger.info(f"Collection complete — {total_added} new bars added across all endpoints")


if __name__ == "__main__":
    asyncio.run(run())
