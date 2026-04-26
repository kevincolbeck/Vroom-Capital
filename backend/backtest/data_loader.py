"""
Historical Data Loader
Downloads and caches BTC/USDT historical klines + funding rates from Binance public API.
No API key required — all public endpoints.
"""
import os
import json
import time
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Optional, Callable
from pathlib import Path
import httpx
from loguru import logger

CACHE_DIR = Path(__file__).parent.parent.parent / "data_cache"
CACHE_DIR.mkdir(exist_ok=True)

SYMBOL = "BTCUSDT"

# CryptoCompare — free, no API key required, full multi-year hourly history
CRYPTOCOMPARE_BASE = "https://min-api.cryptocompare.com"
CC_LIMIT = 2000  # max candles per request

# OKX for funding rate history (US-accessible public API)
OKX_BASE = "https://www.okx.com"


async def fetch_klines_cryptocompare(
    client: httpx.AsyncClient,
    interval: str,
    to_ts: int,
) -> List[Dict]:
    """
    Fetch up to CC_LIMIT hourly candles from CryptoCompare ending at to_ts (seconds).
    Returns candles in ascending time order.
    """
    endpoint_map = {"1h": "histohour", "6h": "histohour", "1d": "histoday"}
    ep = endpoint_map.get(interval, "histohour")
    agg = 6 if interval == "6h" else 1

    resp = await client.get(
        f"{CRYPTOCOMPARE_BASE}/data/v2/{ep}",
        params={
            "fsym": "BTC",
            "tsym": "USD",
            "limit": CC_LIMIT,
            "toTs": to_ts,
            "aggregate": agg,
        },
        timeout=30.0,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("Response") == "Error":
        raise ValueError(f"CryptoCompare error: {body.get('Message')}")

    raw = body["Data"]["Data"]
    candles = []
    for c in raw:
        if c["open"] == 0 and c["close"] == 0:
            continue  # skip empty candles
        ts_ms = int(c["time"]) * 1000
        candles.append({
            "open_time": ts_ms,
            "open": float(c["open"]),
            "high": float(c["high"]),
            "low": float(c["low"]),
            "close": float(c["close"]),
            "volume": float(c["volumefrom"]),
            "close_time": ts_ms + agg * 3600 * 1000 - 1,
        })
    return candles


async def download_klines(
    interval: str,
    start_dt: datetime,
    end_dt: datetime,
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> List[Dict]:
    """
    Download all klines using CryptoCompare (paginating backward from end_dt).
    Returns chronologically sorted list of candles.
    """
    cache_file = CACHE_DIR / f"klines_{interval}_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.json"

    if cache_file.exists():
        logger.info(f"Loading cached {interval} klines from {cache_file}")
        with open(cache_file) as f:
            data = json.load(f)
        if progress_cb:
            progress_cb(1.0, f"Loaded {len(data)} cached {interval} candles")
        return data

    logger.info(f"Downloading {interval} klines {start_dt.date()} → {end_dt.date()} (CryptoCompare)")
    all_candles: List[Dict] = []
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    total_range = end_ms - start_ms

    # CryptoCompare paginates backward — start at end_dt and work backward
    current_to_ts = int(end_dt.timestamp())

    async with httpx.AsyncClient(timeout=30.0) as client:
        consecutive_errors = 0
        while True:
            try:
                page = await fetch_klines_cryptocompare(client, interval, current_to_ts)
                consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                logger.warning(f"CryptoCompare kline fetch failed ({consecutive_errors}): {e}")
                if consecutive_errors >= 5:
                    raise RuntimeError(f"CryptoCompare download failed after 5 retries: {e}")
                await asyncio.sleep(3)
                continue

            if not page:
                break

            # Keep only candles in our date range
            in_range = [c for c in page if start_ms <= c["open_time"] < end_ms]
            all_candles.extend(in_range)

            earliest_in_page = page[0]["open_time"]
            progress = max(0.0, min((end_ms - earliest_in_page) / total_range, 1.0))
            if progress_cb:
                progress_cb((1.0 - progress) * 0.9, f"Downloading {interval}: {len(all_candles)} candles...")

            logger.debug(f"  CC page: {len(page)} candles, earliest={earliest_in_page}, start_ms={start_ms}")

            if earliest_in_page <= start_ms:
                break  # reached the beginning

            # Move toTs back to just before the earliest candle in this page
            agg_hours = 6 if interval == "6h" else 1
            current_to_ts = int(page[0]["open_time"] / 1000) - agg_hours * 3600
            await asyncio.sleep(0.2)

    # Deduplicate and sort
    seen: set = set()
    unique: List[Dict] = []
    for c in all_candles:
        if c["open_time"] not in seen:
            seen.add(c["open_time"])
            unique.append(c)
    unique.sort(key=lambda x: x["open_time"])

    with open(cache_file, "w") as f:
        json.dump(unique, f)

    logger.info(f"Downloaded {len(unique)} {interval} candles, cached to {cache_file}")
    if progress_cb:
        progress_cb(1.0, f"Downloaded {len(unique)} {interval} candles")
    return unique


async def download_funding_rates(
    start_dt: datetime,
    end_dt: datetime,
    progress_cb: Optional[Callable[[float, str], None]] = None,
) -> Dict[int, float]:
    """
    Download historical funding rates from OKX public API.
    Returns {timestamp_ms: funding_rate} dict.
    """
    cache_file = CACHE_DIR / f"funding_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.json"

    if cache_file.exists():
        logger.info("Loading cached funding rates")
        with open(cache_file) as f:
            data = json.load(f)
        return {int(k): v for k, v in data.items()}

    logger.info(f"Downloading funding rates {start_dt.date()} → {end_dt.date()} (OKX)")
    all_rates: Dict[int, float] = {}
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    # Paginate forward using 'after': returns records newer than the given ts
    # Start just before start_ms and step forward
    current_after = start_ms - 1

    async with httpx.AsyncClient(timeout=30.0) as client:
        consecutive_errors = 0
        while current_after < end_ms:
            try:
                resp = await client.get(
                    f"{OKX_BASE}/api/v5/public/funding-rate-history",
                    params={
                        "instId": "BTC-USDT-SWAP",
                        "after": str(current_after),
                        "limit": "100",
                    },
                )
                resp.raise_for_status()
                body = resp.json()
                consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                logger.warning(f"OKX funding fetch failed ({consecutive_errors}): {e}")
                if consecutive_errors >= 5:
                    logger.warning("OKX funding unavailable — using synthetic 0.01% rate")
                    break
                await asyncio.sleep(3)
                continue

            data = body.get("data", [])
            if not data:
                break

            latest_in_page = current_after
            for item in data:
                ts = int(item["fundingTime"])
                rate = float(item["fundingRate"])
                if start_ms <= ts < end_ms:
                    all_rates[ts] = rate
                if ts > latest_in_page:
                    latest_in_page = ts

            if latest_in_page == current_after or latest_in_page >= end_ms:
                break

            current_after = latest_in_page
            await asyncio.sleep(0.2)

    # If we couldn't get any real data, synthesize a typical 0.01% per 8h funding
    if not all_rates:
        logger.warning("No funding rate data — synthesizing 0.01% rate every 8 hours")
        ts = int(start_dt.timestamp() * 1000)
        while ts < end_ms:
            all_rates[ts] = 0.0001
            ts += 8 * 3600 * 1000

    if progress_cb:
        progress_cb(1.0, f"Downloaded {len(all_rates)} funding rate records")

    with open(cache_file, "w") as f:
        json.dump({str(k): v for k, v in all_rates.items()}, f)

    return all_rates


def build_6h_from_1h(candles_1h: List[Dict]) -> List[Dict]:
    """
    Aggregate 1H candles into 6H candles.
    Bitunix/Binance 6H bars open at 0, 6, 12, 18 UTC.
    """
    candles_6h = {}
    for c in candles_1h:
        dt = datetime.fromtimestamp(c["open_time"] / 1000, tz=timezone.utc)
        # Snap to 6H boundary
        hour_bucket = (dt.hour // 6) * 6
        bucket_dt = dt.replace(hour=hour_bucket, minute=0, second=0, microsecond=0)
        bucket_ts = int(bucket_dt.timestamp() * 1000)

        if bucket_ts not in candles_6h:
            candles_6h[bucket_ts] = {
                "open_time": bucket_ts,
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"],
                "volume": c["volume"],
            }
        else:
            b = candles_6h[bucket_ts]
            b["high"] = max(b["high"], c["high"])
            b["low"] = min(b["low"], c["low"])
            b["close"] = c["close"]
            b["volume"] += c["volume"]

    result = sorted(candles_6h.values(), key=lambda x: x["open_time"])
    return result


def get_funding_at_time(funding_rates: Dict[int, float], ts_ms: int) -> float:
    """
    Find the most recent funding rate at or before ts_ms.
    Funding is updated every 8 hours.
    """
    if not funding_rates:
        return 0.0
    # Find the latest funding timestamp <= ts_ms
    valid = [ts for ts in funding_rates if ts <= ts_ms]
    if not valid:
        return 0.0
    return funding_rates[max(valid)]


def clear_cache():
    """Clear all cached data files."""
    for f in CACHE_DIR.glob("*.json"):
        f.unlink()
    logger.info("Cache cleared")
