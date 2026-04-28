"""
Heikin Ashi Candle Calculator
Converts raw OHLCV data into Heikin Ashi candles for trend analysis.
"""
import time
from typing import List, Dict
import numpy as np


def drop_in_progress(candles: List[Dict], interval_seconds: int) -> List[Dict]:
    """
    Return `candles` with the last entry removed only if it is still forming.
    Some exchange APIs return only closed candles (no in-progress stub), in
    which case blindly stripping [-1] silently discards the most recent real
    candle.  We check: if open_time + interval > now → still in-progress →
    safe to strip.  Otherwise all returned candles are closed → keep them all.
    """
    if not candles:
        return candles
    now_ms = time.time() * 1000
    last_open_ms = candles[-1]["open_time"]
    if last_open_ms + interval_seconds * 1000 > now_ms:
        return candles[:-1]
    return candles


def compute_heikin_ashi(candles: List[Dict]) -> List[Dict]:
    """
    Compute Heikin Ashi candles from raw OHLCV data.

    HA formulas:
      HA_Close = (Open + High + Low + Close) / 4
      HA_Open  = (prev_HA_Open + prev_HA_Close) / 2
      HA_High  = max(High, HA_Open, HA_Close)
      HA_Low   = min(Low, HA_Open, HA_Close)
    """
    if not candles:
        return []

    ha = []
    for i, c in enumerate(candles):
        ha_close = (c["open"] + c["high"] + c["low"] + c["close"]) / 4.0

        if i == 0:
            ha_open = (c["open"] + c["close"]) / 2.0
        else:
            ha_open = (ha[i - 1]["ha_open"] + ha[i - 1]["ha_close"]) / 2.0

        ha_high = max(c["high"], ha_open, ha_close)
        ha_low = min(c["low"], ha_open, ha_close)

        ha.append({
            "open_time": c["open_time"],
            "ha_open": ha_open,
            "ha_high": ha_high,
            "ha_low": ha_low,
            "ha_close": ha_close,
            "color": "GREEN" if ha_close >= ha_open else "RED",
            "upper_wick": ha_high - max(ha_open, ha_close),
            "lower_wick": min(ha_open, ha_close) - ha_low,
            "body": abs(ha_close - ha_open),
            "raw_close": c["close"],
            "volume": c.get("volume", 0),
        })

    return ha


def get_trend(ha_candles: List[Dict], lookback: int = 3) -> str:
    """
    Determine trend from the last N Heikin Ashi candles.
    Returns 'BULLISH', 'BEARISH', or 'NEUTRAL'.
    """
    if len(ha_candles) < lookback:
        return "NEUTRAL"

    recent = ha_candles[-lookback:]
    colors = [c["color"] for c in recent]

    green_count = colors.count("GREEN")
    red_count = colors.count("RED")

    if green_count == lookback:
        last = ha_candles[-1]
        # Strong bull: no lower wick on latest candle
        if last["lower_wick"] < last["body"] * 0.1:
            return "STRONG_BULLISH"
        return "BULLISH"
    elif red_count == lookback:
        last = ha_candles[-1]
        # Strong bear: no upper wick on latest candle
        if last["upper_wick"] < last["body"] * 0.1:
            return "STRONG_BEARISH"
        return "BEARISH"
    elif green_count > red_count:
        return "BULLISH"
    elif red_count > green_count:
        return "BEARISH"
    return "NEUTRAL"


def get_candle_color(ha_candles: List[Dict]) -> str:
    """Return the color of the most recent HA candle."""
    if not ha_candles:
        return "NEUTRAL"
    return ha_candles[-1]["color"]


def count_consecutive_opposite(
    ha_candles: List[Dict],
    position_side: str,
    min_body_pct: float = 0.0008,
) -> int:
    """
    Count consecutive meaningful opposing HA candles from the end.
    Skips doji/tiny candles (body < min_body_pct of price) — those are
    consolidation noise, not real reversals.

    position_side: 'LONG' or 'SHORT'
    min_body_pct:  minimum body as fraction of raw close (default 0.08%)
    Returns: count of consecutive qualifying opposing candles
    """
    if not ha_candles:
        return 0

    opposite_color = "RED" if position_side == "LONG" else "GREEN"
    count = 0

    for candle in reversed(ha_candles):
        price = candle.get("raw_close", 0) or 1
        min_body = price * min_body_pct
        is_opposing = candle["color"] == opposite_color
        has_real_body = candle["body"] >= min_body
        if is_opposing and has_real_body:
            count += 1
        else:
            break

    return count


def detect_reversal(ha_candles: List[Dict], current_side: str) -> bool:
    """
    Detect a solid 6h trend reversal (not just a wick).
    Returns True if a genuine reversal is confirmed.
    """
    if len(ha_candles) < 2:
        return False

    last = ha_candles[-1]
    prev = ha_candles[-2]

    if current_side == "LONG":
        # Reversal: last candle is RED and prev was GREEN
        if last["color"] == "RED" and prev["color"] == "GREEN":
            # Confirm it's a real body, not a tiny doji
            if last["body"] > last["upper_wick"] * 0.5:
                return True
    elif current_side == "SHORT":
        if last["color"] == "GREEN" and prev["color"] == "RED":
            if last["body"] > last["lower_wick"] * 0.5:
                return True

    return False
