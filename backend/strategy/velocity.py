"""
Velocity Filter
Measures how fast price has moved in the last 2 hours using raw close prices.
Blocks trades when momentum is too strong (chasing / falling knife).
"""
from typing import List, Dict, Tuple, Optional
from loguru import logger


def compute_velocity(candles: List[Dict], window_hours: int = 2) -> Dict:
    """
    Compute price velocity over the past N hours using raw 1h candles.

    Returns:
        {
          "pct_change": float,    # % change over the window
          "direction": "UP" | "DOWN" | "FLAT",
          "start_price": float,
          "end_price": float,
          "high_in_window": float,
          "low_in_window": float,
        }
    """
    if len(candles) < window_hours + 1:
        return {
            "pct_change": 0.0,
            "direction": "FLAT",
            "start_price": 0.0,
            "end_price": 0.0,
            "high_in_window": 0.0,
            "low_in_window": 0.0,
        }

    # Use the last window_hours + 1 candles to get the range
    window_candles = candles[-(window_hours + 1):]

    start_price = window_candles[0]["close"]
    end_price = window_candles[-1]["close"]

    highs = [c["high"] for c in window_candles]
    lows = [c["low"] for c in window_candles]

    pct_change = ((end_price - start_price) / start_price) * 100.0

    if pct_change > 0.05:
        direction = "UP"
    elif pct_change < -0.05:
        direction = "DOWN"
    else:
        direction = "FLAT"

    return {
        "pct_change": pct_change,
        "direction": direction,
        "start_price": start_price,
        "end_price": end_price,
        "high_in_window": max(highs),
        "low_in_window": min(lows),
    }


def check_velocity_filter(
    candles: List[Dict],
    direction: str,
    threshold_pct: float = 1.5,
    window_hours: int = 2,
) -> Tuple[bool, str, Dict]:
    """
    Check if velocity is too high to enter a trade.

    Args:
        candles: List of 1h raw OHLCV candles (most recent last)
        direction: 'LONG' or 'SHORT'
        threshold_pct: Block if abs(velocity) > this %
        window_hours: Number of hours to measure

    Returns:
        (is_allowed: bool, reason: str, velocity_data: dict)
    """
    velocity = compute_velocity(candles, window_hours)
    pct = velocity["pct_change"]
    abs_pct = abs(pct)

    if direction == "LONG" and pct < -threshold_pct:
        reason = (
            f"LONG blocked: Price dumped {abs(pct):.2f}% in last {window_hours}h "
            f"(threshold: {threshold_pct}%) — don't catch falling knives"
        )
        return False, reason, velocity

    if direction == "SHORT" and pct > threshold_pct:
        reason = (
            f"SHORT blocked: Price pumped {pct:.2f}% in last {window_hours}h "
            f"(threshold: {threshold_pct}%) — don't short into momentum"
        )
        return False, reason, velocity

    reason = f"Velocity filter: PASS ({pct:+.2f}% in {window_hours}h)"
    return True, reason, velocity
