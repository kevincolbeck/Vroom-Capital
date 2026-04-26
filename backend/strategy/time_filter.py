"""
Time-Based Market Manipulation Filter
Blocks/allows trade signals based on historical time patterns.
All times in EST (UTC-5 / UTC-4 during DST).
"""
from datetime import datetime
from typing import Tuple, Optional
import pytz

EST = pytz.timezone("America/New_York")

# ─────────────────────────────────────────────────────────────────
# Time filter rules (hour in EST, 24-hour format)
# ─────────────────────────────────────────────────────────────────

# Hours where LONG is blocked (dump hours / high risk)
LONG_BLOCKED_HOURS = {0, 1}          # 12:00 - 2:00 AM EST (witching hour)
LONG_PARTIAL_BLOCK = {4}             # 4:00-5:00 AM EST (transition)

# Hours where SHORT is blocked (pump hours)
SHORT_BLOCKED_HOURS = {
    6,    # 6:00 - 7:00 AM EST
    9,    # 9:00 - 10:00 AM EST (stock open)
    10,   # 10:00 - 11:00 AM EST
    11,   # 11:00 - 12:00 PM
    12,   # 12:00 - 1:00 PM
    16,   # 4:00 - 5:00 PM
    20,   # 8:00 - 9:00 PM
}

# Hours where BOTH are blocked (high risk transition)
BOTH_BLOCKED_HOURS = {5}             # 5:00 AM EST


def get_est_hour() -> int:
    """Return current hour in EST."""
    return datetime.now(EST).hour


def get_est_datetime() -> datetime:
    """Return current datetime in EST."""
    return datetime.now(EST)


def check_time_filter(direction: str, hour_est: Optional[int] = None) -> Tuple[bool, str]:
    """
    Check if a trade direction is allowed at the current time.

    Returns: (is_allowed: bool, reason: str)
    """
    if hour_est is None:
        hour_est = get_est_hour()

    if hour_est in BOTH_BLOCKED_HOURS:
        return False, f"Both directions blocked at {hour_est}:00 EST (high-risk transition hour)"

    if direction == "LONG":
        if hour_est in LONG_BLOCKED_HOURS:
            return False, f"LONG blocked at {hour_est}:00 EST (witching hour — high dump risk)"
        if hour_est in LONG_PARTIAL_BLOCK:
            return False, f"LONG blocked at {hour_est}:00 EST (transition hour — unreliable)"

    elif direction == "SHORT":
        if hour_est in SHORT_BLOCKED_HOURS:
            return False, f"SHORT blocked at {hour_est}:00 EST (pump hour — momentum against shorts)"

    return True, "Time filter: PASS"


def get_time_context(hour_est: Optional[int] = None) -> dict:
    """Return context about the current time window."""
    if hour_est is None:
        hour_est = get_est_hour()

    long_blocked = hour_est in LONG_BLOCKED_HOURS or hour_est in LONG_PARTIAL_BLOCK or hour_est in BOTH_BLOCKED_HOURS
    short_blocked = hour_est in SHORT_BLOCKED_HOURS or hour_est in BOTH_BLOCKED_HOURS

    if hour_est in {0, 1}:
        label = "WITCHING_HOUR"
        risk_level = "EXTREME"
    elif hour_est in {5}:
        label = "TRANSITION_HIGH_RISK"
        risk_level = "HIGH"
    elif hour_est in {6, 7}:
        label = "MORNING_PUMP"
        risk_level = "LOW"
    elif hour_est in {9, 10, 11, 12, 13}:
        label = "US_SESSION_PUMP"
        risk_level = "LOW"
    elif hour_est == 16:
        label = "MARKET_CLOSE_PUMP"
        risk_level = "LOW"
    elif hour_est == 20:
        label = "ASIA_OPEN_PUMP"
        risk_level = "LOW"
    else:
        label = "NEUTRAL"
        risk_level = "NORMAL"

    return {
        "hour_est": hour_est,
        "label": label,
        "risk_level": risk_level,
        "long_blocked": long_blocked,
        "short_blocked": short_blocked,
        "both_blocked": hour_est in BOTH_BLOCKED_HOURS,
    }
