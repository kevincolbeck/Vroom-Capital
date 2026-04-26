"""
Zone System
Divides Bitcoin price into $1,000 zones and tracks zone state.
Implements: zone confirmation, zone cooldowns, second-break rules.
"""
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple
from loguru import logger


def get_zone_key(price: float, zone_size: float = 1000.0) -> str:
    """Return the zone key for a given price. e.g. 94350 -> '$94k'"""
    zone_base = int(price // zone_size) * int(zone_size)
    return f"${zone_base // 1000}k"


def get_zone_bounds(price: float, zone_size: float = 1000.0) -> Tuple[float, float]:
    """Return (lower, upper) bounds of the current zone."""
    lower = int(price // zone_size) * zone_size
    upper = lower + zone_size
    return lower, upper


def get_zone_position(price: float, zone_size: float = 1000.0) -> str:
    """
    Classify where in the zone the price sits.
    Returns 'TOP', 'BOTTOM', or 'MID'.
    """
    lower, upper = get_zone_bounds(price, zone_size)
    zone_range = upper - lower
    relative = (price - lower) / zone_range

    if relative >= 0.80:
        return "TOP"
    elif relative <= 0.20:
        return "BOTTOM"
    return "MID"


def is_whole_number_break(price: float, prev_price: float, zone_size: float = 1000.0) -> Optional[str]:
    """
    Detect if price just crossed a whole number boundary.
    Returns 'UP' or 'DOWN' if crossed, None otherwise.
    """
    current_zone = int(price // zone_size)
    prev_zone = int(prev_price // zone_size)

    if current_zone > prev_zone:
        return "UP"
    elif current_zone < prev_zone:
        return "DOWN"
    return None


class ZoneTracker:
    """
    Tracks zone visits, signal counts, and cooldowns.
    Implements the "throw the dwarf" second-break rule.
    """

    def __init__(self, zone_size: float = 1000.0, cooldown_minutes: int = 120):
        self.zone_size = zone_size
        self.cooldown_minutes = cooldown_minutes
        # zone_key -> {direction -> {count, last_signal, cooldown_until, first_break_direction}}
        self._zone_state: Dict[str, Dict] = {}
        # Track first breaks for second-break rule
        self._first_breaks: Dict[str, Dict] = {}

    def get_state(self, zone_key: str) -> Dict:
        if zone_key not in self._zone_state:
            self._zone_state[zone_key] = {}
        return self._zone_state[zone_key]

    def record_first_break(self, zone_key: str, direction: str):
        """Record the first break of a whole number for the dwarf rule."""
        self._first_breaks[zone_key] = {
            "direction": direction,
            "time": datetime.utcnow(),
        }
        logger.info(f"Zone {zone_key}: First break {direction} — waiting for confirmation (throwing the dwarf)")

    def has_had_first_break(self, zone_key: str, direction: str, timeout_minutes: int = 60) -> bool:
        """
        Check if this zone already had its first break in the given direction.
        This means we're now on the 'second break' — safe to trade.
        """
        if zone_key not in self._first_breaks:
            return False
        fb = self._first_breaks[zone_key]
        if fb["direction"] != direction:
            return False
        elapsed = (datetime.utcnow() - fb["time"]).total_seconds() / 60
        if elapsed > timeout_minutes:
            # First break timed out — reset
            del self._first_breaks[zone_key]
            return False
        return True

    def is_in_cooldown(self, zone_key: str, direction: str) -> bool:
        """Check if this zone+direction combination is in cooldown."""
        state = self.get_state(zone_key)
        dir_state = state.get(direction, {})
        cooldown_until = dir_state.get("cooldown_until")
        if cooldown_until and datetime.utcnow() < cooldown_until:
            return True
        return False

    def get_cooldown_remaining(self, zone_key: str, direction: str) -> float:
        """Return seconds remaining in cooldown, 0 if not in cooldown."""
        state = self.get_state(zone_key)
        dir_state = state.get(direction, {})
        cooldown_until = dir_state.get("cooldown_until")
        if cooldown_until and datetime.utcnow() < cooldown_until:
            return (cooldown_until - datetime.utcnow()).total_seconds()
        return 0.0

    def record_signal(self, zone_key: str, direction: str):
        """Record a signal for a zone+direction. Apply cooldown if needed."""
        state = self.get_state(zone_key)
        if direction not in state:
            state[direction] = {"count": 0, "last_signal": None, "cooldown_until": None}

        dir_state = state[direction]
        dir_state["count"] = dir_state.get("count", 0) + 1
        dir_state["last_signal"] = datetime.utcnow()

        # Apply cooldown after 2 consecutive signals at the same zone+direction
        if dir_state["count"] >= 2:
            dir_state["cooldown_until"] = datetime.utcnow() + timedelta(minutes=self.cooldown_minutes)
            logger.warning(f"Zone {zone_key} {direction}: Cooldown triggered for {self.cooldown_minutes}min")

    def reset_zone(self, zone_key: str):
        """Reset zone state when price leaves the zone."""
        if zone_key in self._zone_state:
            del self._zone_state[zone_key]
        logger.debug(f"Zone {zone_key} reset — price left zone")

    def get_signal_count(self, zone_key: str, direction: str) -> int:
        """Get number of signals for a zone+direction."""
        state = self.get_state(zone_key)
        return state.get(direction, {}).get("count", 0)

    def check_zone_position_bias(self, price: float) -> Optional[str]:
        """
        Return trade bias based on where price is in the zone.
        TOP of zone -> short bias
        BOTTOM of zone -> long bias
        MID -> no bias
        """
        pos = get_zone_position(price, self.zone_size)
        if pos == "TOP":
            return "SHORT"
        elif pos == "BOTTOM":
            return "LONG"
        return None

    def to_dict(self) -> Dict:
        """Serialize zone tracker state."""
        result = {}
        for zone_key, dirs in self._zone_state.items():
            result[zone_key] = {}
            for direction, state in dirs.items():
                result[zone_key][direction] = {
                    "count": state.get("count", 0),
                    "last_signal": state.get("last_signal").isoformat() if state.get("last_signal") else None,
                    "cooldown_until": state.get("cooldown_until").isoformat() if state.get("cooldown_until") else None,
                    "in_cooldown": self.is_in_cooldown(zone_key, direction),
                }
        return result
