"""
Macro Calendar
Tracks FOMC meetings, CPI releases, and other macro events that affect Bitcoin.
"""
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
import pytz

UTC = pytz.utc
EST = pytz.timezone("America/New_York")

# ─────────────────────────────────────────────────────────────────
# CPI release dates — BLS publishes monthly around the 10th–15th
# Higher-than-expected = bearish, lower = bullish
# We apply caution on release day and day before
# ─────────────────────────────────────────────────────────────────
CPI_DATES = [
    # 2025
    date(2025, 1, 15),
    date(2025, 2, 12),
    date(2025, 3, 12),
    date(2025, 4, 10),
    date(2025, 5, 13),
    date(2025, 6, 11),
    date(2025, 7, 15),
    date(2025, 8, 12),
    date(2025, 9, 10),
    date(2025, 10, 14),
    date(2025, 11, 13),
    date(2025, 12, 10),
    # 2026
    date(2026, 1, 14),
    date(2026, 2, 11),
    date(2026, 3, 11),
    date(2026, 4, 14),
    date(2026, 5, 13),
    date(2026, 6, 10),
    date(2026, 7, 14),
    date(2026, 8, 12),
    date(2026, 9, 9),
    date(2026, 10, 13),
    date(2026, 11, 12),
    date(2026, 12, 9),
]

# ─────────────────────────────────────────────────────────────────
# Known upcoming FOMC meeting dates (update quarterly)
# Format: (year, month, day) — the announcement day
# ─────────────────────────────────────────────────────────────────
FOMC_DATES = [
    date(2025, 1, 29),
    date(2025, 3, 19),
    date(2025, 5, 7),
    date(2025, 6, 18),
    date(2025, 7, 30),
    date(2025, 9, 17),
    date(2025, 10, 29),
    date(2025, 12, 10),
    # 2026
    date(2026, 1, 28),
    date(2026, 3, 18),
    date(2026, 5, 6),
    date(2026, 6, 17),
    date(2026, 7, 29),
    date(2026, 9, 16),
    date(2026, 10, 28),
    date(2026, 12, 9),
]

# Quad witching dates (3rd Friday of Mar, Jun, Sep, Dec)
QUAD_WITCHING_2025 = [
    date(2025, 3, 21),
    date(2025, 6, 20),
    date(2025, 9, 19),
    date(2025, 12, 19),
]
QUAD_WITCHING_2026 = [
    date(2026, 3, 20),
    date(2026, 6, 19),
    date(2026, 9, 18),
    date(2026, 12, 18),
]


class MacroCalendar:

    FOMC_CAUTION_DAYS = 7
    FOMC_AVOID_HOURS = 2  # Hours around announcement to avoid

    def get_next_fomc(self) -> Optional[date]:
        """Return the next upcoming FOMC date."""
        today = date.today()
        future = [d for d in FOMC_DATES if d >= today]
        return future[0] if future else None

    def get_days_to_fomc(self) -> Optional[int]:
        """Return days until next FOMC meeting."""
        next_fomc = self.get_next_fomc()
        if next_fomc is None:
            return None
        return (next_fomc - date.today()).days

    def is_fomc_day(self) -> bool:
        """Return True if today is a FOMC announcement day."""
        return date.today() in FOMC_DATES

    def is_fomc_window(self) -> bool:
        """
        Return True if we're within the 2-hour avoidance window around FOMC.
        (2 hours before 2pm EST through 2 hours after)
        """
        if not self.is_fomc_day():
            return False
        now_est = datetime.now(EST)
        # FOMC typically announces at 2pm EST
        fomc_hour = 14
        return fomc_hour - self.FOMC_AVOID_HOURS <= now_est.hour < fomc_hour + self.FOMC_AVOID_HOURS

    def get_fomc_risk_level(self) -> Tuple[str, str]:
        """
        Return (risk_level, description) based on proximity to FOMC.
        """
        days = self.get_days_to_fomc()

        if days is None:
            return "NORMAL", "No upcoming FOMC data"

        if self.is_fomc_window():
            return "EXTREME", "FOMC announcement window — NO TRADING"

        if days == 0:
            return "HIGH", "FOMC day — avoid trading, high volatility expected"
        elif days <= 1:
            return "HIGH", f"FOMC tomorrow — high volatility window, reduce size"
        elif days <= 3:
            return "MODERATE", f"FOMC in {days} days — moderate caution"
        elif days <= self.FOMC_CAUTION_DAYS:
            return "LOW", f"FOMC in {days} days — light caution, reduce aggression"
        else:
            return "NORMAL", f"FOMC in {days} days — no impact"

    def get_next_cpi(self) -> Optional[date]:
        """Return the next upcoming CPI release date."""
        today = date.today()
        future = [d for d in CPI_DATES if d >= today]
        return future[0] if future else None

    def get_days_to_cpi(self) -> Optional[int]:
        """Return days until next CPI release."""
        next_cpi = self.get_next_cpi()
        if next_cpi is None:
            return None
        return (next_cpi - date.today()).days

    def get_cpi_risk_level(self) -> Tuple[str, str]:
        """Return (risk_level, description) based on proximity to CPI release."""
        days = self.get_days_to_cpi()
        if days is None:
            return "NORMAL", "No upcoming CPI data"
        if days == 0:
            return "MODERATE", "CPI release day — expect volatility spike on print"
        elif days == 1:
            return "LOW", "CPI tomorrow — light pre-positioning caution"
        else:
            return "NORMAL", f"CPI in {days} days — no impact"

    def is_quad_witching(self) -> bool:
        """Check if today is near quad witching (±2 days)."""
        all_dates = QUAD_WITCHING_2025 + QUAD_WITCHING_2026
        today = date.today()
        for qw in all_dates:
            if abs((today - qw).days) <= 2:
                return True
        return False

    def get_macro_context(self) -> Dict:
        """Return full macro context for the signal engine."""
        fomc_risk, fomc_desc = self.get_fomc_risk_level()
        days_to_fomc = self.get_days_to_fomc()
        is_fomc_window = self.is_fomc_window()
        quad_witching = self.is_quad_witching()
        cpi_risk, cpi_desc = self.get_cpi_risk_level()
        days_to_cpi = self.get_days_to_cpi()

        # Determine position size modifier — FOMC takes priority over CPI
        if is_fomc_window:
            size_modifier = 0.0  # Hard stop — no trading during announcement
        elif fomc_risk == "HIGH":
            size_modifier = 0.5
        elif fomc_risk == "MODERATE":
            size_modifier = 0.75
        elif quad_witching:
            size_modifier = 0.75
        elif cpi_risk == "MODERATE":
            size_modifier = 0.75  # CPI day — reduce size
        elif cpi_risk == "LOW":
            size_modifier = 0.90  # Day before CPI — slight caution
        else:
            size_modifier = 1.0

        # Block description covers both FOMC and CPI
        block_description = fomc_desc
        if cpi_risk != "NORMAL" and fomc_risk == "NORMAL":
            block_description = cpi_desc

        return {
            "fomc_risk_level": fomc_risk,
            "fomc_description": fomc_desc,
            "days_to_fomc": days_to_fomc,
            "is_fomc_day": self.is_fomc_day(),
            "is_fomc_window": is_fomc_window,
            "is_quad_witching": quad_witching,
            "cpi_risk_level": cpi_risk,
            "cpi_description": cpi_desc,
            "days_to_cpi": days_to_cpi,
            "position_size_modifier": size_modifier,
            "should_trade": size_modifier > 0.0,
            "block_description": block_description,
        }
