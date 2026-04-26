"""
Liquidation Monitor
Tracks open interest, long/short positioning, and estimates liquidation zones
using publicly available Binance futures data.

Liquidation clusters form at round numbers where leveraged positions are dense.
We use OI + long/short ratio to determine which side is overcrowded and
estimate the nearest high-value liquidation target for trade sizing.
"""
import asyncio
import time
import httpx
from typing import Dict, List, Optional, Tuple
from loguru import logger


class LiquidationMonitor:

    BINANCE_URL = "https://fapi.binance.com"
    SYMBOL = "BTCUSDT"
    CACHE_SECONDS = 300  # Re-fetch every 5 minutes

    # Crowd thresholds for long/short ratio
    CROWD_LONG_THRESHOLD = 0.57   # >57% long = longs overcrowded
    CROWD_SHORT_THRESHOLD = 0.43  # <43% long = shorts overcrowded

    def __init__(self):
        self._cache: Optional[Dict] = None
        self._cache_time: float = 0

    # ─────────────────────────────────────────────────────────────────
    # Data Fetching
    # ─────────────────────────────────────────────────────────────────

    async def _fetch_open_interest(self, client: httpx.AsyncClient) -> Optional[float]:
        try:
            resp = await client.get(
                f"{self.BINANCE_URL}/fapi/v1/openInterest",
                params={"symbol": self.SYMBOL},
            )
            return float(resp.json()["openInterest"])
        except Exception as e:
            logger.debug(f"OI fetch failed: {e}")
            return None

    async def _fetch_long_short_ratio(self, client: httpx.AsyncClient) -> Optional[Dict]:
        """Global long/short account ratio — tracks which side retail is on."""
        try:
            resp = await client.get(
                f"{self.BINANCE_URL}/futures/data/globalLongShortAccountRatio",
                params={"symbol": self.SYMBOL, "period": "1h", "limit": 4},
            )
            data = resp.json()
            if not data:
                return None
            latest = data[-1]
            prev = data[-4] if len(data) >= 4 else data[0]
            long_pct = float(latest["longAccount"])
            prev_long_pct = float(prev["longAccount"])
            return {
                "long_pct": long_pct,
                "short_pct": float(latest["shortAccount"]),
                "ratio": float(latest["longShortRatio"]),
                "trend": "MORE_LONGS" if long_pct > prev_long_pct else "MORE_SHORTS",
            }
        except Exception as e:
            logger.debug(f"Long/short ratio fetch failed: {e}")
            return None

    async def _fetch_oi_history(self, client: httpx.AsyncClient) -> Optional[List]:
        """OI over last 6 hours to detect leverage build-up or unwind."""
        try:
            resp = await client.get(
                f"{self.BINANCE_URL}/futures/data/openInterestHist",
                params={"symbol": self.SYMBOL, "period": "1h", "limit": 6},
            )
            return resp.json()
        except Exception as e:
            logger.debug(f"OI history fetch failed: {e}")
            return None

    async def _fetch_top_trader_ratio(self, client: httpx.AsyncClient) -> Optional[Dict]:
        """Top trader position ratio — 'smart money' positioning."""
        try:
            resp = await client.get(
                f"{self.BINANCE_URL}/futures/data/topLongShortPositionRatio",
                params={"symbol": self.SYMBOL, "period": "1h", "limit": 1},
            )
            data = resp.json()
            if data:
                d = data[-1]
                return {
                    "long_pct": float(d["longAccount"]),
                    "short_pct": float(d["shortAccount"]),
                    "ratio": float(d["longShortRatio"]),
                }
        except Exception as e:
            logger.debug(f"Top trader ratio fetch failed: {e}")
        return None

    # ─────────────────────────────────────────────────────────────────
    # Analysis
    # ─────────────────────────────────────────────────────────────────

    def _compute_oi_trend(self, oi_history: Optional[List]) -> str:
        """Return OI trend over past 6h: RISING | FALLING | FLAT."""
        if not oi_history or len(oi_history) < 2:
            return "FLAT"
        try:
            first = float(oi_history[0]["sumOpenInterest"])
            last = float(oi_history[-1]["sumOpenInterest"])
            change_pct = (last - first) / first * 100
            if change_pct > 2.0:
                return "RISING"
            elif change_pct < -2.0:
                return "FALLING"
            return "FLAT"
        except Exception:
            return "FLAT"

    def compute_liquidation_zones(
        self,
        current_price: float,
        ls_ratio: Optional[Dict],
        window_pct: float = 0.08,
    ) -> Dict:
        """
        Estimate liquidation clusters near current price.

        Logic:
        - Round numbers ($1k increments) are where stop losses and liquidations
          concentrate — they are the magnetic targets for price.
        - If longs are overcrowded, the highest-density LONG liquidation zone
          is the most likely SHORT target (cascade into it).
        - If shorts are overcrowded, the SHORT liquidation zone above is the target.
        """
        long_crowd = False
        short_crowd = False

        if ls_ratio:
            long_pct = ls_ratio.get("long_pct", 0.5)
            if long_pct > self.CROWD_LONG_THRESHOLD:
                long_crowd = True
            elif long_pct < self.CROWD_SHORT_THRESHOLD:
                short_crowd = True

        # Find round number levels within ±8% of current price
        zone_size = 1000.0
        base_zone = int(current_price // zone_size) * zone_size
        levels = []
        for offset in range(-8, 9):
            lvl = base_zone + offset * zone_size
            dist_pct = abs(lvl - current_price) / current_price * 100
            if dist_pct > window_pct * 100 or dist_pct < 0.1:
                continue
            direction = "BELOW" if lvl < current_price else "ABOVE"
            levels.append({
                "price": lvl,
                "label": f"${int(lvl // 1000)}k",
                "direction": direction,
                "dist_pct": round(dist_pct, 2),
            })

        below = sorted([l for l in levels if l["direction"] == "BELOW"],
                       key=lambda x: x["dist_pct"])
        above = sorted([l for l in levels if l["direction"] == "ABOVE"],
                       key=lambda x: x["dist_pct"])

        # The nearest clusters are the most likely cascade targets
        nearest_long_liq = below[0] if below else None   # longs get liq'd below
        nearest_short_liq = above[0] if above else None  # shorts get liq'd above

        return {
            "long_crowd": long_crowd,
            "short_crowd": short_crowd,
            "nearest_long_liquidation": nearest_long_liq,
            "nearest_short_liquidation": nearest_short_liq,
            "levels_below": below[:3],
            "levels_above": above[:3],
        }

    def get_trade_context(
        self,
        direction: str,
        current_price: float,
        analysis: Dict,
    ) -> Tuple[float, str]:
        """
        Return (confidence_modifier, description) for the trade direction.

        Crowd on the OTHER side = good for us (we squeeze them into liquidation).
        Crowd on OUR side = caution (if our position loses, we cascade too).
        """
        ls_ratio = analysis.get("ls_ratio")
        liq_zones = analysis.get("liquidation_zones", {})

        if ls_ratio is None:
            return 0.0, "Positioning data unavailable"

        long_crowd = liq_zones.get("long_crowd", False)
        short_crowd = liq_zones.get("short_crowd", False)

        long_pct = ls_ratio.get("long_pct", 0.5)
        short_pct = ls_ratio.get("short_pct", 0.5)

        if direction == "SHORT":
            if long_crowd:
                target = liq_zones.get("nearest_long_liquidation")
                target_str = f"→ cascade target {target['label']}" if target else ""
                return 8.0, (
                    f"Longs overcrowded ({long_pct*100:.0f}% long) — SHORT squeezes them {target_str}"
                )
            elif short_crowd:
                return -5.0, f"Shorts already overcrowded ({short_pct*100:.0f}% short) — crowded trade"
            return 0.0, f"Positioning neutral ({long_pct*100:.0f}% long / {short_pct*100:.0f}% short)"

        else:  # LONG
            if short_crowd:
                target = liq_zones.get("nearest_short_liquidation")
                target_str = f"→ cascade target {target['label']}" if target else ""
                return 8.0, (
                    f"Shorts overcrowded ({short_pct*100:.0f}% short) — LONG squeezes them {target_str}"
                )
            elif long_crowd:
                return -5.0, f"Longs already overcrowded ({long_pct*100:.0f}% long) — crowded trade"
            return 0.0, f"Positioning neutral ({long_pct*100:.0f}% long / {short_pct*100:.0f}% short)"

    # ─────────────────────────────────────────────────────────────────
    # Main Fetch
    # ─────────────────────────────────────────────────────────────────

    async def fetch_all(self, current_price: float) -> Dict:
        """Fetch all positioning data and return unified analysis."""
        if self._cache and (time.time() - self._cache_time) < self.CACHE_SECONDS:
            # Recompute liquidation zones with fresh price but cached ratios
            cached = dict(self._cache)
            cached["liquidation_zones"] = self.compute_liquidation_zones(
                current_price, cached.get("ls_ratio")
            )
            return cached

        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                oi_raw, ls_ratio, oi_hist, top_ratio = await asyncio.gather(
                    self._fetch_open_interest(client),
                    self._fetch_long_short_ratio(client),
                    self._fetch_oi_history(client),
                    self._fetch_top_trader_ratio(client),
                    return_exceptions=True,
                )

            if isinstance(oi_raw, Exception):
                oi_raw = None
            if isinstance(ls_ratio, Exception):
                ls_ratio = None
            if isinstance(oi_hist, Exception):
                oi_hist = None
            if isinstance(top_ratio, Exception):
                top_ratio = None

            oi_usd = (oi_raw * current_price) if oi_raw else None
            oi_trend = self._compute_oi_trend(oi_hist if not isinstance(oi_hist, Exception) else None)
            liq_zones = self.compute_liquidation_zones(current_price, ls_ratio)

            result = {
                "open_interest_btc": oi_raw,
                "open_interest_usd": oi_usd,
                "oi_trend": oi_trend,
                "ls_ratio": ls_ratio,
                "top_trader_ratio": top_ratio,
                "liquidation_zones": liq_zones,
                "available": ls_ratio is not None,
            }

            self._cache = result
            self._cache_time = time.time()
            return result

        except Exception as e:
            logger.warning(f"Liquidation monitor fetch failed: {e}")
            return {
                "open_interest_btc": None,
                "open_interest_usd": None,
                "oi_trend": "FLAT",
                "ls_ratio": None,
                "top_trader_ratio": None,
                "liquidation_zones": self.compute_liquidation_zones(current_price, None),
                "available": False,
            }
