"""
Liquidation Monitor
Tracks open interest and real liquidation zones using OKX data plus a
synthetic leverage-distribution model.

Two data sources are merged:
  1. OKX liquidation orders — actual liquidation events, last ~24h
  2. Synthetic clusters     — leverage-math estimate (10x/25x/50x/75x/100x distribution)

Historical events (40% weight) validate which synthetic levels are realistic.
Synthetic forward estimates (60% weight) are the primary forward-looking signal.

Liquidation clusters are the price levels price is "magnetically attracted to"
because market makers profit from triggering them.
"""
import asyncio
import time
import httpx
from typing import Dict, List, Optional, Tuple
from loguru import logger


class LiquidationMonitor:

    SYMBOL        = "BTCUSDT"
    CACHE_SECONDS = 300  # 5-minute cache — positions don't change that fast

    # Crowd thresholds (used when L/S ratio data is available)
    CROWD_LONG_THRESHOLD  = 0.57   # >57% long = longs overcrowded
    CROWD_SHORT_THRESHOLD = 0.43   # <43% long = shorts overcrowded

    # Leverage distribution assumption for synthetic clusters (retail-weighted)
    LEVERAGE_DIST = [
        (10,  0.25),
        (25,  0.35),
        (50,  0.25),
        (75,  0.10),
        (100, 0.05),
    ]

    def __init__(self):
        self._cache: Optional[Dict] = None
        self._cache_time: float = 0

    # ─────────────────────────────────────────────────────────────────
    # Data Fetching
    # ─────────────────────────────────────────────────────────────────

    async def _fetch_okx_liquidations(self, client: httpx.AsyncClient) -> List[Dict]:
        """OKX BTC perpetual liquidation events. Public endpoint, no auth."""
        try:
            resp = await client.get(
                "https://www.okx.com/api/v5/public/liquidation-orders",
                params={"instType": "SWAP", "uly": "BTC-USD", "state": "filled", "limit": "100"},
            )
            data = resp.json()
            orders = []
            for item in (data.get("data") or []):
                for detail in (item.get("details") or []):
                    orders.append(detail)
            return orders
        except Exception as e:
            logger.debug(f"OKX liquidations failed: {e}")
            return []

    async def _fetch_okx_oi(self, client: httpx.AsyncClient) -> Optional[float]:
        """OKX BTC perpetual open interest in BTC."""
        try:
            resp = await client.get(
                "https://www.okx.com/api/v5/public/open-interest",
                params={"instType": "SWAP", "instId": "BTC-USDT-SWAP"},
            )
            data = resp.json()
            item = (data.get("data") or [{}])[0]
            oi_ccy = float(item.get("oiCcy", 0) or 0)
            return oi_ccy if oi_ccy > 0 else None
        except Exception as e:
            logger.debug(f"OKX OI failed: {e}")
            return None

    # ─────────────────────────────────────────────────────────────────
    # Analysis
    # ─────────────────────────────────────────────────────────────────

    def _compute_oi_trend(self, oi_history: Optional[List]) -> str:
        """OI trend over past 6h: RISING | FALLING | FLAT."""
        if not oi_history or len(oi_history) < 2:
            return "FLAT"
        try:
            first = float(oi_history[0]["sumOpenInterest"])
            last  = float(oi_history[-1]["sumOpenInterest"])
            change_pct = (last - first) / first * 100
            if change_pct > 2.0:
                return "RISING"
            elif change_pct < -2.0:
                return "FALLING"
            return "FLAT"
        except Exception:
            return "FLAT"

    def _cluster_liquidations(
        self,
        orders: List[Dict],
        source: str,
        bin_size: float = 500.0,
    ) -> Dict[float, float]:
        """
        Cluster liquidation events into $500 price bands.
        Returns {band_price: total_usd_liquidated}.
        """
        bands: Dict[float, float] = {}
        for order in (orders or []):
            try:
                if source == "binance":
                    # Fields: price/averagePrice, executedQty/origQty (in BTC)
                    price = float(order.get("price") or order.get("averagePrice") or 0)
                    qty   = float(order.get("executedQty") or order.get("origQty") or 0)
                elif source == "okx":
                    # Fields: bkPx (bankruptcy price), sz (contracts, 0.01 BTC each for USDT perp)
                    price = float(order.get("bkPx") or order.get("px") or 0)
                    qty   = float(order.get("sz") or 0) * 0.01
                else:
                    continue
                if price > 0 and qty > 0:
                    band = round(price / bin_size) * bin_size
                    bands[band] = bands.get(band, 0) + price * qty
            except Exception:
                continue
        return bands

    def _compute_synthetic_clusters(
        self,
        current_price: float,
        oi_usd: Optional[float],
        ls_ratio: Optional[Dict],
        bin_size: float = 500.0,
    ) -> Dict[float, float]:
        """
        Estimate where liquidations stack up using leverage-distribution math.

        For a long position at 25x, liquidation occurs at ~4% below entry.
        For a short at 25x, liquidation occurs at ~4% above entry.
        We distribute OI across the standard retail leverage mix and compute
        the resulting liquidation density at each price level.
        """
        if not oi_usd or oi_usd <= 0:
            return {}

        long_pct  = (ls_ratio.get("long_pct", 0.5) if ls_ratio else 0.5)
        long_oi   = oi_usd * long_pct
        short_oi  = oi_usd * (1.0 - long_pct)

        bands: Dict[float, float] = {}
        for lev, weight in self.LEVERAGE_DIST:
            # Longs get liquidated below current price
            long_liq = current_price * (1.0 - 1.0 / lev)
            band = round(long_liq / bin_size) * bin_size
            bands[band] = bands.get(band, 0) + long_oi * weight

            # Shorts get liquidated above current price
            short_liq = current_price * (1.0 + 1.0 / lev)
            band = round(short_liq / bin_size) * bin_size
            bands[band] = bands.get(band, 0) + short_oi * weight

        return bands

    def compute_liquidation_zones(
        self,
        current_price: float,
        ls_ratio: Optional[Dict],
        force_orders: Optional[List] = None,
        okx_liquidations: Optional[List] = None,
        synthetic: Optional[Dict[float, float]] = None,
        window_pct: float = 0.10,
    ) -> Dict:
        """
        Merge real liquidation events + synthetic estimates into weighted cluster map.
        Returns the top-3 levels above and below price with estimated USD at risk.

        Historical events get 40% weight (validates which levels are realistic).
        Synthetic forward estimates get 60% weight (primary forward-looking signal).
        """
        long_crowd  = False
        short_crowd = False
        if ls_ratio:
            long_pct = ls_ratio.get("long_pct", 0.5)
            if long_pct > self.CROWD_LONG_THRESHOLD:
                long_crowd = True
            elif long_pct < self.CROWD_SHORT_THRESHOLD:
                short_crowd = True

        # Cluster real events
        binance_clusters = self._cluster_liquidations(force_orders or [], "binance")
        okx_clusters     = self._cluster_liquidations(okx_liquidations or [], "okx")

        # Merge historical into combined map (40% weight)
        combined: Dict[float, float] = {}
        for band, usd in binance_clusters.items():
            combined[band] = combined.get(band, 0) + usd * 0.4
        for band, usd in okx_clusters.items():
            combined[band] = combined.get(band, 0) + usd * 0.4

        # Add synthetic forward estimates (60% weight)
        for band, usd in (synthetic or {}).items():
            combined[band] = combined.get(band, 0) + usd * 0.6

        # Filter to window and split above/below current price
        price_lo = current_price * (1 - window_pct)
        price_hi = current_price * (1 + window_pct)

        below = [
            {
                "price":         band,
                "label":         f"${band/1000:.1f}k",
                "direction":     "BELOW",
                "dist_pct":      round((current_price - band) / current_price * 100, 2),
                "estimated_usd": round(usd),
            }
            for band, usd in combined.items()
            if price_lo <= band < current_price and usd > 0
        ]
        above = [
            {
                "price":         band,
                "label":         f"${band/1000:.1f}k",
                "direction":     "ABOVE",
                "dist_pct":      round((band - current_price) / current_price * 100, 2),
                "estimated_usd": round(usd),
            }
            for band, usd in combined.items()
            if current_price < band <= price_hi and usd > 0
        ]

        below = sorted(below, key=lambda x: x["dist_pct"])  # nearest first
        above = sorted(above, key=lambda x: x["dist_pct"])

        # Fallback to round numbers if no data at all
        if not combined:
            zone_size = 1000.0
            base = int(current_price // zone_size) * zone_size
            below = [{"price": base, "label": f"${int(base/1000)}k", "direction": "BELOW",
                      "dist_pct": round((current_price - base) / current_price * 100, 2),
                      "estimated_usd": 0}]
            above = [{"price": base + zone_size, "label": f"${int((base+zone_size)/1000)}k",
                      "direction": "ABOVE",
                      "dist_pct": round((base + zone_size - current_price) / current_price * 100, 2),
                      "estimated_usd": 0}]

        data_sources = []
        if binance_clusters:
            data_sources.append("binance_forceorders")
        if okx_clusters:
            data_sources.append("okx_liquidations")
        if synthetic:
            data_sources.append("synthetic")

        return {
            "long_crowd":               long_crowd,
            "short_crowd":              short_crowd,
            "nearest_long_liquidation": below[0] if below else None,
            "nearest_short_liquidation": above[0] if above else None,
            "levels_below":             below[:3],
            "levels_above":             above[:3],
            "data_sources":             data_sources,
        }

    def get_trade_context(
        self,
        direction: str,
        current_price: float,
        analysis: Dict,
    ) -> Tuple[float, str]:
        """
        Return (confidence_modifier, description) for the trade direction.
        Crowd on the OTHER side = we squeeze them into liquidation = good.
        Crowd on OUR side = caution, cascade risk if we're wrong.
        """
        ls_ratio  = analysis.get("ls_ratio")
        liq_zones = analysis.get("liquidation_zones", {})

        if ls_ratio is None:
            return 0.0, "Positioning data unavailable"

        long_crowd  = liq_zones.get("long_crowd", False)
        short_crowd = liq_zones.get("short_crowd", False)
        long_pct    = ls_ratio.get("long_pct", 0.5)
        short_pct   = ls_ratio.get("short_pct", 0.5)

        def _usd_str(zone: Optional[Dict]) -> str:
            if not zone:
                return ""
            usd = zone.get("estimated_usd", 0)
            return f"~${usd/1e6:.0f}M" if usd >= 1_000_000 else ""

        if direction == "SHORT":
            if long_crowd:
                target  = liq_zones.get("nearest_long_liquidation")
                usd_str = _usd_str(target)
                label   = f"{target['label']} {usd_str}".strip() if target else ""
                return 8.0, (
                    f"Longs overcrowded ({long_pct*100:.0f}% long) — "
                    f"SHORT squeezes them → cascade target {label}"
                )
            elif short_crowd:
                return -5.0, f"Shorts already overcrowded ({short_pct*100:.0f}% short) — crowded trade"
            return 0.0, f"Positioning neutral ({long_pct*100:.0f}% long / {short_pct*100:.0f}% short)"

        else:  # LONG
            if short_crowd:
                target  = liq_zones.get("nearest_short_liquidation")
                usd_str = _usd_str(target)
                label   = f"{target['label']} {usd_str}".strip() if target else ""
                return 8.0, (
                    f"Shorts overcrowded ({short_pct*100:.0f}% short) — "
                    f"LONG squeezes them → cascade target {label}"
                )
            elif long_crowd:
                return -5.0, f"Longs already overcrowded ({long_pct*100:.0f}% long) — crowded trade"
            return 0.0, f"Positioning neutral ({long_pct*100:.0f}% long / {short_pct*100:.0f}% short)"

    # ─────────────────────────────────────────────────────────────────
    # Main Fetch
    # ─────────────────────────────────────────────────────────────────

    async def fetch_all(self, current_price: float) -> Dict:
        """
        Fetch liquidation data from OKX and return unified analysis.
        Results cached for 5 minutes.
        """
        if self._cache and (time.time() - self._cache_time) < self.CACHE_SECONDS:
            cached = dict(self._cache)
            synthetic = self._compute_synthetic_clusters(
                current_price,
                cached.get("total_oi_usd"),
                cached.get("ls_ratio"),
            )
            cached["liquidation_zones"] = self.compute_liquidation_zones(
                current_price,
                cached.get("ls_ratio"),
                okx_liquidations=cached.get("_cached_okx_liqs", []),
                synthetic=synthetic,
            )
            return cached

        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                results = await asyncio.gather(
                    self._fetch_okx_liquidations(client),
                    self._fetch_okx_oi(client),
                    return_exceptions=True,
                )

            def safe(r):
                return None if isinstance(r, Exception) else r

            okx_liqs, okx_oi_btc = [safe(r) for r in results]

            okx_oi_usd = (float(okx_oi_btc) * current_price) if okx_oi_btc else 0.0
            total_oi_usd = okx_oi_usd or None

            synthetic = self._compute_synthetic_clusters(current_price, total_oi_usd, None)
            liq_zones = self.compute_liquidation_zones(
                current_price, None,
                okx_liquidations=okx_liqs or [],
                synthetic=synthetic,
            )

            result = {
                "total_oi_usd":      total_oi_usd,
                "oi_breakdown":      {"okx": round(okx_oi_usd)},
                "oi_trend":          "FLAT",
                "ls_ratio":          None,
                "liquidation_zones": liq_zones,
                "available":         total_oi_usd is not None,
                "_cached_okx_liqs":  okx_liqs or [],
            }

            self._cache      = result
            self._cache_time = time.time()
            return result

        except Exception as e:
            logger.warning(f"Liquidation monitor fetch failed: {e}")
            return {
                "total_oi_usd":      None,
                "oi_breakdown":      {},
                "oi_trend":          "FLAT",
                "ls_ratio":          None,
                "liquidation_zones": self.compute_liquidation_zones(current_price, None),
                "available":         False,
            }
