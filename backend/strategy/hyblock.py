"""
Hyblock Capital API Integration
Fetches order book pressure, liquidation clusters, whale/retail positioning,
and cascade risk signals from the Hyblock Capital enterprise API.
"""
import asyncio
import time
from typing import Dict, List, Optional, Tuple
import httpx
from loguru import logger

from backend.config import settings

COIN = "BTC"
EXCHANGE = "binance_perp_stable"
DEPTH_LEVELS = [1, 2, 5, 10]
CACHE_TTL_SECONDS = 60


class HyblockMonitor:
    """
    Fetches and analyzes Hyblock Capital enterprise data.

    Signals produced:
    - OBI surface & depth slope (institutional bid/ask imbalance across depth)
    - Fragility score (thin-book contrarian warning)
    - Cascade risk level (LOW/MEDIUM/HIGH/CRITICAL)
    - Whale/retail delta sentiment
    - Top trader positioning (used as contrarian indicator)
    - Volume delta (buy vs sell flow)
    - Liquidation cluster proximity (price magnet targets)
    """

    BASE_URL = "https://api.hyblockcapital.com/v2"

    def __init__(self):
        self._cache: Dict[str, Tuple[float, Dict]] = {}

    # ─── Caching ─────────────────────────────────────────────────────────────

    def _get_cached(self, key: str) -> Optional[Dict]:
        entry = self._cache.get(key)
        if entry and (time.monotonic() - entry[0]) < CACHE_TTL_SECONDS:
            return entry[1]
        return None

    def _set_cached(self, key: str, data: Dict):
        self._cache[key] = (time.monotonic(), data)

    # ─── HTTP ─────────────────────────────────────────────────────────────────

    async def _fetch(
        self, client: httpx.AsyncClient, endpoint: str, params: Dict
    ) -> Dict:
        url = f"{self.BASE_URL}/{endpoint}"
        try:
            resp = await client.get(
                url,
                params=params,
                headers={"x-api-key": settings.hyblock_api_key},
                timeout=10.0,
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPStatusError as e:
            logger.warning(f"Hyblock {endpoint}: HTTP {e.response.status_code}")
            return {}
        except Exception as e:
            logger.warning(f"Hyblock {endpoint} error: {e}")
            return {}

    # ─── Public ───────────────────────────────────────────────────────────────

    async def fetch_all(self, current_price: float) -> Dict:
        """
        Fetch all Hyblock signals concurrently and return a structured analysis dict.
        Results are cached for CACHE_TTL_SECONDS to stay within rate limits.
        """
        cache_key = f"all_{int(current_price // 100)}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        if not settings.hyblock_api_key:
            return {"available": False, "reason": "No Hyblock API key configured"}

        p = {"coin": COIN, "exchange": EXCHANGE}

        async with httpx.AsyncClient() as client:
            keys = [
                "bid_ask", "bids_change", "asks_change",
                "liq_heatmap", "cumulative_liq", "open_interest",
                "avg_leverage", "top_trader_pos", "top_trader_acc",
                "whale_retail", "volume_delta", "funding",
            ]
            coros = [
                self._fetch(client, "bidAsk", p),
                self._fetch(client, "bidsIncreaseDecrease", p),
                self._fetch(client, "asksIncreaseDecrease", p),
                self._fetch(client, "liquidationHeatmap", p),
                self._fetch(client, "cumulativeLiqLevel", p),
                self._fetch(client, "openInterest", p),
                self._fetch(client, "averageLeverageUsed", p),
                self._fetch(client, "topTraderPositions", p),
                self._fetch(client, "topTraderAccounts", p),
                self._fetch(client, "whaleRetailDelta", p),
                self._fetch(client, "volumeDelta", p),
                self._fetch(client, "fundingRate", p),
            ]
            raw = dict(zip(keys, await asyncio.gather(*coros, return_exceptions=True)))

        # Replace any exceptions with empty dicts
        for k in raw:
            if isinstance(raw[k], Exception):
                logger.warning(f"Hyblock gather exception [{k}]: {raw[k]}")
                raw[k] = {}

        obi_surface = self._compute_obi_surface(raw["bid_ask"])
        obi_slope = self._compute_obi_slope(obi_surface)
        fragility = self._compute_fragility(obi_surface, raw["bids_change"])
        cascade = self._compute_cascade_risk(raw, current_price)
        whale = self._parse_whale_sentiment(raw["whale_retail"])
        top_traders = self._parse_top_trader_sentiment(raw["top_trader_pos"])
        vol_delta = self._parse_volume_delta(raw["volume_delta"])
        liq_clusters = self._parse_liq_clusters(raw["liq_heatmap"], current_price)
        oi_trend = self._parse_oi_trend(raw["open_interest"])

        result = {
            "available": True,
            # OBI surface
            "obi_surface": obi_surface,
            "obi_slope": round(obi_slope, 5),
            "obi_slope_direction": (
                "BULLISH" if obi_slope > 0.005
                else ("BEARISH" if obi_slope < -0.005 else "NEUTRAL")
            ),
            # Fragility
            "fragility_score": round(fragility, 4),
            "fragility_level": (
                "HIGH" if abs(fragility) > 0.4
                else ("MEDIUM" if abs(fragility) > 0.15 else "LOW")
            ),
            # Risk
            "cascade_risk": cascade,
            # Sentiment
            "whale_sentiment": whale,
            "top_trader_sentiment": top_traders,
            "volume_delta_sentiment": vol_delta,
            # Liquidations
            "liq_clusters": liq_clusters,
            "oi_trend": oi_trend,
            # Useful raw snippets for the dashboard
            "funding_rate_raw": raw["funding"].get("rate", raw["funding"].get("fundingRate", 0.0)),
            "avg_leverage_raw": raw["avg_leverage"].get("value", raw["avg_leverage"].get("avgLeverage", 0.0)),
        }
        self._set_cached(cache_key, result)
        return result

    def get_trade_context(
        self, direction: str, data: Dict
    ) -> Tuple[float, str, List[str], bool]:
        """
        Returns (score_delta, description, warnings, should_block).

        score_delta  — points to add/subtract from confidence score
        description  — human-readable summary of Hyblock signals
        warnings     — list of caution strings
        should_block — True when cascade risk is CRITICAL (hard block)
        """
        if not data.get("available"):
            return 0.0, "Hyblock data unavailable", [], False

        score = 0.0
        notes: List[str] = []
        warnings: List[str] = []
        should_block = False

        # ── OBI slope ────────────────────────────────────────────────────────
        obi_dir = data.get("obi_slope_direction", "NEUTRAL")
        if obi_dir == "BULLISH":
            if direction == "LONG":
                score += 8.0
                notes.append("OBI depth-slope bullish")
            else:
                score -= 5.0
                warnings.append("OBI depth-slope bullish — contradicts SHORT")
        elif obi_dir == "BEARISH":
            if direction == "SHORT":
                score += 8.0
                notes.append("OBI depth-slope bearish")
            else:
                score -= 5.0
                warnings.append("OBI depth-slope bearish — contradicts LONG")

        # ── Whale/retail delta ────────────────────────────────────────────────
        whale = data.get("whale_sentiment", "NEUTRAL")
        if whale == "BULLISH":
            if direction == "LONG":
                score += 5.0
                notes.append("whales net long")
            else:
                score -= 4.0
                warnings.append("whales net long — contradicts SHORT")
        elif whale == "BEARISH":
            if direction == "SHORT":
                score += 5.0
                notes.append("whales net short")
            else:
                score -= 4.0
                warnings.append("whales net short — contradicts LONG")

        # ── Top traders (contrarian — crowded positions get faded) ────────────
        top = data.get("top_trader_sentiment", "NEUTRAL")
        if top == "BULLISH":
            if direction == "SHORT":
                score += 5.0
                notes.append("top traders crowded long (fade setup)")
            else:
                score -= 3.0
                warnings.append("top traders crowded long — crowded LONG trade")
        elif top == "BEARISH":
            if direction == "LONG":
                score += 5.0
                notes.append("top traders crowded short (squeeze setup)")
            else:
                score -= 3.0
                warnings.append("top traders crowded short — crowded SHORT trade")

        # ── Volume delta ──────────────────────────────────────────────────────
        vol = data.get("volume_delta_sentiment", "BALANCED")
        if vol == "BUY_DOMINANT":
            if direction == "LONG":
                score += 5.0
                notes.append("buy-dominant volume flow")
            else:
                score -= 3.0
                warnings.append("buy-dominant volume — contradicts SHORT")
        elif vol == "SELL_DOMINANT":
            if direction == "SHORT":
                score += 5.0
                notes.append("sell-dominant volume flow")
            else:
                score -= 3.0
                warnings.append("sell-dominant volume — contradicts LONG")

        # ── Liquidation cluster proximity (price magnets) ─────────────────────
        liq = data.get("liq_clusters", {})
        nearest = liq.get("nearest_side")
        if nearest == "ABOVE" and direction == "LONG":
            score += 5.0
            notes.append(f"liq cluster {liq.get('above_pct', '?')}% above (magnet)")
        elif nearest == "BELOW" and direction == "SHORT":
            score += 5.0
            notes.append(f"liq cluster {liq.get('below_pct', '?')}% below (magnet)")

        # ── Fragility ─────────────────────────────────────────────────────────
        frag = data.get("fragility_level", "LOW")
        if frag == "HIGH":
            score -= 3.0
            warnings.append("high order-book fragility — elevated slippage risk")
        elif frag == "MEDIUM":
            score -= 1.0

        # ── Cascade risk ──────────────────────────────────────────────────────
        cascade = data.get("cascade_risk", "LOW")
        if cascade == "CRITICAL":
            should_block = True
            warnings.append("CRITICAL cascade risk — new entries blocked")
        elif cascade == "HIGH":
            score -= 10.0
            warnings.append("HIGH cascade risk — elevated liquidation cascade probability")
        elif cascade == "MEDIUM":
            score -= 3.0
            warnings.append("MEDIUM cascade risk")

        description = " | ".join(notes) if notes else "No strong Hyblock signals"
        return round(score, 1), description, warnings, should_block

    # ─── Derived metric helpers ───────────────────────────────────────────────

    def _compute_obi_surface(self, bid_ask_data: Dict) -> Dict:
        """
        OBI_d = (BidVol_d - AskVol_d) / (BidVol_d + AskVol_d) for each depth %.
        Tries several common response shapes from the Hyblock API.
        """
        surface: Dict[int, float] = {}

        # Shape 1: {"depths": {"1": {"bid_volume": x, "ask_volume": y}, ...}}
        depths = bid_ask_data.get("depths") or bid_ask_data.get("data", {}).get("depths", {})
        if depths:
            for d in DEPTH_LEVELS:
                entry = depths.get(str(d)) or depths.get(d, {})
                bid = float(entry.get("bid_volume", entry.get("bidVolume", entry.get("bid", 0))))
                ask = float(entry.get("ask_volume", entry.get("askVolume", entry.get("ask", 0))))
                total = bid + ask
                surface[d] = (bid - ask) / total if total > 0 else 0.0
            return surface

        # Shape 2: {"bidVolume": x, "askVolume": y} (single depth)
        bid = float(bid_ask_data.get("bidVolume", bid_ask_data.get("bid_volume", 0)))
        ask = float(bid_ask_data.get("askVolume", bid_ask_data.get("ask_volume", 0)))
        total = bid + ask
        val = (bid - ask) / total if total > 0 else 0.0
        for d in DEPTH_LEVELS:
            surface[d] = val

        return surface

    def _compute_obi_slope(self, surface: Dict) -> float:
        """
        Linear regression slope of OBI across depth levels.
        Positive = more relative bid depth at larger depths (bullish institutional pressure).
        Negative = more relative ask depth at larger depths (bearish institutional presence).
        """
        if len(surface) < 2:
            return 0.0
        xs = [float(k) for k in sorted(surface.keys())]
        ys = [surface[int(x)] for x in xs]
        n = len(xs)
        xm = sum(xs) / n
        ym = sum(ys) / n
        num = sum((x - xm) * (y - ym) for x, y in zip(xs, ys))
        den = sum((x - xm) ** 2 for x in xs)
        return num / den if den else 0.0

    def _compute_fragility(self, surface: Dict, bids_change: Dict) -> float:
        """
        Fragility = shallow OBI × (1 - bid_flow_pct/100).
        High positive fragility on a SHORT = lots of bid depth that could evaporate.
        """
        shallow_obi = surface.get(1, 0.0)
        bid_flow = float(bids_change.get("change_pct", bids_change.get("changePct", 0.0)))
        fragility = shallow_obi * (1.0 - bid_flow / 100.0)
        return max(-1.0, min(1.0, fragility))

    def _compute_cascade_risk(self, raw: Dict, current_price: float) -> str:
        """
        Heuristic cascade risk from leverage z-score, OI rate of change,
        extreme funding, and nearest liquidation cluster.
        Returns 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL'.
        """
        risk = 0

        # Leverage z-score
        lev = raw.get("avg_leverage", {})
        lev_z = float(lev.get("z_score", lev.get("zScore", 0.0)))
        if lev_z > 2.0:
            risk += 3
        elif lev_z > 1.0:
            risk += 1

        # OI rate of change (24h)
        oi = raw.get("open_interest", {})
        oi_roc = float(oi.get("roc_24h_pct", oi.get("roc24hPct", oi.get("changePercent", 0.0))))
        if abs(oi_roc) > 10.0:
            risk += 2
        elif abs(oi_roc) > 5.0:
            risk += 1

        # Extreme funding
        fd = raw.get("funding", {})
        fr = float(fd.get("rate", fd.get("fundingRate", 0.0)))
        if abs(fr) > 0.001:
            risk += 2
        elif abs(fr) > 0.0005:
            risk += 1

        # Nearest liq cluster (from cumulative liq level)
        cl = raw.get("cumulative_liq", {})
        prox = float(cl.get("nearest_cluster_pct", cl.get("nearestClusterPct", 100.0)))
        if prox < 2.0:
            risk += 3
        elif prox < 5.0:
            risk += 1

        if risk >= 6:
            return "CRITICAL"
        if risk >= 4:
            return "HIGH"
        if risk >= 2:
            return "MEDIUM"
        return "LOW"

    def _parse_whale_sentiment(self, data: Dict) -> str:
        delta = float(data.get("whale_delta", data.get("whaleDelta", data.get("delta", 0.0))))
        if delta > 0.1:
            return "BULLISH"
        if delta < -0.1:
            return "BEARISH"
        return "NEUTRAL"

    def _parse_top_trader_sentiment(self, data: Dict) -> str:
        long_pct = float(data.get("long_pct", data.get("longPct", data.get("longAccount", 50.0))))
        if long_pct > 60.0:
            return "BULLISH"
        if long_pct < 40.0:
            return "BEARISH"
        return "NEUTRAL"

    def _parse_volume_delta(self, data: Dict) -> str:
        delta = float(data.get("delta", data.get("volumeDelta", 0.0)))
        if delta > 0.15:
            return "BUY_DOMINANT"
        if delta < -0.15:
            return "SELL_DOMINANT"
        return "BALANCED"

    def _parse_liq_clusters(self, heatmap: Dict, current_price: float) -> Dict:
        """Find nearest liquidation clusters above and below current price."""
        levels = heatmap.get("levels", heatmap.get("data", []))
        if not levels or current_price <= 0:
            return {"above_pct": None, "below_pct": None, "nearest_side": None}

        above = [l for l in levels if float(l.get("price", 0)) > current_price]
        below = [l for l in levels if float(l.get("price", 0)) < current_price]

        above_pct = None
        below_pct = None

        if above:
            px = min(float(l["price"]) for l in above)
            above_pct = round((px - current_price) / current_price * 100, 2)
        if below:
            px = max(float(l["price"]) for l in below)
            below_pct = round((current_price - px) / current_price * 100, 2)

        if above_pct is not None and below_pct is not None:
            nearest_side = "ABOVE" if above_pct < below_pct else "BELOW"
        elif above_pct is not None:
            nearest_side = "ABOVE"
        elif below_pct is not None:
            nearest_side = "BELOW"
        else:
            nearest_side = None

        return {"above_pct": above_pct, "below_pct": below_pct, "nearest_side": nearest_side}

    def _parse_oi_trend(self, data: Dict) -> str:
        roc = float(data.get("roc_24h_pct", data.get("roc24hPct", data.get("changePercent", 0.0))))
        if roc > 3.0:
            return "RISING"
        if roc < -3.0:
            return "FALLING"
        return "FLAT"
