"""
Hyblock Capital API Integration
Fetches order book pressure, liquidation clusters, whale/retail positioning,
and cascade risk signals from the Hyblock Capital enterprise API.
"""
import asyncio
import base64
import time
from typing import Dict, List, Optional, Tuple
import httpx
from loguru import logger

from backend.config import settings

COIN = "BTC"
EXCHANGE = "binance_perp_stable"
DEPTH_LEVELS = [1, 2, 5, 10]
CACHE_TTL_SECONDS = 60
TOKEN_TTL_SECONDS = 82800   # refresh 1h before the 24h expiry


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
        self._token: Optional[str] = None
        self._token_fetched_at: float = 0.0
        self._token_lock = asyncio.Lock()

    # ─── Caching ─────────────────────────────────────────────────────────────

    def _get_cached(self, key: str) -> Optional[Dict]:
        entry = self._cache.get(key)
        if entry and (time.monotonic() - entry[0]) < CACHE_TTL_SECONDS:
            return entry[1]
        return None

    def _set_cached(self, key: str, data: Dict):
        self._cache[key] = (time.monotonic(), data)

    # ─── OAuth2 token ─────────────────────────────────────────────────────────

    async def _get_token(self, client: httpx.AsyncClient) -> Optional[str]:
        """Return a valid Bearer token, fetching/refreshing as needed."""
        async with self._token_lock:
            age = time.monotonic() - self._token_fetched_at
            if self._token and age < TOKEN_TTL_SECONDS:
                return self._token
            if not settings.hyblock_access_key_id or not settings.hyblock_api_secret:
                return None
            try:
                basic = base64.b64encode(
                    f"{settings.hyblock_access_key_id}:{settings.hyblock_api_secret}".encode()
                ).decode()
                resp = await client.post(
                    f"{self.BASE_URL}/oauth2/token",
                    data="grant_type=client_credentials",
                    headers={
                        "Authorization": f"Basic {basic}",
                        "Content-Type": "application/x-www-form-urlencoded",
                        "x-api-key": settings.hyblock_api_key,
                    },
                    timeout=15.0,
                )
                resp.raise_for_status()
                self._token = resp.json()["access_token"]
                self._token_fetched_at = time.monotonic()
                logger.info("Hyblock OAuth2 token refreshed")
                return self._token
            except Exception as e:
                logger.warning(f"Hyblock token refresh failed: {e}")
                return None

    # ─── HTTP ─────────────────────────────────────────────────────────────────

    async def _fetch(
        self,
        client: httpx.AsyncClient,
        endpoint: str,
        params: Dict,
        unwrap_latest: bool = False,
    ) -> Dict:
        """
        Fetch one Hyblock endpoint.
        unwrap_latest=True  → for time-series endpoints: return the most recent bar
                              from {"data": [...]} (limit=1 so only one bar anyway)
        unwrap_latest=False → for snapshot endpoints: return the full response body
        """
        url = f"{self.BASE_URL}/{endpoint}"
        token = await self._get_token(client)
        headers: Dict[str, str] = {"x-api-key": settings.hyblock_api_key}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            resp = await client.get(url, params=params, headers=headers, timeout=10.0)
            resp.raise_for_status()
            body = resp.json()
            if unwrap_latest and isinstance(body, dict) and "data" in body:
                data = body["data"]
                if isinstance(data, list) and data:
                    return data[-1]  # most recent bar
                if isinstance(data, dict):
                    return data
                return {}
            return body if isinstance(body, dict) else {}
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

        # Snapshot endpoints: no timeframe param accepted
        p_snap = {"coin": COIN, "exchange": EXCHANGE}
        # Time-series endpoints: valid limit values are 5, 10, 20, 50, 100, 500, 1000
        p_ts = {"coin": COIN, "exchange": EXCHANGE, "timeframe": "1h", "limit": 5}
        # averageLeverageUsed only works on OKX (not binance_perp_stable)
        p_lev = {"coin": COIN, "exchange": "okx_perp_coin", "timeframe": "1h", "limit": 5}

        async with httpx.AsyncClient() as client:
            keys = [
                "bid_ask", "bids_change", "asks_change",
                "liq_heatmap", "cumulative_liq", "open_interest",
                "avg_leverage", "top_trader_pos", "top_trader_acc",
                "whale_retail", "volume_delta", "funding", "market_imbalance",
            ]
            coros = [
                self._fetch(client, "bidAsk",                 p_ts,   unwrap_latest=True),
                self._fetch(client, "bidsIncreaseDecrease",   p_ts,   unwrap_latest=True),
                self._fetch(client, "asksIncreaseDecrease",   p_ts,   unwrap_latest=True),
                self._fetch(client, "liquidationHeatmap",     p_snap),  # snapshot only
                self._fetch(client, "cumulativeLiqLevel",     p_snap),  # snapshot only
                self._fetch(client, "openInterest",           p_ts,   unwrap_latest=True),
                self._fetch(client, "averageLeverageUsed",    p_lev,  unwrap_latest=True),
                self._fetch(client, "topTraderPositions",     p_ts,   unwrap_latest=True),
                self._fetch(client, "topTraderAccounts",      p_ts,   unwrap_latest=True),
                self._fetch(client, "whaleRetailDelta",       p_ts,   unwrap_latest=True),
                self._fetch(client, "volumeDelta",            p_ts,   unwrap_latest=True),
                self._fetch(client, "fundingRate",            p_ts,   unwrap_latest=True),
                self._fetch(client, "marketImbalanceIndex",   p_ts,   unwrap_latest=True),
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
            # Market Imbalance Index (-1 to 1): combines orderflow + orderbook pressure
            "market_imbalance_index": float(
                raw["market_imbalance"].get("marketImbalanceIndex", 0.0)
            ),
            # Useful raw snippets for the dashboard
            "funding_rate_raw": float(raw["funding"].get("fundingRate", raw["funding"].get("rate", 0.0))),
            "avg_leverage_raw": (
                (float(raw["avg_leverage"].get("avgLongLev", 0.0)) +
                 float(raw["avg_leverage"].get("avgShortLev", 0.0))) / 2.0
                if raw["avg_leverage"].get("avgLongLev") or raw["avg_leverage"].get("avgShortLev")
                else 0.0
            ),
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

        # ── Whale flow + top trader positioning (divergence-aware) ──────────────
        # Best LONG: whales bullish + top traders crowded short (squeeze setup) → +12
        # Best SHORT: whales bearish + top traders crowded long (fade setup)   → +12
        # Conflicted (both imply same direction doubt)                          → +2 + warning
        # Single signal alone                                                   → ±5
        whale = data.get("whale_sentiment", "NEUTRAL")
        top   = data.get("top_trader_sentiment", "NEUTRAL")
        whale_bull      = whale == "BULLISH"
        whale_bear      = whale == "BEARISH"
        top_crowd_long  = top == "BULLISH"   # top traders long → contrarian bearish
        top_crowd_short = top == "BEARISH"   # top traders short → contrarian bullish

        if direction == "LONG":
            if whale_bull and top_crowd_short:
                score += 12.0
                notes.append("whales net long + top traders crowded short (strong squeeze setup)")
            elif whale_bear and top_crowd_long:
                score += 2.0
                warnings.append("whale/top trader conflict — both suggest caution for LONG")
            elif whale_bull:
                score += 5.0
                notes.append("whales net long")
            elif top_crowd_short:
                score += 5.0
                notes.append("top traders crowded short (squeeze setup)")
            elif whale_bear:
                score -= 4.0
                warnings.append("whales net short — contradicts LONG")
            elif top_crowd_long:
                score -= 3.0
                warnings.append("top traders crowded long — crowded LONG trade")
        elif direction == "SHORT":
            if whale_bear and top_crowd_long:
                score += 12.0
                notes.append("whales net short + top traders crowded long (strong fade setup)")
            elif whale_bull and top_crowd_short:
                score += 2.0
                warnings.append("whale/top trader conflict — both suggest caution for SHORT")
            elif whale_bear:
                score += 5.0
                notes.append("whales net short")
            elif top_crowd_long:
                score += 5.0
                notes.append("top traders crowded long (fade setup)")
            elif whale_bull:
                score -= 4.0
                warnings.append("whales net long — contradicts SHORT")
            elif top_crowd_short:
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
        # Tier: <10 BTC = ignore noise, 10-100 BTC = +3, 100-500 BTC = +5, >500 BTC = +8
        MIN_LIQ_BTC = 10.0
        liq = data.get("liq_clusters", {})
        if direction == "LONG":
            above_pct  = liq.get("above_pct")
            above_size = liq.get("above_size", 0.0) or 0.0
            if above_pct is not None and above_size >= MIN_LIQ_BTC:
                liq_pts = 8.0 if above_size > 500 else 5.0 if above_size > 100 else 3.0
                score += liq_pts
                notes.append(f"liq cluster {above_pct}% above ({above_size:.0f} BTC magnet +{liq_pts:.0f})")
        elif direction == "SHORT":
            below_pct  = liq.get("below_pct")
            below_size = liq.get("below_size", 0.0) or 0.0
            if below_pct is not None and below_size >= MIN_LIQ_BTC:
                liq_pts = 8.0 if below_size > 500 else 5.0 if below_size > 100 else 3.0
                score += liq_pts
                notes.append(f"liq cluster {below_pct}% below ({below_size:.0f} BTC magnet +{liq_pts:.0f})")

        # ── Market Imbalance Index (replaces whale wall approximation) ───────────
        # Combines futures orderflow + orderbook pressure; -1 (sellers) to +1 (buyers)
        mii = data.get("market_imbalance_index", 0.0)
        if mii > 0.3:
            if direction == "LONG":
                score += 6.0
                notes.append(f"market imbalance strongly bullish ({mii:+.2f})")
            else:
                score -= 4.0
                warnings.append(f"market imbalance bullish ({mii:+.2f}) — contradicts SHORT")
        elif mii > 0.1:
            if direction == "LONG":
                score += 3.0
                notes.append(f"market imbalance mildly bullish ({mii:+.2f})")
        elif mii < -0.3:
            if direction == "SHORT":
                score += 6.0
                notes.append(f"market imbalance strongly bearish ({mii:+.2f})")
            else:
                score -= 4.0
                warnings.append(f"market imbalance bearish ({mii:+.2f}) — contradicts LONG")
        elif mii < -0.1:
            if direction == "SHORT":
                score += 3.0
                notes.append(f"market imbalance mildly bearish ({mii:+.2f})")

        # ── Fragility ─────────────────────────────────────────────────────────
        # Skip if MII already shows directional pressure — avoid double-counting
        frag = data.get("fragility_level", "LOW")
        if abs(mii) <= 0.1:
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

        # Shape 2: {"bid": x, "ask": y} (single depth, official field names)
        bid = float(bid_ask_data.get("bid", bid_ask_data.get("bidVolume", bid_ask_data.get("bid_volume", 0))))
        ask = float(bid_ask_data.get("ask", bid_ask_data.get("askVolume", bid_ask_data.get("ask_volume", 0))))
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

        # Average leverage level (avgLongLev / avgShortLev from API)
        lev = raw.get("avg_leverage", {})
        avg_long_lev = float(lev.get("avgLongLev", 0.0))
        avg_short_lev = float(lev.get("avgShortLev", 0.0))
        avg_lev = (avg_long_lev + avg_short_lev) / 2.0 if (avg_long_lev or avg_short_lev) else 0.0
        if avg_lev > 30.0:
            risk += 3
        elif avg_lev > 20.0:
            risk += 1

        # OI rate of change (computed from OHLC open/close — no roc field in API)
        oi = raw.get("open_interest", {})
        oi_open = float(oi.get("open", 0.0))
        oi_close = float(oi.get("close", 0.0))
        oi_roc = (oi_close - oi_open) / oi_open * 100 if oi_open > 0 else 0.0
        if abs(oi_roc) > 10.0:
            risk += 2
        elif abs(oi_roc) > 5.0:
            risk += 1

        # Extreme funding
        fd = raw.get("funding", {})
        fr = float(fd.get("fundingRate", fd.get("rate", 0.0)))
        if abs(fr) > 0.001:
            risk += 2
        elif abs(fr) > 0.0005:
            risk += 1

        # Liq pressure imbalance (totalLong vs totalShort liquidation size)
        cl = raw.get("cumulative_liq", {})
        long_liq = float(cl.get("totalLongLiquidationSize", 0.0))
        short_liq = float(cl.get("totalShortLiquidationSize", 0.0))
        total_liq = long_liq + short_liq
        if total_liq > 0:
            liq_imbalance = abs(long_liq - short_liq) / total_liq
            if liq_imbalance > 0.5:
                risk += 3
            elif liq_imbalance > 0.25:
                risk += 1

        if risk >= 6:
            return "CRITICAL"
        if risk >= 4:
            return "HIGH"
        if risk >= 2:
            return "MEDIUM"
        return "LOW"

    def _parse_whale_sentiment(self, data: Dict) -> str:
        delta = float(data.get("whaleRetailDelta", data.get("whale_delta", data.get("whaleDelta", data.get("delta", 0.0)))))
        if delta > 0.1:
            return "BULLISH"
        if delta < -0.1:
            return "BEARISH"
        return "NEUTRAL"

    def _parse_top_trader_sentiment(self, data: Dict) -> str:
        long_pct = float(data.get("longPct", data.get("long_pct", data.get("longAccount", 50.0))))
        if long_pct > 60.0:
            return "BULLISH"
        if long_pct < 40.0:
            return "BEARISH"
        return "NEUTRAL"

    def _parse_volume_delta(self, data: Dict) -> str:
        delta = float(data.get("volumeDelta", data.get("delta", 0.0)))
        if delta > 0.15:
            return "BUY_DOMINANT"
        if delta < -0.15:
            return "SELL_DOMINANT"
        return "BALANCED"

    def _parse_liq_clusters(self, heatmap: Dict, current_price: float) -> Dict:
        """Find nearest liquidation clusters above and below current price, with BTC size."""
        levels = heatmap.get("data") or heatmap.get("levels") or heatmap.get("heatmap") or []
        if isinstance(levels, dict):
            levels = list(levels.values())
        if not levels or current_price <= 0:
            return {"above_pct": None, "below_pct": None, "nearest_side": None,
                    "above_size": 0.0, "below_size": 0.0}

        def get_px(l: Dict) -> float:
            start = l.get("startingPrice")
            end = l.get("endingPrice")
            if start is not None and end is not None:
                return (float(start) + float(end)) / 2.0
            for key in ("price", "priceLevel", "price_level", "liqPrice", "liquidationPrice"):
                v = l.get(key)
                if v is not None:
                    return float(v)
            return 0.0

        def get_size(l: Dict) -> float:
            # Sum long + short liquidation size; fall back to generic size fields
            long_s  = l.get("longLiquidationSize",  l.get("longSize",  0.0)) or 0.0
            short_s = l.get("shortLiquidationSize", l.get("shortSize", 0.0)) or 0.0
            if long_s or short_s:
                return float(long_s) + float(short_s)
            for key in ("liquidationSize", "size", "btcSize", "totalSize", "notional"):
                v = l.get(key)
                if v is not None:
                    return float(v)
            return 0.0

        above_pct = above_size = above_price_val = None
        below_pct = below_size = below_price_val = None

        above_levels = [(get_px(l), get_size(l)) for l in levels if get_px(l) > current_price]
        below_levels = [(get_px(l), get_size(l)) for l in levels if 0 < get_px(l) < current_price]

        if above_levels:
            px = min(p for p, _ in above_levels)
            above_price_val = round(px, 2)
            above_pct  = round((px - current_price) / current_price * 100, 2)
            above_size = round(sum(s for _, s in above_levels), 2)
        if below_levels:
            px = max(p for p, _ in below_levels)
            below_price_val = round(px, 2)
            below_pct  = round((current_price - px) / current_price * 100, 2)
            below_size = round(sum(s for _, s in below_levels), 2)

        if above_pct is not None and below_pct is not None:
            nearest_side = "ABOVE" if above_pct < below_pct else "BELOW"
        elif above_pct is not None:
            nearest_side = "ABOVE"
        elif below_pct is not None:
            nearest_side = "BELOW"
        else:
            nearest_side = None

        return {
            "above_pct":    above_pct,
            "above_price":  above_price_val,
            "below_pct":    below_pct,
            "below_price":  below_price_val,
            "nearest_side": nearest_side,
            "above_size":   above_size or 0.0,
            "below_size":   below_size or 0.0,
        }

    def _parse_oi_trend(self, data: Dict) -> str:
        # OI API returns OHLC — compute pct change from open to close
        oi_open = float(data.get("open", 0.0))
        oi_close = float(data.get("close", 0.0))
        roc = (oi_close - oi_open) / oi_open * 100 if oi_open > 0 else 0.0
        if roc > 3.0:
            return "RISING"
        if roc < -3.0:
            return "FALLING"
        return "FLAT"
