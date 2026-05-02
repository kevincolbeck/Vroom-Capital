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
# Stable-margined perp exchanges supporting BTC (used for MII and liq heatmap)
ALL_EXCHANGES = (
    "binance_perp_stable,bybit_perp_stable,okx_perp_stable,bitget_perp_stable,"
    "hyperliquid_perp_stable,coinbaseadvanced_perp_stable,deribit_perp_stable,"
    "bitfinex_perp_stable,phemex_perp_stable,arkham_perp_stable"
)
# MII confirmed working on 4 major stable perps (others return 422)
MII_EXCHANGES = "binance_perp_stable,bybit_perp_stable,okx_perp_stable,bitget_perp_stable"
# Full OI exchange list — 17 exchanges (Hyblock openInterest endpoint defaults)
OI_EXCHANGES = (
    "bitmex_perp_coin,bybit_perp_coin,bitfinex_perp_stable,deribit_perp_stable,"
    "phemex_perp_stable,huobi_perp_coin,okx_perp_coin,okx_qtrly,binance_perp_stable,"
    "binance_perp_coin,arkham_perp_stable,bybit_perp_stable,bitget_perp_stable,"
    "bitget_perp_coin,okx_perp_stable,hyperliquid_perp_stable,hyperliquid_xyz"
)
# Net L/S positioning exchanges — 10 exchanges (Hyblock netLongShortDelta defaults)
NLS_EXCHANGES = (
    "bitmex_perp_coin,bybit_perp_coin,binance_perp_stable,binance_perp_coin,"
    "bybit_perp_stable,bitget_perp_stable,bitget_perp_coin,okx_perp_stable,"
    "okx_perp_coin,okx_qtrly"
)
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
            status = e.response.status_code
            if status == 422:
                logger.debug(f"Hyblock {endpoint}: HTTP 422 (exchange not supported for this endpoint)")
            else:
                logger.warning(f"Hyblock {endpoint}: HTTP {status}")
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

        # Single-exchange snapshots (liq levels, cumulative liq — single-only APIs)
        # liquidationLevels confirmed working (via /catalog test): binance_perp_stable,
        # bitmex_perp_coin, bybit_perp_coin, phemex_perp_stable. All others return 422.
        p_snap        = {"coin": COIN, "exchange": "binance_perp_stable"}
        p_snap_bitmex = {"coin": COIN, "exchange": "bitmex_perp_coin"}
        p_snap_bybit  = {"coin": COIN, "exchange": "bybit_perp_coin"}
        p_snap_phemex = {"coin": COIN, "exchange": "phemex_perp_stable"}
        # Time-series params — base (Binance) + confirmed multi-exchange partners
        # bidAsk: 15/17 exchanges work; using top-5 by volume
        # topTraderPositions: binance, binance_coin, okx, bitget, huobi confirmed
        # whaleRetailDelta: binance, binance_coin, okx_coin confirmed
        # liqLevelsCount: binance_perp_stable returns 422 — use coin/bitmex/phemex
        # globalAccounts: binance, binance_coin, bybit_coin confirmed
        p_ts        = {"coin": COIN, "exchange": "binance_perp_stable",      "timeframe": "1h", "limit": 5}
        p_ts_bcoin  = {"coin": COIN, "exchange": "binance_perp_coin",        "timeframe": "1h", "limit": 5}
        p_ts_bybit  = {"coin": COIN, "exchange": "bybit_perp_stable",        "timeframe": "1h", "limit": 5}
        p_ts_okx    = {"coin": COIN, "exchange": "okx_perp_stable",          "timeframe": "1h", "limit": 5}
        p_ts_okxc   = {"coin": COIN, "exchange": "okx_perp_coin",            "timeframe": "1h", "limit": 5}
        p_ts_bitget = {"coin": COIN, "exchange": "bitget_perp_stable",       "timeframe": "1h", "limit": 5}
        p_ts_hyper  = {"coin": COIN, "exchange": "hyperliquid_perp_stable",  "timeframe": "1h", "limit": 5}
        p_ts_huobi  = {"coin": COIN, "exchange": "huobi_perp_coin",          "timeframe": "1h", "limit": 5}
        p_ts_bitmex = {"coin": COIN, "exchange": "bitmex_perp_coin",         "timeframe": "1h", "limit": 5}
        p_ts_phemex = {"coin": COIN, "exchange": "phemex_perp_stable",       "timeframe": "1h", "limit": 5}
        p_ts_bbybit = {"coin": COIN, "exchange": "bybit_perp_coin",          "timeframe": "1h", "limit": 5}
        # averageLeverageUsed only works on OKX
        p_lev      = {"coin": COIN, "exchange": "okx_perp_coin", "timeframe": "1h", "limit": 5}
        # MII: cross-exchange aggregate (4 confirmed-working stable perps)
        p_mii      = {"coin": COIN, "exchange": MII_EXCHANGES, "timeframe": "15m", "limit": 5}
        # Liquidation heatmap: spec default exchanges (others return 422)
        p_liq_heat = {"coin": COIN, "exchange": "binance_perp_stable,bitmex_perp_coin,bybit_perp_coin"}
        # Orderflow: multi-exchange perpetuals (volumeDelta, volumeRatio, buySellTradeCountRatio)
        p_flow     = {"coin": COIN, "timeframe": "1h", "limit": 5, "marketTypes": "perpetuals"}
        # CVD: 20 bars of volumeDelta, multi-exchange perpetuals
        p_cvd      = {"coin": COIN, "timeframe": "1h", "limit": 20, "marketTypes": "perpetuals"}
        # Spot CVD: same window, spot exchanges — for divergence detection
        p_cvd_spot = {"coin": COIN, "timeframe": "1h", "limit": 20, "marketTypes": "spot"}
        # OI: 17-exchange aggregate
        p_oi       = {"coin": COIN, "exchange": OI_EXCHANGES, "timeframe": "1h", "limit": 5}
        # OI multi-bar: 5 bars for rate-of-change, 17-exchange aggregate
        p_oi_multi = {"coin": COIN, "exchange": OI_EXCHANGES, "timeframe": "1h", "limit": 5}
        # Net L/S delta: 10-exchange aggregate
        p_nls      = {"coin": COIN, "exchange": NLS_EXCHANGES, "timeframe": "1h", "limit": 5}
        # 4H compression detection (Binance is price reference)
        p_4h       = {"coin": COIN, "exchange": "binance_perp_stable", "timeframe": "4h", "limit": 20}
        # Previous day levels — 1d timeframe required (1h returns empty); limit:1+desc = latest bar
        p_pd       = {"coin": COIN, "exchange": "binance_perp_stable", "timeframe": "1d", "limit": 1, "sort": "desc"}
        # Previous week levels — same: 1d timeframe, limit:1+desc
        p_pw       = {"coin": COIN, "exchange": "binance_perp_stable", "timeframe": "1d", "limit": 1, "sort": "desc"}

        async with httpx.AsyncClient() as client:
            keys = [
                "bid_ask", "bids_change", "asks_change",
                "liq_heatmap", "cumulative_liq", "open_interest",
                "avg_leverage", "top_trader_pos", "top_trader_acc",
                "whale_retail", "volume_delta", "funding", "market_imbalance",
                # Precision scalping signals — liq levels from 4 confirmed exchanges
                "liq_levels", "liq_levels_bitmex", "liq_levels_bybit", "liq_levels_phemex",
                "liq_levels_size", "liq_levels_count",
                "volume_ratio", "buy_sell_count",
                # CVD and OI multi-bar (full response, not unwrapped)
                "volume_delta_multi", "cvd_spot", "oi_multi",
                # WarriorAI-aligned signals
                "true_retail", "global_accts", "net_ls_delta", "prev_day", "prev_week", "kline_4h",
                # bidAsk multi-exchange: upgrade OBI from Binance-only to 5-exchange average
                "bid_ask_bybit", "bid_ask_okx", "bid_ask_bitget", "bid_ask_hyper",
                # cumulativeLiqLevel multi-exchange: same 4 exchanges as liquidationLevels
                "cumulative_liq_bitmex", "cumulative_liq_bybit", "cumulative_liq_phemex",
                # topTraderPositions multi-exchange: 5 confirmed-working exchanges
                "top_trader_pos_bcoin", "top_trader_pos_okx", "top_trader_pos_bitget", "top_trader_pos_huobi",
                # whaleRetailDelta multi-exchange: 3 confirmed-working exchanges
                "whale_retail_bcoin", "whale_retail_okxc",
                # liqLevelsCount additional exchanges
                "liq_levels_count_bitmex", "liq_levels_count_phemex",
                # globalAccounts multi-exchange: 3 confirmed-working exchanges
                "global_accts_bcoin", "global_accts_bbybit",
            ]
            coros = [
                self._fetch(client, "bidAsk",                 p_ts,       unwrap_latest=True),
                self._fetch(client, "bidsIncreaseDecrease",   p_ts,       unwrap_latest=True),
                self._fetch(client, "asksIncreaseDecrease",   p_ts,       unwrap_latest=True),
                self._fetch(client, "liquidationHeatmap",     p_liq_heat),           # multi-exchange
                self._fetch(client, "cumulativeLiqLevel",     p_snap,     unwrap_latest=True),   # single-only
                self._fetch(client, "openInterest",           p_oi,       unwrap_latest=True),   # multi-exchange
                self._fetch(client, "averageLeverageUsed",    p_lev,      unwrap_latest=True),
                self._fetch(client, "topTraderPositions",     p_ts,       unwrap_latest=True),
                self._fetch(client, "topTraderAccounts",      p_ts,       unwrap_latest=True),
                self._fetch(client, "whaleRetailDelta",       p_ts,       unwrap_latest=True),
                self._fetch(client, "volumeDelta",            p_flow,     unwrap_latest=True),   # multi-exchange perpetuals
                self._fetch(client, "fundingRate",            p_ts,       unwrap_latest=True),
                self._fetch(client, "marketImbalanceIndex",   p_mii,      unwrap_latest=False),
                # Exact per-price liquidation levels — 4 confirmed-working exchanges (single-only APIs)
                self._fetch(client, "liquidationLevels",      p_snap),
                self._fetch(client, "liquidationLevels",      p_snap_bitmex),
                self._fetch(client, "liquidationLevels",      p_snap_bybit),
                self._fetch(client, "liquidationLevels",      p_snap_phemex),
                # Liq level size/count delta oscillators
                # liqLevelsSize: binance_perp_stable per API docs example
                # liqLevelsCount: binance_perp_stable returns 422; use coin/bitmex/phemex
                self._fetch(client, "liqLevelsSize",          p_ts,       unwrap_latest=True),
                self._fetch(client, "liqLevelsCount",         p_ts_bcoin, unwrap_latest=True),
                # Volume ratio and buy/sell trade count ratio — multi-exchange perpetuals
                self._fetch(client, "volumeRatio",            p_flow,     unwrap_latest=True),   # multi-exchange perpetuals
                self._fetch(client, "buySellTradeCountRatio", p_flow,     unwrap_latest=True),   # multi-exchange perpetuals
                # CVD: 20 bars of volumeDelta, multi-exchange perpetuals
                self._fetch(client, "volumeDelta",            p_cvd,      unwrap_latest=False),
                # Spot CVD: same window but spot exchanges (gracefully returns {} on 422)
                self._fetch(client, "volumeDelta",            p_cvd_spot, unwrap_latest=False),
                # OI multi-bar: 5 bars for rate-of-change, 17-exchange aggregate
                self._fetch(client, "openInterest",           p_oi_multi, unwrap_latest=False),
                # WarriorAI-aligned: retail/global L/S (single-only), net positioning, PDH/PDL, 4H compression
                self._fetch(client, "trueRetailLongShort",    p_ts,       unwrap_latest=True),
                self._fetch(client, "globalAccounts",         p_ts,       unwrap_latest=True),
                self._fetch(client, "netLongShortDelta",      p_nls,      unwrap_latest=True),   # multi-exchange
                self._fetch(client, "pdLevels",               p_pd,       unwrap_latest=True),
                self._fetch(client, "pwLevels",               p_pw,       unwrap_latest=True),
                self._fetch(client, "klines",                 p_4h,       unwrap_latest=False),
                # bidAsk multi-exchange
                self._fetch(client, "bidAsk",                 p_ts_bybit,  unwrap_latest=True),
                self._fetch(client, "bidAsk",                 p_ts_okx,    unwrap_latest=True),
                self._fetch(client, "bidAsk",                 p_ts_bitget, unwrap_latest=True),
                self._fetch(client, "bidAsk",                 p_ts_hyper,  unwrap_latest=True),
                # cumulativeLiqLevel multi-exchange
                self._fetch(client, "cumulativeLiqLevel",     p_snap_bitmex, unwrap_latest=True),
                self._fetch(client, "cumulativeLiqLevel",     p_snap_bybit,  unwrap_latest=True),
                self._fetch(client, "cumulativeLiqLevel",     p_snap_phemex, unwrap_latest=True),
                # topTraderPositions multi-exchange
                self._fetch(client, "topTraderPositions",     p_ts_bcoin,  unwrap_latest=True),
                self._fetch(client, "topTraderPositions",     p_ts_okx,    unwrap_latest=True),
                self._fetch(client, "topTraderPositions",     p_ts_bitget, unwrap_latest=True),
                self._fetch(client, "topTraderPositions",     p_ts_huobi,  unwrap_latest=True),
                # whaleRetailDelta multi-exchange
                self._fetch(client, "whaleRetailDelta",       p_ts_bcoin,  unwrap_latest=True),
                self._fetch(client, "whaleRetailDelta",       p_ts_okxc,   unwrap_latest=True),
                # liqLevelsCount additional exchanges
                self._fetch(client, "liqLevelsCount",         p_ts_bitmex, unwrap_latest=True),
                self._fetch(client, "liqLevelsCount",         p_ts_phemex, unwrap_latest=True),
                # globalAccounts multi-exchange
                self._fetch(client, "globalAccounts",         p_ts_bcoin,  unwrap_latest=True),
                self._fetch(client, "globalAccounts",         p_ts_bbybit, unwrap_latest=True),
            ]
            raw = dict(zip(keys, await asyncio.gather(*coros, return_exceptions=True)))

        # Replace any exceptions with empty dicts
        for k in raw:
            if isinstance(raw[k], Exception):
                logger.warning(f"Hyblock gather exception [{k}]: {raw[k]}")
                raw[k] = {}

        # OBI: 5-exchange average surface → more robust slope than single exchange
        _obi_surfaces = [self._compute_obi_surface(raw[k])
                         for k in ("bid_ask", "bid_ask_bybit", "bid_ask_okx", "bid_ask_bitget", "bid_ask_hyper")]
        obi_surface = self._merge_obi_surfaces(_obi_surfaces)
        obi_slope = self._compute_obi_slope(obi_surface)
        fragility = self._compute_fragility(obi_surface, raw["bids_change"])
        cascade = self._compute_cascade_risk(raw, current_price)
        # Whale: average delta across 3 exchanges
        whale = self._merge_whale_sentiment([raw["whale_retail"], raw["whale_retail_bcoin"], raw["whale_retail_okxc"]])
        # Top traders: average long_pct across 5 exchanges
        top_traders = self._merge_top_trader_sentiment([
            raw["top_trader_pos"], raw["top_trader_pos_bcoin"],
            raw["top_trader_pos_okx"], raw["top_trader_pos_bitget"], raw["top_trader_pos_huobi"],
        ])
        vol_delta = self._parse_volume_delta(raw["volume_delta"])
        liq_clusters = self._parse_liq_clusters(raw["liq_heatmap"], current_price)
        oi_trend = self._parse_oi_trend(raw["open_interest"])
        _liq_lvl_binance = self._parse_liq_levels(raw["liq_levels"],         current_price)
        _liq_lvl_bitmex  = self._parse_liq_levels(raw["liq_levels_bitmex"], current_price)
        _liq_lvl_bybit   = self._parse_liq_levels(raw["liq_levels_bybit"],  current_price)
        _liq_lvl_phemex  = self._parse_liq_levels(raw["liq_levels_phemex"], current_price)
        liq_levels = self._merge_liq_levels([_liq_lvl_binance, _liq_lvl_bitmex, _liq_lvl_bybit, _liq_lvl_phemex])
        volume_ratio = self._parse_scalar(raw["volume_ratio"], ("volumeRatio", "ratio", "value", "delta"))
        buy_sell_count = self._parse_scalar(raw["buy_sell_count"], ("buySellTradeCountRatio", "ratio", "value", "delta"))
        liq_levels_size = self._parse_scalar(raw["liq_levels_size"], ("liqLevelSizeDelta", "liqLevelsSizeDelta", "sizeDelta", "delta", "value"))
        # liqLevelsCount: average across 3 working exchanges (binance_perp_stable returns 422)
        _cnt_vals = [self._parse_scalar(raw[k], ("liqLevelCountDelta", "liqLevelsCountDelta", "countDelta", "delta", "value"))
                     for k in ("liq_levels_count", "liq_levels_count_bitmex", "liq_levels_count_phemex")]
        _cnt_nonzero = [v for v in _cnt_vals if v != 0.0]
        liq_levels_count = sum(_cnt_nonzero) / len(_cnt_nonzero) if _cnt_nonzero else 0.0
        cvd = self._parse_cvd(raw["volume_delta_multi"])
        cvd_spot = self._parse_cvd(raw["cvd_spot"])
        oi_delta = self._parse_oi_delta(raw["oi_multi"])
        # cumulativeLiqLevel: sum long/short sizes across 4 exchanges for accurate bias
        cumulative_liq_detail = self._merge_cumulative_liq([
            raw["cumulative_liq"], raw["cumulative_liq_bitmex"],
            raw["cumulative_liq_bybit"], raw["cumulative_liq_phemex"],
        ])
        true_retail = self._parse_retail_ratio(raw["true_retail"])
        # globalAccounts: average long/short pct across 3 exchanges
        _ga_sources = [raw["global_accts"], raw["global_accts_bcoin"], raw["global_accts_bbybit"]]
        global_accts = self._merge_retail_ratio(_ga_sources)
        net_ls_delta = self._parse_net_ls_delta(raw["net_ls_delta"])
        prev_day  = self._parse_prev_day_structure(raw["prev_day"],  current_price)
        prev_week = self._parse_prev_week_structure(raw["prev_week"], current_price)
        compression = self._parse_4h_compression(raw["kline_4h"])

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
            # Market Imbalance Index: cross-exchange 15m, latest value + sustained bars
            **self._parse_mii(raw["market_imbalance"]),
            # Precision scalping signals
            "liq_levels": liq_levels,
            "volume_ratio": volume_ratio,
            "buy_sell_count_ratio": buy_sell_count,
            "liq_levels_size_delta": liq_levels_size,
            "liq_levels_count_delta": liq_levels_count,
            "cvd": cvd,
            "cvd_spot": cvd_spot,
            "oi_delta_pct": oi_delta,
            # Cumulative liq zone exposure (long vs short $ outstanding)
            **cumulative_liq_detail,
            # True retail + global accounts L/S positioning
            "true_retail_long_pct": true_retail["long_pct"],
            "true_retail_short_pct": true_retail["short_pct"],
            "global_accounts_long_pct": global_accts["long_pct"],
            "global_accounts_short_pct": global_accts["short_pct"],
            # Net long/short delta (overall positioning imbalance)
            "net_ls_delta": net_ls_delta,
            # Previous day levels + structure
            **prev_day,
            # Previous week levels + structure
            **prev_week,
            # 4H compression / volatility expansion detection
            "is_compressed": compression["is_compressed"],
            "compression_ratio": compression["compression_ratio"],
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

    def _parse_mii(self, raw_response: Dict) -> Dict:
        """
        Parse the full MII response (5 bars) into:
          market_imbalance_index  — latest bar value (-1 to +1)
          mii_sustained_bars      — consecutive hours the latest reading has held
                                    above mii_entry_threshold in the same direction
        """
        bars = raw_response.get("data", []) if isinstance(raw_response, dict) else []
        if not bars:
            return {"market_imbalance_index": 0.0, "mii_sustained_bars": 0}

        values = [float(b.get("marketImbalanceIndex", 0.0)) for b in bars]
        latest = values[-1]
        threshold = settings.mii_entry_threshold
        sign = 1.0 if latest >= 0 else -1.0

        sustained = 0
        for v in reversed(values):
            if sign * v >= threshold:
                sustained += 1
            else:
                break

        return {
            "market_imbalance_index": round(latest, 4),
            "mii_sustained_bars": sustained,
        }

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

        # ── Volume ratio oscillator (-1 to +1) ───────────────────────────────
        vr = float(data.get("volume_ratio") or 0.0)
        if abs(vr) > 0.05:
            vr_confirms = (direction == "LONG" and vr > 0) or (direction == "SHORT" and vr < 0)
            if vr_confirms:
                vr_pts = 8.0 if abs(vr) > 0.3 else 4.0
                score += vr_pts
                notes.append(f"volume ratio {'bullish' if vr > 0 else 'bearish'} ({vr:+.2f}, +{vr_pts:.0f})")
            else:
                score -= 3.0
                warnings.append(f"volume ratio contradicts {direction} ({vr:+.2f})")

        # ── Buy/sell trade count ratio (-1 to +1) ────────────────────────────
        bsr = float(data.get("buy_sell_count_ratio") or 0.0)
        if abs(bsr) > 0.05:
            bsr_confirms = (direction == "LONG" and bsr > 0) or (direction == "SHORT" and bsr < 0)
            if bsr_confirms:
                bsr_pts = 6.0 if abs(bsr) > 0.3 else 3.0
                score += bsr_pts
                notes.append(f"trade count ratio {'buy-heavy' if bsr > 0 else 'sell-heavy'} ({bsr:+.2f}, +{bsr_pts:.0f})")
            else:
                score -= 2.0
                warnings.append(f"trade count ratio contradicts {direction} ({bsr:+.2f})")

        # ── Cumulative Volume Delta ───────────────────────────────────────────
        # CVD = sum of volume delta bars: sustained positive → buyers in control,
        # sustained negative → sellers in control. Sign confirms direction;
        # magnitude calibrated once real Hyblock values are observed.
        cvd = float(data.get("cvd") or 0.0)
        if abs(cvd) > 0.01:  # deadband filters near-zero noise
            cvd_confirms = (direction == "LONG" and cvd > 0) or (direction == "SHORT" and cvd < 0)
            if cvd_confirms:
                cvd_pts = 8.0 if abs(cvd) > 2.0 else 5.0 if abs(cvd) > 0.5 else 3.0
                score += cvd_pts
                notes.append(f"CVD {'positive' if cvd > 0 else 'negative'} ({cvd:+.2f}, +{cvd_pts:.0f})")
            else:
                score -= 3.0
                warnings.append(f"CVD contradicts {direction} ({cvd:+.2f})")

        # ── Spot CVD vs Futures CVD divergence ───────────────────────────────
        # Spot CVD positive + Futures CVD negative = real money accumulating while
        # leverage sells → strongest divergence signal (smart money vs paper hands).
        # Spot CVD negative + Futures CVD positive = distribution while futures pile in.
        # Both aligned = confirms the direction (additional conviction).
        cvd_spot = float(data.get("cvd_spot") or 0.0)
        cvd_futures = float(data.get("cvd") or 0.0)
        _spot_buy = cvd_spot > 0.01
        _spot_sell = cvd_spot < -0.01
        _fut_buy = cvd_futures > 0.01
        _fut_sell = cvd_futures < -0.01
        _spot_meaningful = abs(cvd_spot) > 0.01

        if _spot_meaningful:
            if direction == "LONG":
                if _spot_buy and _fut_sell:
                    # Classic bullish divergence: spot accumulating, futures capitulating
                    score += 8.0
                    notes.append(f"spot CVD +{cvd_spot:.2f} vs futures CVD {cvd_futures:.2f} — bullish divergence (+8)")
                elif _spot_buy and _fut_buy:
                    # Both buying — extra conviction
                    score += 3.0
                    notes.append(f"spot+futures CVD both positive — aligned buying (+3)")
                elif _spot_sell:
                    score -= 4.0
                    warnings.append(f"spot CVD {cvd_spot:.2f} negative — real money selling contradicts LONG")
            elif direction == "SHORT":
                if _spot_sell and _fut_buy:
                    # Classic bearish divergence: spot distributing, futures piling in
                    score += 8.0
                    notes.append(f"spot CVD {cvd_spot:.2f} vs futures CVD +{cvd_futures:.2f} — bearish divergence (+8)")
                elif _spot_sell and _fut_sell:
                    # Both selling — extra conviction
                    score += 3.0
                    notes.append(f"spot+futures CVD both negative — aligned selling (+3)")
                elif _spot_buy:
                    score -= 4.0
                    warnings.append(f"spot CVD +{cvd_spot:.2f} positive — real money buying contradicts SHORT")

        # ── Open Interest Delta ───────────────────────────────────────────────
        # Rising OI + negative funding → new SHORT positions piling in = squeeze fuel → confirms LONG
        # Rising OI + positive funding → new LONG positions piling in = crowded long → confirms SHORT
        # Falling OI → de-leveraging; reduces confidence regardless of direction
        oi_delta = float(data.get("oi_delta_pct") or 0.0)
        fr = float(data.get("funding_rate_raw") or 0.0)
        if abs(oi_delta) > 2.0:
            if oi_delta > 0:
                # Determine who is opening: funding sign reveals the dominant side
                if direction == "LONG" and fr < -0.0001:
                    # Shorts piling in + negative funding = squeeze fuel for LONG
                    score += 7.0
                    notes.append(f"OI +{oi_delta:.1f}% + neg funding = short squeeze building (+7)")
                elif direction == "SHORT" and fr > 0.0001:
                    # Longs piling in + positive funding = fade opportunity for SHORT
                    score += 7.0
                    notes.append(f"OI +{oi_delta:.1f}% + pos funding = long overextension (+7)")
                elif direction == "LONG" and fr > 0.0001:
                    # Longs crowding in = bad for LONG (crowded trade)
                    score -= 3.0
                    warnings.append(f"OI rising + pos funding = longs crowding — contradicts LONG")
                elif direction == "SHORT" and fr < -0.0001:
                    # Shorts crowding in = bad for SHORT (crowded trade)
                    score -= 3.0
                    warnings.append(f"OI rising + neg funding = shorts crowding — contradicts SHORT")
            else:
                # OI falling = positions unwinding, trend losing momentum
                score -= 2.0
                warnings.append(f"OI declining {oi_delta:.1f}% — de-leveraging in progress")

        # ── Liquidation cluster proximity (price magnets) ─────────────────────
        # Uses nearest single cluster size (not sum of all clusters above/below).
        # Tier: <300 BTC = noise, 300-1000 BTC = +3, 1000-3000 BTC = +5, >3000 BTC = +8
        MIN_LIQ_BTC = settings.min_liq_cluster_btc
        liq = data.get("liq_clusters", {})
        if direction == "LONG":
            above_pct  = liq.get("above_pct")
            above_size = liq.get("above_size", 0.0) or 0.0
            if above_pct is not None and above_size >= MIN_LIQ_BTC:
                liq_pts = 8.0 if above_size > 3000 else 5.0 if above_size > 1000 else 3.0
                score += liq_pts
                notes.append(f"liq cluster {above_pct}% above ({above_size:.0f} BTC magnet +{liq_pts:.0f})")
        elif direction == "SHORT":
            below_pct  = liq.get("below_pct")
            below_size = liq.get("below_size", 0.0) or 0.0
            if below_pct is not None and below_size >= MIN_LIQ_BTC:
                liq_pts = 8.0 if below_size > 3000 else 5.0 if below_size > 1000 else 3.0
                score += liq_pts
                notes.append(f"liq cluster {below_pct}% below ({below_size:.0f} BTC magnet +{liq_pts:.0f})")

        # ── Market Imbalance Index ────────────────────────────────────────────
        # Combines cross-exchange orderflow + orderbook pressure; -1 to +1
        # Current bar: entry pressure (what is happening right now)
        # Sustained bars: how many consecutive hours MII has held above threshold
        mii = data.get("market_imbalance_index", 0.0)
        mii_sustained = data.get("mii_sustained_bars", 0)

        if mii > 0.3:
            if direction == "LONG":
                score += 6.0
                notes.append(f"MII bullish ({mii:+.2f})")
            else:
                score -= 4.0
                warnings.append(f"MII bullish ({mii:+.2f}) — contradicts SHORT")
        elif mii > 0.1:
            if direction == "LONG":
                score += 3.0
                notes.append(f"MII mildly bullish ({mii:+.2f})")
        elif mii < -0.3:
            if direction == "SHORT":
                score += 6.0
                notes.append(f"MII bearish ({mii:+.2f})")
            else:
                score -= 4.0
                warnings.append(f"MII bearish ({mii:+.2f}) — contradicts LONG")
        elif mii < -0.1:
            if direction == "SHORT":
                score += 3.0
                notes.append(f"MII mildly bearish ({mii:+.2f})")

        # Sustained pressure bonus — persistent multi-hour imbalance adds conviction
        mii_confirms = (direction == "LONG" and mii > 0) or (direction == "SHORT" and mii < 0)
        if mii_confirms:
            if mii_sustained >= 3:
                score += 10.0
                notes.append(f"MII sustained {mii_sustained}h (+10)")
            elif mii_sustained == 2:
                score += 5.0
                notes.append(f"MII sustained {mii_sustained}h (+5)")

        # ── Fragility ─────────────────────────────────────────────────────────
        # Skip if MII already shows directional pressure — avoid double-counting
        frag = data.get("fragility_level", "LOW")
        if abs(mii) <= 0.1:
            if frag == "HIGH":
                score -= 3.0
                warnings.append("high order-book fragility — elevated slippage risk")
            elif frag == "MEDIUM":
                score -= 1.0

        # ── Cascade risk (direction-aware) ────────────────────────────────────
        # HIGH cascade is not inherently bad — it means a big forced-liquidation
        # move is building. The key question is WHICH DIRECTION.
        #
        # We vote using two independent signals:
        #   cumulative_liq_bias  — total $ at risk each side (snapshot)
        #   liq_levels.cascade_direction — which exact cluster is bigger/closer
        #
        # Aligned = cascade primed to move WITH our trade → still volatile but
        #   directionally sound; small execution-risk penalty only.
        # Opposed = cascade primed to move AGAINST our trade → we are entering
        #   into an oncoming liquidation wave → hard block.
        # Mixed/unclear → elevated uncertainty penalty.
        cascade = data.get("cascade_risk", "LOW")
        _cum_bias    = data.get("cumulative_liq_bias", "BALANCED")
        _lv_casc_dir = (data.get("liq_levels") or {}).get("cascade_direction")

        _votes_for     = 0
        _votes_against = 0
        if direction == "LONG":
            if _cum_bias == "SHORT_HEAVY":    _votes_for     += 1
            elif _cum_bias == "LONG_HEAVY":   _votes_against += 1
            if _lv_casc_dir == "LONG":        _votes_for     += 1
            elif _lv_casc_dir == "SHORT":     _votes_against += 1
        elif direction == "SHORT":
            if _cum_bias == "LONG_HEAVY":     _votes_for     += 1
            elif _cum_bias == "SHORT_HEAVY":  _votes_against += 1
            if _lv_casc_dir == "SHORT":       _votes_for     += 1
            elif _lv_casc_dir == "LONG":      _votes_against += 1

        _casc_aligned = _votes_for > _votes_against
        _casc_opposed = _votes_against > _votes_for

        if cascade == "CRITICAL":
            should_block = True
            warnings.append("CRITICAL cascade risk — new entries blocked")
        elif cascade == "HIGH":
            if _casc_opposed:
                # Entering into an oncoming cascade on the wrong side — liquidation risk
                should_block = True
                warnings.append(
                    f"HIGH cascade risk AGAINST {direction} "
                    f"(bias={_cum_bias} casc_dir={_lv_casc_dir}) — blocked"
                )
            elif _casc_aligned:
                # Cascade primed for our direction — this IS our setup, just volatile
                score -= 3.0
                warnings.append(
                    f"HIGH cascade aligned with {direction} — directionally sound but volatile (-3)"
                )
            else:
                # Direction unclear — execution risk without edge
                score -= 8.0
                warnings.append("HIGH cascade risk — direction unclear, elevated uncertainty (-8)")
        elif cascade == "MEDIUM":
            if _casc_opposed:
                score -= 5.0
                warnings.append(f"MEDIUM cascade risk against {direction} (-5)")
            else:
                score -= 1.0
                warnings.append("MEDIUM cascade risk — slightly elevated volatility (-1)")

        # ── Liq levels size/count delta (cascade initiation signal) ──────────
        # liq_levels_size_delta  < 0 → liq positions shrinking (being triggered)
        # liq_levels_count_delta < 0 → fewer distinct levels (levels being hit)
        # Both negative + aligned = cascade is underway = highest conviction entry.
        # Either shrinking but opposing = wrong cascade is happening = danger.
        _lvl_sz_d  = float(data.get("liq_levels_size_delta")  or 0.0)
        _lvl_cnt_d = float(data.get("liq_levels_count_delta") or 0.0)
        _lv_casc_dir = data.get("liq_levels", {}).get("cascade_direction")
        _sz_shrinking  = _lvl_sz_d  < -0.005
        _cnt_shrinking = _lvl_cnt_d < -0.005
        _either_shrinking = _sz_shrinking or _cnt_shrinking
        _both_shrinking   = _sz_shrinking and _cnt_shrinking
        _lv_aligned = _lv_casc_dir == direction

        if _either_shrinking:
            if _lv_aligned:
                _lv_pts = 10.0 if _both_shrinking else 6.0
                score += _lv_pts
                _lv_state = "underway" if _both_shrinking else "starting"
                notes.append(
                    f"cascade {_lv_state} (sz_d={_lvl_sz_d:+.3f} cnt_d={_lvl_cnt_d:+.3f} +{_lv_pts:.0f})"
                )
            else:
                score -= 5.0
                warnings.append(
                    f"opposite cascade active (sz_d={_lvl_sz_d:+.3f} cnt_d={_lvl_cnt_d:+.3f} -5)"
                )
        elif abs(_lvl_sz_d) > 0.005 or abs(_lvl_cnt_d) > 0.005:
            notes.append(f"liq levels building (sz_d={_lvl_sz_d:+.3f} cnt_d={_lvl_cnt_d:+.3f})")

        # ── Cumulative liquidation zone bias ──────────────────────────────────
        # Total predicted short liq > long liq → more shorts will be forced to
        # buy back → upward cascade pressure → confirms LONG (and vice versa).
        cum_bias = data.get("cumulative_liq_bias", "BALANCED")
        if cum_bias == "SHORT_HEAVY":
            if direction == "LONG":
                score += 8.0
                notes.append(f"liq zone SHORT-heavy (short squeeze fuel +8)")
            else:
                score -= 4.0
                warnings.append("liq zone SHORT-heavy — contradicts SHORT")
        elif cum_bias == "LONG_HEAVY":
            if direction == "SHORT":
                score += 8.0
                notes.append(f"liq zone LONG-heavy (long cascade fuel +8)")
            else:
                score -= 4.0
                warnings.append("liq zone LONG-heavy — contradicts LONG")

        # ── True retail positioning (contrarian) ──────────────────────────────
        # Retail > 60% long → crowded long trade → fade = confirms SHORT
        # Retail > 60% short → crowded short → squeeze setup = confirms LONG
        retail_long = float(data.get("true_retail_long_pct") or 50.0)
        if retail_long > 60.0:
            if direction == "SHORT":
                score += 8.0
                notes.append(f"retail {retail_long:.1f}% long — crowded, fade confirms SHORT (+8)")
            else:
                score -= 5.0
                warnings.append(f"retail {retail_long:.1f}% long — crowded LONG trade")
        elif retail_long < 40.0:
            if direction == "LONG":
                score += 8.0
                notes.append(f"retail {retail_long:.1f}% long — crowded short, squeeze confirms LONG (+8)")
            else:
                score -= 5.0
                warnings.append(f"retail {retail_long:.1f}% long — crowded SHORT trade")
        elif retail_long > 55.0:
            if direction == "SHORT":
                score += 3.0
                notes.append(f"retail mildly long-leaning ({retail_long:.1f}%) — mild fade (+3)")
        elif retail_long < 45.0:
            if direction == "LONG":
                score += 3.0
                notes.append(f"retail mildly short-leaning ({retail_long:.1f}%) — mild squeeze (+3)")

        # ── Net long/short delta (overall positioning imbalance) ──────────────
        # Positive = more net longs outstanding (crowded → fade for LONG)
        # Negative = more net shorts outstanding (crowded → squeeze for LONG)
        nls = float(data.get("net_ls_delta") or 0.0)
        if abs(nls) > 0.05:
            nls_confirms = (direction == "LONG" and nls < 0) or (direction == "SHORT" and nls > 0)
            if nls_confirms:
                score += 5.0
                notes.append(f"net L/S delta confirms {direction} ({nls:+.3f}, +5)")
            else:
                score -= 3.0
                warnings.append(f"net L/S delta against {direction} ({nls:+.3f})")

        # ── Previous day structure ─────────────────────────────────────────────
        # Price accepted above PDH → bullish structural break → confirms LONG
        # Price rejected below PDL → bearish break → confirms SHORT
        pds = data.get("prev_day_structure", "BETWEEN")
        if pds == "ABOVE_PDH":
            if direction == "LONG":
                score += 8.0
                notes.append("price above PDH — bullish structure break (+8)")
            else:
                score -= 5.0
                warnings.append("price above PDH — counter-trend SHORT")
        elif pds == "BELOW_PDL":
            if direction == "SHORT":
                score += 8.0
                notes.append("price below PDL — bearish structure break (+8)")
            else:
                score -= 5.0
                warnings.append("price below PDL — counter-trend LONG")
        else:
            # Between PDH and PDL — neutral, small bonus for trading in PDO direction
            above_pdo = data.get("prev_day_above_pdo")
            if above_pdo is True and direction == "LONG":
                score += 3.0
            elif above_pdo is False and direction == "SHORT":
                score += 3.0

        # ── Previous week structure ───────────────────────────────────────────────
        # PWH/PWL = major institutional reference levels; breaking them weekly
        # confirms trend continuation; failing inside them signals range-bound.
        pws = data.get("prev_week_structure", "UNKNOWN")
        if pws == "ABOVE_PWH":
            if direction == "LONG":
                score += 6.0
                notes.append("price above PWH — bullish weekly break (+6)")
            else:
                score -= 4.0
                warnings.append("price above PWH — counter-trend SHORT vs weekly structure")
        elif pws == "BELOW_PWL":
            if direction == "SHORT":
                score += 6.0
                notes.append("price below PWL — bearish weekly break (+6)")
            else:
                score -= 4.0
                warnings.append("price below PWL — counter-trend LONG vs weekly structure")
        elif pws == "BETWEEN":
            above_pwo = data.get("prev_week_above_pwo")
            if above_pwo is True and direction == "LONG":
                score += 2.0
                notes.append("price above PW open — mild weekly bullish bias (+2)")
            elif above_pwo is False and direction == "SHORT":
                score += 2.0
                notes.append("price below PW open — mild weekly bearish bias (+2)")

        description = " | ".join(notes) if notes else "No strong Hyblock signals"
        return round(score, 1), description, warnings, should_block

    # ─── Derived metric helpers ───────────────────────────────────────────────

    def _merge_obi_surfaces(self, surfaces: List[Dict]) -> Dict:
        """Average OBI surfaces across multiple exchanges for cross-exchange consensus."""
        valid = [s for s in surfaces if s]
        if not valid:
            return {}
        totals: Dict = {}
        counts: Dict = {}
        for s in valid:
            for depth, val in s.items():
                totals[depth] = totals.get(depth, 0.0) + val
                counts[depth] = counts.get(depth, 0) + 1
        return {d: totals[d] / counts[d] for d in totals}

    def _merge_cumulative_liq(self, sources: List[Dict]) -> Dict:
        """Sum long/short cumulative liquidation sizes across exchanges, then compute bias."""
        total_long  = sum(float(s.get("totalLongLiquidationSize",  0) or 0) for s in sources if s)
        total_short = sum(float(s.get("totalShortLiquidationSize", 0) or 0) for s in sources if s)
        return self._parse_cumulative_liq_detail({
            "totalLongLiquidationSize":  total_long,
            "totalShortLiquidationSize": total_short,
        })

    def _merge_whale_sentiment(self, sources: List[Dict]) -> str:
        """Average whale delta across exchanges, then apply threshold."""
        deltas = []
        for d in sources:
            if not d:
                continue
            v = d.get("whaleRetailDelta", d.get("whale_delta", d.get("whaleDelta", d.get("delta"))))
            if v is not None:
                try:
                    deltas.append(float(v))
                except (TypeError, ValueError):
                    pass
        if not deltas:
            return "NEUTRAL"
        avg = sum(deltas) / len(deltas)
        if avg > 0.1:
            return "BULLISH"
        if avg < -0.1:
            return "BEARISH"
        return "NEUTRAL"

    def _merge_top_trader_sentiment(self, sources: List[Dict]) -> str:
        """Average top trader long_pct across exchanges, then apply threshold."""
        long_pcts = []
        for d in sources:
            if not d:
                continue
            lp = d.get("longPct", d.get("long_pct", d.get("longAccount")))
            if lp is not None:
                try:
                    long_pcts.append(float(lp))
                except (TypeError, ValueError):
                    pass
        if not long_pcts:
            return "NEUTRAL"
        avg = sum(long_pcts) / len(long_pcts)
        if avg > 60.0:
            return "BULLISH"
        if avg < 40.0:
            return "BEARISH"
        return "NEUTRAL"

    def _merge_retail_ratio(self, sources: List[Dict]) -> Dict:
        """Average long/short pct across multiple exchange sources."""
        long_pcts  = []
        short_pcts = []
        for d in sources:
            if not d:
                continue
            lp = d.get("longPct",  d.get("long_pct",  d.get("longAccount")))
            sp = d.get("shortPct", d.get("short_pct", d.get("shortAccount")))
            if lp is not None:
                try:
                    long_pcts.append(float(lp))
                except (TypeError, ValueError):
                    pass
            if sp is not None:
                try:
                    short_pcts.append(float(sp))
                except (TypeError, ValueError):
                    pass
        if not long_pcts:
            return self._parse_retail_ratio({})
        return {
            "long_pct":  round(sum(long_pcts)  / len(long_pcts),  2),
            "short_pct": round(sum(short_pcts) / len(short_pcts), 2) if short_pcts else round(100 - sum(long_pcts) / len(long_pcts), 2),
        }

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

        When the API returns only a single aggregate bid/ask (no per-depth data), all
        surface levels are identical and the regression slope would be 0. In that case
        return the OBI value directly — it's still a valid directional signal.
        """
        if len(surface) < 2:
            return 0.0
        xs = [float(k) for k in sorted(surface.keys())]
        ys = [surface[int(x)] for x in xs]
        if len(set(ys)) == 1:
            return ys[0]
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
                    "above_size": 0.0, "below_size": 0.0,
                    "above_wide_pct": None, "above_wide_size": 0.0,
                    "below_wide_pct": None, "below_wide_size": 0.0}

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
            # btcSize is already denominated in BTC — return directly
            btc = l.get("btcSize")
            if btc is not None:
                return float(btc)
            # All other size fields are in USD — convert to BTC
            long_s  = l.get("longLiquidations",    l.get("longLiquidationSize",  l.get("longSize",  0.0))) or 0.0
            short_s = l.get("shortLiquidations",   l.get("shortLiquidationSize", l.get("shortSize", 0.0))) or 0.0
            if long_s or short_s:
                return (float(long_s) + float(short_s)) / current_price
            for key in ("totalLiquidations", "liquidationSize", "size", "totalSize", "notional"):
                v = l.get(key)
                if v is not None:
                    return float(v) / current_price
            return 0.0

        above_pct = above_size = above_price_val = None
        below_pct = below_size = below_price_val = None

        # Only consider clusters within the configured max % range
        max_pct = settings.liq_cluster_max_pct / 100.0
        above_levels = [
            (get_px(l), get_size(l)) for l in levels
            if get_px(l) > current_price
            and (get_px(l) - current_price) / current_price <= max_pct
        ]
        below_levels = [
            (get_px(l), get_size(l)) for l in levels
            if 0 < get_px(l) < current_price
            and (current_price - get_px(l)) / current_price <= max_pct
        ]

        if above_levels:
            # Largest cluster within range — the true price magnet, not just the nearest
            best_above = max(above_levels, key=lambda x: x[1])
            above_price_val = round(best_above[0], 2)
            above_pct  = round((best_above[0] - current_price) / current_price * 100, 2)
            above_size = round(best_above[1], 2)
        if below_levels:
            # Largest cluster within range
            best_below = max(below_levels, key=lambda x: x[1])
            below_price_val = round(best_below[0], 2)
            below_pct  = round((current_price - best_below[0]) / current_price * 100, 2)
            below_size = round(best_below[1], 2)

        if above_pct is not None and below_pct is not None:
            nearest_side = "ABOVE" if above_pct < below_pct else "BELOW"
        elif above_pct is not None:
            nearest_side = "ABOVE"
        elif below_pct is not None:
            nearest_side = "BELOW"
        else:
            nearest_side = None

        # Wide search: largest non-zero cluster anywhere in the heatmap, each direction.
        # No range cap — nearby clusters are often wiped out after a price move;
        # the meaningful zones may be 15-30%+ away. Dashboard display only.
        wide_above_pct = wide_above_size = None
        wide_below_pct = wide_below_size = None
        wide_above = [
            (get_px(l), get_size(l)) for l in levels
            if get_px(l) > current_price and get_size(l) > 0
        ]
        wide_below = [
            (get_px(l), get_size(l)) for l in levels
            if 0 < get_px(l) < current_price and get_size(l) > 0
        ]
        if wide_above:
            best = max(wide_above, key=lambda x: x[1])
            wide_above_pct  = round((best[0] - current_price) / current_price * 100, 2)
            wide_above_size = round(best[1], 2)
        if wide_below:
            best = max(wide_below, key=lambda x: x[1])
            wide_below_pct  = round((current_price - best[0]) / current_price * 100, 2)
            wide_below_size = round(best[1], 2)

        return {
            "above_pct":       above_pct,
            "above_price":     above_price_val,
            "below_pct":       below_pct,
            "below_price":     below_price_val,
            "nearest_side":    nearest_side,
            "above_size":      above_size or 0.0,
            "below_size":      below_size or 0.0,
            "above_wide_pct":  wide_above_pct,
            "above_wide_size": wide_above_size or 0.0,
            "below_wide_pct":  wide_below_pct,
            "below_wide_size": wide_below_size or 0.0,
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

    def _merge_liq_levels(self, sources: List[Dict]) -> Dict:
        """
        Merge liq level results from multiple exchanges.
        Picks the largest qualifying cluster per direction across all sources.
        Better exchange coverage = less likely to miss a real cluster at the gate.
        """
        best_long:  Dict = {"pct": None, "size": 0.0, "price": None}
        best_short: Dict = {"pct": None, "size": 0.0, "price": None}

        for src in sources:
            if src.get("long_cluster_pct") is not None:
                sz = src.get("long_cluster_size") or 0.0
                if sz > best_long["size"]:
                    best_long = {
                        "pct":   src["long_cluster_pct"],
                        "size":  sz,
                        "price": src.get("long_cluster_price"),
                    }
            if src.get("short_cluster_pct") is not None:
                sz = src.get("short_cluster_size") or 0.0
                if sz > best_short["size"]:
                    best_short = {
                        "pct":   src["short_cluster_pct"],
                        "size":  sz,
                        "price": src.get("short_cluster_price"),
                    }

        has_long  = best_long["pct"]  is not None
        has_short = best_short["pct"] is not None

        if has_long and has_short:
            long_score  = best_long["size"]  / (best_long["pct"]  or 0.001)
            short_score = best_short["size"] / (best_short["pct"] or 0.001)
            cascade_dir = "SHORT" if long_score >= short_score else "LONG"
        elif has_long:
            cascade_dir = "SHORT"
        elif has_short:
            cascade_dir = "LONG"
        else:
            cascade_dir = None

        return {
            "cascade_direction":   cascade_dir,
            "long_cluster_pct":   best_long["pct"],
            "long_cluster_size":  best_long["size"],
            "long_cluster_price": best_long["price"],
            "short_cluster_pct":  best_short["pct"],
            "short_cluster_size": best_short["size"],
            "short_cluster_price": best_short["price"],
        }

    def _parse_liq_levels(self, data: Dict, current_price: float) -> Dict:
        """
        Parse exact per-price liquidation level data to determine cascade direction.

        Cascade mechanics:
          LONG cluster BELOW price → if hit, longs are sold → price falls → SHORT entry
          SHORT cluster ABOVE price → if hit, shorts buy back → price rises → LONG entry

        Returns cascade_direction + nearest significant cluster details for each side.
        """
        levels = data.get("data") or data.get("levels") or data.get("liquidationLevels") or []
        if isinstance(levels, dict):
            levels = list(levels.values())
        if not levels or current_price <= 0:
            return {
                "cascade_direction": None,
                "long_cluster_pct": None, "long_cluster_size": 0.0, "long_cluster_price": None,
                "short_cluster_pct": None, "short_cluster_size": 0.0, "short_cluster_price": None,
            }

        max_pct = settings.liq_cluster_max_pct / 100.0
        min_btc = settings.min_liq_cluster_btc

        def _px(l: Dict) -> float:
            for key in ("price", "priceLevel", "price_level", "liqPrice", "liquidationPrice"):
                v = l.get(key)
                if v is not None:
                    return float(v)
            return 0.0

        long_below: List[Tuple[float, float, float]] = []
        short_above: List[Tuple[float, float, float]] = []

        for lv in levels:
            px = _px(lv)
            if px <= 0:
                continue
            side = lv.get("side", "")
            # API returns size in USD — convert to BTC; btcSize is already BTC
            raw_size = float(lv.get("btcSize", 0.0) or 0.0)
            if not raw_size:
                usd = float(lv.get("size", 0.0) or 0.0)
                if not usd:
                    usd = float(lv.get("longLiquidations", lv.get("longLiquidationSize",
                        lv.get("shortLiquidations", lv.get("shortLiquidationSize", 0.0)))) or 0.0)
                raw_size = usd / current_price if current_price > 0 else 0.0
            size = raw_size

            is_long_cluster  = (side == "long")  or (not side and px < current_price)
            is_short_cluster = (side == "short") or (not side and px > current_price)

            if is_long_cluster and px < current_price:
                pct = (current_price - px) / current_price
                if pct <= max_pct and size >= min_btc:
                    long_below.append((px, size, pct))
            elif is_short_cluster and px > current_price:
                pct = (px - current_price) / current_price
                if pct <= max_pct and size >= min_btc:
                    short_above.append((px, size, pct))

        best_long = max(long_below, key=lambda x: x[1]) if long_below else None
        best_short = max(short_above, key=lambda x: x[1]) if short_above else None

        long_pct  = round(best_long[2] * 100, 2)  if best_long  else None
        short_pct = round(best_short[2] * 100, 2) if best_short else None
        long_sz   = round(best_long[1], 2)  if best_long  else 0.0
        short_sz  = round(best_short[1], 2) if best_short else 0.0

        has_long  = best_long  is not None
        has_short = best_short is not None

        if has_long and has_short:
            # Both present — score by size/proximity; bigger relative signal wins
            long_score  = long_sz  / (long_pct  or 0.001)
            short_score = short_sz / (short_pct or 0.001)
            cascade_dir = "SHORT" if long_score >= short_score else "LONG"
        elif has_long:
            cascade_dir = "SHORT"
        elif has_short:
            cascade_dir = "LONG"
        else:
            cascade_dir = None

        return {
            "cascade_direction": cascade_dir,
            "long_cluster_pct":   long_pct,
            "long_cluster_size":  long_sz,
            "long_cluster_price": round(best_long[0], 2)  if best_long  else None,
            "short_cluster_pct":  short_pct,
            "short_cluster_size": short_sz,
            "short_cluster_price": round(best_short[0], 2) if best_short else None,
        }

    def _parse_scalar(self, data: Dict, keys: tuple) -> float:
        """Extract a scalar float from a data dict by trying keys in order."""
        for key in keys:
            v = data.get(key)
            if v is not None:
                try:
                    return round(float(v), 4)
                except (TypeError, ValueError):
                    pass
        return 0.0

    def _parse_cvd(self, data: Dict) -> float:
        """
        Cumulative Volume Delta = sum of volumeDelta values across all returned bars.
        Positive = sustained buying pressure; negative = sustained selling pressure.
        Returns the raw summed value (units match Hyblock's volumeDelta field).
        """
        bars = data.get("data", []) if isinstance(data, dict) else []
        if not bars:
            return 0.0
        total = 0.0
        for b in bars:
            if not isinstance(b, dict):
                continue
            v = b.get("volumeDelta", b.get("delta", b.get("value", 0.0)))
            try:
                total += float(v or 0.0)
            except (TypeError, ValueError):
                pass
        return round(total, 4)

    def _parse_oi_delta(self, data: Dict) -> float:
        """
        Open Interest rate of change as a percentage from the first bar's close to
        the last bar's close across the fetched window.
        Positive = OI growing (new leverage entering); negative = OI falling (unwinding).
        """
        bars = data.get("data", []) if isinstance(data, dict) else []
        if len(bars) < 2:
            return 0.0
        def _bar_val(b: Dict) -> float:
            for key in ("close", "value", "open"):
                v = b.get(key)
                if v is not None:
                    try:
                        return float(v)
                    except (TypeError, ValueError):
                        pass
            return 0.0
        first_val = _bar_val(bars[0])
        last_val  = _bar_val(bars[-1])
        if first_val <= 0:
            return 0.0
        return round((last_val - first_val) / first_val * 100, 2)

    def _parse_cumulative_liq_detail(self, data: Dict) -> Dict:
        """
        Parse cumulative liq level snapshot into long/short exposure and directional bias.
        SHORT_HEAVY = more shorts at risk of liquidation → price magnet UP → confirms LONG.
        LONG_HEAVY  = more longs at risk               → price magnet DOWN → confirms SHORT.
        """
        long_sz  = float(data.get("totalLongLiquidationSize",  0.0) or 0.0)
        short_sz = float(data.get("totalShortLiquidationSize", 0.0) or 0.0)
        total    = long_sz + short_sz
        if total > 0:
            delta = short_sz - long_sz
            ratio = delta / total  # positive = short-heavy
            if ratio > 0.15:
                bias = "SHORT_HEAVY"
            elif ratio < -0.15:
                bias = "LONG_HEAVY"
            else:
                bias = "BALANCED"
        else:
            delta = 0.0
            bias  = "BALANCED"
        return {
            "cumulative_liq_long_size":  round(long_sz, 2),
            "cumulative_liq_short_size": round(short_sz, 2),
            "cumulative_liq_delta":      round(short_sz - long_sz, 2),
            "cumulative_liq_bias":       bias,
        }

    def _parse_retail_ratio(self, data: Dict) -> Dict:
        """Parse long/short percentage from trueRetailLongShort or globalAccounts."""
        long_pct  = float(data.get("longPct",  data.get("long_pct",  data.get("longAccount",  50.0))) or 50.0)
        short_pct = float(data.get("shortPct", data.get("short_pct", data.get("shortAccount", 50.0))) or 50.0)
        return {"long_pct": round(long_pct, 2), "short_pct": round(short_pct, 2)}

    def _parse_net_ls_delta(self, data: Dict) -> float:
        """Net long/short delta — positive = more net longs, negative = more net shorts."""
        v = data.get("netLongShortDelta", data.get("delta", data.get("value", 0.0)))
        try:
            return round(float(v or 0.0), 4)
        except (TypeError, ValueError):
            return 0.0

    def _parse_prev_day_structure(self, data: Dict, current_price: float) -> Dict:
        """
        Parse previous day levels and determine price structure.
        ABOVE_PDH = bullish structural break; BELOW_PDL = bearish break; BETWEEN = neutral.
        pdEq = equilibrium midpoint (avg of high and low) — key mean-reversion target.
        """
        pdh  = float(data.get("pdHigh", data.get("pd_high", data.get("high", 0.0))) or 0.0)
        pdl  = float(data.get("pdLow",  data.get("pd_low",  data.get("low",  0.0))) or 0.0)
        pdo  = float(data.get("pdOpen", data.get("pd_open", data.get("open", 0.0))) or 0.0)
        pdeq = float(data.get("pdEq",   data.get("pd_eq",   0.0)) or 0.0)
        if not pdeq and pdh > 0 and pdl > 0:
            pdeq = (pdh + pdl) / 2.0  # compute locally if API omits it
        if pdh <= 0 or pdl <= 0 or current_price <= 0:
            return {"prev_day_high": None, "prev_day_low": None, "prev_day_open": None,
                    "prev_day_eq": None, "prev_day_structure": "UNKNOWN"}
        if current_price > pdh:
            structure = "ABOVE_PDH"
        elif current_price < pdl:
            structure = "BELOW_PDL"
        else:
            structure = "BETWEEN"
        return {
            "prev_day_high":      round(pdh, 2),
            "prev_day_low":       round(pdl, 2),
            "prev_day_open":      round(pdo, 2),
            "prev_day_eq":        round(pdeq, 2) if pdeq else None,
            "prev_day_structure": structure,
            "prev_day_above_pdo": (current_price > pdo) if pdo > 0 else None,
        }

    def _parse_prev_week_structure(self, data: Dict, current_price: float) -> Dict:
        """
        Parse previous week levels and determine price structure vs weekly range.
        ABOVE_PWH = bullish weekly break; BELOW_PWL = bearish weekly break; BETWEEN = neutral.
        pwEq = equilibrium midpoint (avg of high and low).
        """
        pwh  = float(data.get("pwHigh", 0.0) or 0.0)
        pwl  = float(data.get("pwLow",  0.0) or 0.0)
        pwo  = float(data.get("pwOpen", 0.0) or 0.0)
        pweq = float(data.get("pwEq",   0.0) or 0.0)
        if not pweq and pwh > 0 and pwl > 0:
            pweq = (pwh + pwl) / 2.0
        if pwh <= 0 or pwl <= 0 or current_price <= 0:
            return {"prev_week_high": None, "prev_week_low": None, "prev_week_open": None,
                    "prev_week_eq": None, "prev_week_structure": "UNKNOWN"}
        if current_price > pwh:
            structure = "ABOVE_PWH"
        elif current_price < pwl:
            structure = "BELOW_PWL"
        else:
            structure = "BETWEEN"
        return {
            "prev_week_high":      round(pwh, 2),
            "prev_week_low":       round(pwl, 2),
            "prev_week_open":      round(pwo, 2),
            "prev_week_eq":        round(pweq, 2) if pweq else None,
            "prev_week_structure": structure,
            "prev_week_above_pwo": (current_price > pwo) if pwo > 0 else None,
        }

    def _parse_4h_compression(self, data: Dict) -> Dict:
        """
        Detect 4H range compression (ATR squeeze before volatility expansion).
        Compares the most recent 4H candle range to the 20-bar average.
        compression_ratio < 0.6 = compressed; is_compressed = True.
        """
        bars = data.get("data", []) if isinstance(data, dict) else []
        if len(bars) < 5:
            return {"is_compressed": False, "compression_ratio": 1.0}
        ranges = []
        for b in bars:
            if not isinstance(b, dict):
                continue
            h = float(b.get("high", b.get("ha_high", 0.0)) or 0.0)
            l = float(b.get("low",  b.get("ha_low",  0.0)) or 0.0)
            if h > l > 0:
                ranges.append(h - l)
        if len(ranges) < 5:
            return {"is_compressed": False, "compression_ratio": 1.0}
        avg_range     = sum(ranges[:-1]) / len(ranges[:-1])
        current_range = ranges[-1]
        ratio = round(current_range / avg_range, 3) if avg_range > 0 else 1.0
        return {"is_compressed": ratio < 0.6, "compression_ratio": ratio}
