"""
Spot Order Flow Monitor
Tracks real BTC buy/sell pressure across Binance, Coinbase, and Kraken spot markets.

Spot order flow reflects real money — institutions and whales placing actual buy/sell walls.
Futures reflects speculation and leverage. When they diverge, the spot side usually wins.

Whale wall detection is an approximation (large order book levels ≥$5M) and is
deliberately scored with low weight — it is informational, not a primary signal.
"""
import asyncio
import time
import httpx
from typing import Dict, List, Optional, Tuple
from loguru import logger


class SpotOrderFlowMonitor:

    WALL_THRESHOLD_USD  = 1_000_000   # $1M+ = significant spot wall
    WHALE_THRESHOLD_USD = 5_000_000   # $5M+ = whale wall approximation

    PRESSURE_WINDOW_PCT = 0.02        # ±2% price window for bid/ask imbalance
    WALL_WINDOW_PCT     = 0.05        # ±5% window to look for walls
    CACHE_SECONDS       = 30          # Order books refresh every 30s

    # Score weights
    PRESSURE_SCORE   = 6.0    # Real bid/ask imbalance — meaningful signal
    DIVERGENCE_SCORE = 4.0    # Spot vs futures divergence — meaningful signal
    WHALE_WALL_SCORE = 2.0    # Approximation only — intentionally low weight

    BINANCE_URL  = "https://api.binance.com"
    COINBASE_URL = "https://api.exchange.coinbase.com"
    KRAKEN_URL   = "https://api.kraken.com"

    def __init__(self):
        self._cache: Optional[Dict] = None
        self._cache_time: float = 0
        self._cached_books: List[Dict] = []

    # ─────────────────────────────────────────────────────────────────
    # Data Fetching
    # ─────────────────────────────────────────────────────────────────

    async def _fetch_binance_depth(self, client: httpx.AsyncClient) -> Optional[Dict]:
        try:
            resp = await client.get(
                f"{self.BINANCE_URL}/api/v3/depth",
                params={"symbol": "BTCUSDT", "limit": 1000},
            )
            data = resp.json()
            return {
                "exchange": "binance",
                "bids": [[float(p), float(q)] for p, q in data.get("bids", [])],
                "asks": [[float(p), float(q)] for p, q in data.get("asks", [])],
            }
        except Exception as e:
            logger.debug(f"Binance spot depth failed: {e}")
            return None

    async def _fetch_coinbase_depth(self, client: httpx.AsyncClient) -> Optional[Dict]:
        try:
            resp = await client.get(
                f"{self.COINBASE_URL}/products/BTC-USD/book",
                params={"level": "2"},
            )
            data = resp.json()
            return {
                "exchange": "coinbase",
                "bids": [[float(row[0]), float(row[1])] for row in data.get("bids", [])],
                "asks": [[float(row[0]), float(row[1])] for row in data.get("asks", [])],
            }
        except Exception as e:
            logger.debug(f"Coinbase spot depth failed: {e}")
            return None

    async def _fetch_kraken_depth(self, client: httpx.AsyncClient) -> Optional[Dict]:
        try:
            resp = await client.get(
                f"{self.KRAKEN_URL}/0/public/Depth",
                params={"pair": "XBTUSD", "count": 500},
            )
            data = resp.json()
            book = (data.get("result") or {}).get("XXBTZUSD") or {}
            return {
                "exchange": "kraken",
                "bids": [[float(row[0]), float(row[1])] for row in book.get("bids", [])],
                "asks": [[float(row[0]), float(row[1])] for row in book.get("asks", [])],
            }
        except Exception as e:
            logger.debug(f"Kraken spot depth failed: {e}")
            return None

    # ─────────────────────────────────────────────────────────────────
    # Analysis
    # ─────────────────────────────────────────────────────────────────

    def _find_walls(self, books: List[Dict], current_price: float) -> List[Dict]:
        """
        Identify significant bid/ask walls within ±5% of current price.
        Aggregates USD across all exchanges at $100 price bands so a wall
        split across exchanges registers as one combined wall.
        """
        bin_size = 100.0
        price_lo = current_price * (1 - self.WALL_WINDOW_PCT)
        price_hi = current_price * (1 + self.WALL_WINDOW_PCT)

        bid_bands: Dict[float, float] = {}
        ask_bands: Dict[float, float] = {}

        for book in books:
            for price, qty in book.get("bids", []):
                if price_lo <= price < current_price:
                    band = round(price / bin_size) * bin_size
                    bid_bands[band] = bid_bands.get(band, 0) + price * qty
            for price, qty in book.get("asks", []):
                if current_price < price <= price_hi:
                    band = round(price / bin_size) * bin_size
                    ask_bands[band] = ask_bands.get(band, 0) + price * qty

        walls = []
        for band, usd in bid_bands.items():
            if usd >= self.WALL_THRESHOLD_USD:
                walls.append({
                    "price": band,
                    "usd_size": round(usd),
                    "side": "bid",
                    "is_whale": usd >= self.WHALE_THRESHOLD_USD,
                    "label": f"${band/1000:.1f}k bid ({usd/1e6:.1f}M)",
                })
        for band, usd in ask_bands.items():
            if usd >= self.WALL_THRESHOLD_USD:
                walls.append({
                    "price": band,
                    "usd_size": round(usd),
                    "side": "ask",
                    "is_whale": usd >= self.WHALE_THRESHOLD_USD,
                    "label": f"${band/1000:.1f}k ask ({usd/1e6:.1f}M)",
                })

        return sorted(walls, key=lambda x: x["usd_size"], reverse=True)

    def _compute_pressure(self, books: List[Dict], current_price: float) -> Dict:
        """
        Bid vs ask pressure within ±2% of current price.
        Ratio > 1.3 = buyers winning, < 0.77 = sellers winning.
        """
        price_lo = current_price * (1 - self.PRESSURE_WINDOW_PCT)
        price_hi = current_price * (1 + self.PRESSURE_WINDOW_PCT)

        bid_usd = 0.0
        ask_usd = 0.0

        for book in books:
            for price, qty in book.get("bids", []):
                if price_lo <= price < current_price:
                    bid_usd += price * qty
            for price, qty in book.get("asks", []):
                if current_price <= price <= price_hi:
                    ask_usd += price * qty

        ratio = bid_usd / ask_usd if ask_usd > 0 else 1.0

        if ratio > 1.3:
            pressure = "BUY"
        elif ratio < 0.77:
            pressure = "SELL"
        else:
            pressure = "NEUTRAL"

        return {
            "bid_usd": round(bid_usd),
            "ask_usd": round(ask_usd),
            "ratio": round(ratio, 3),
            "pressure": pressure,
        }

    def _compute_divergence(self, pressure: str, oi_trend: str) -> str:
        """
        Spot vs futures divergence per document logic:
          Spot buying  + OI rising  = ALIGNED_BULLISH  (real demand + speculation building)
          Spot buying  + OI falling = DIVERGENT_BULLISH (real demand vs leverage unwind — often bullish)
          Spot selling + OI falling = ALIGNED_BEARISH  (real selling + speculation unwinding)
          Spot selling + OI rising  = DIVERGENT_BEARISH (real selling vs leverage mania — often bearish)
        """
        if pressure == "BUY" and oi_trend == "RISING":
            return "ALIGNED_BULLISH"
        if pressure == "BUY" and oi_trend == "FALLING":
            return "DIVERGENT_BULLISH"
        if pressure == "SELL" and oi_trend == "FALLING":
            return "ALIGNED_BEARISH"
        if pressure == "SELL" and oi_trend == "RISING":
            return "DIVERGENT_BEARISH"
        return "NEUTRAL"

    # ─────────────────────────────────────────────────────────────────
    # Main Fetch
    # ─────────────────────────────────────────────────────────────────

    async def fetch_all(self, current_price: float, oi_trend: str = "FLAT") -> Dict:
        """Fetch spot order books from all exchanges and return unified analysis."""
        # Use cached books but recompute price-dependent values
        if self._cache and (time.time() - self._cache_time) < self.CACHE_SECONDS:
            books = self._cached_books
            pressure = self._compute_pressure(books, current_price)
            walls    = self._find_walls(books, current_price)
            return {
                **self._cache,
                "pressure":    pressure,
                "walls":       walls[:10],
                "whale_walls": [w for w in walls if w["is_whale"]][:3],
                "divergence":  self._compute_divergence(pressure["pressure"], oi_trend),
            }

        try:
            async with httpx.AsyncClient(timeout=6.0) as client:
                results = await asyncio.gather(
                    self._fetch_binance_depth(client),
                    self._fetch_coinbase_depth(client),
                    self._fetch_kraken_depth(client),
                    return_exceptions=True,
                )

            books     = [r for r in results if r and not isinstance(r, Exception)]
            exchanges = [b["exchange"] for b in books]
            pressure  = self._compute_pressure(books, current_price)
            walls     = self._find_walls(books, current_price)
            divergence = self._compute_divergence(pressure["pressure"], oi_trend)

            result = {
                "exchanges":      exchanges,
                "exchange_count": len(exchanges),
                "pressure":       pressure,
                "walls":          walls[:10],
                "whale_walls":    [w for w in walls if w["is_whale"]][:3],
                "divergence":     divergence,
                "available":      len(books) > 0,
            }

            self._cached_books = books
            self._cache = {k: v for k, v in result.items() if k not in ("pressure", "walls", "whale_walls", "divergence")}
            self._cache_time = time.time()
            return result

        except Exception as e:
            logger.warning(f"Spot order flow fetch failed: {e}")
            return {
                "exchanges": [], "exchange_count": 0,
                "pressure":  {"bid_usd": 0, "ask_usd": 0, "ratio": 1.0, "pressure": "NEUTRAL"},
                "walls": [], "whale_walls": [],
                "divergence": "NEUTRAL",
                "available": False,
            }

    # ─────────────────────────────────────────────────────────────────
    # Signal
    # ─────────────────────────────────────────────────────────────────

    def get_trade_context(
        self,
        direction: str,
        current_price: float,
        analysis: Dict,
    ) -> Tuple[float, str]:
        """
        Return (score_delta, description).

        Max contributions:
          Pressure confirms direction:  +6
          Divergence confirms direction: +4
          Whale wall confirms direction: +2  (kept low — approximation only)
          Pressure against direction:   -3  (caution, not a block)
        """
        if not analysis.get("available"):
            return 0.0, "Spot flow unavailable"

        pressure    = analysis.get("pressure", {}).get("pressure", "NEUTRAL")
        ratio       = analysis.get("pressure", {}).get("ratio", 1.0)
        divergence  = analysis.get("divergence", "NEUTRAL")
        whale_walls = analysis.get("whale_walls", [])

        score = 0.0
        parts = []

        # Bid/ask pressure
        if direction == "LONG" and pressure == "BUY":
            score += self.PRESSURE_SCORE
            parts.append(f"spot bid pressure {ratio:.2f}x confirms LONG")
        elif direction == "SHORT" and pressure == "SELL":
            score += self.PRESSURE_SCORE
            parts.append(f"spot ask pressure {ratio:.2f}x confirms SHORT")
        elif direction == "LONG" and pressure == "SELL":
            score -= 3.0
            parts.append("spot ask-heavy — caution for LONG")
        elif direction == "SHORT" and pressure == "BUY":
            score -= 3.0
            parts.append("spot bid-heavy — caution for SHORT")

        # Spot vs futures divergence
        bullish_div = divergence in ("ALIGNED_BULLISH", "DIVERGENT_BULLISH")
        bearish_div = divergence in ("ALIGNED_BEARISH", "DIVERGENT_BEARISH")
        if direction == "LONG" and bullish_div:
            score += self.DIVERGENCE_SCORE
            parts.append(f"spot/futures {divergence.lower().replace('_', ' ')}")
        elif direction == "SHORT" and bearish_div:
            score += self.DIVERGENCE_SCORE
            parts.append(f"spot/futures {divergence.lower().replace('_', ' ')}")

        # Whale wall approximation — low weight as designed
        for w in whale_walls[:2]:
            if direction == "LONG" and w["side"] == "bid" and w["price"] < current_price:
                score += self.WHALE_WALL_SCORE
                parts.append(w["label"])
                break
            if direction == "SHORT" and w["side"] == "ask" and w["price"] > current_price:
                score += self.WHALE_WALL_SCORE
                parts.append(w["label"])
                break

        desc = " | ".join(parts) if parts else f"Spot flow neutral (pressure={pressure})"
        return score, desc
