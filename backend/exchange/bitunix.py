"""
Bitunix Futures API Client
Handles all exchange communication including orders, positions, and market data.
"""
import hashlib
import hmac
import time
import asyncio
from typing import Optional, Dict, Any, List
import httpx
from loguru import logger
from backend.config import settings


class BitunixClient:
    def __init__(self, api_key: str = None, api_secret: str = None):
        self.api_key = api_key or settings.bitunix_api_key
        self.api_secret = api_secret or settings.bitunix_api_secret
        self.base_url = settings.bitunix_base_url
        self.symbol = "BTCUSDT"
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(15.0),
                headers={"Content-Type": "application/json"}
            )
        return self._client

    def _build_signed_headers(self, query_str: str = "", body: str = "") -> Dict[str, str]:
        import uuid
        timestamp = str(int(time.time() * 1000))
        nonce = uuid.uuid4().hex  # random 32-char string as required by Bitunix docs
        # Bitunix double-SHA256:
        # step1 = SHA256(nonce + timestamp + apiKey + queryString + body)
        # sign  = SHA256(step1 + secretKey)
        digest_input = nonce + timestamp + self.api_key + query_str + body
        digest = hashlib.sha256(digest_input.encode("utf-8")).hexdigest()
        sign = hashlib.sha256((digest + self.api_secret).encode("utf-8")).hexdigest()
        return {
            "api-key": self.api_key,
            "timestamp": timestamp,
            "nonce": nonce,
            "sign": sign,
            "Content-Type": "application/json",
        }

    async def _get(self, path: str, params: Dict = None) -> Dict:
        client = await self._get_client()
        try:
            resp = await client.get(path, params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"GET {path} failed: {e}")
            raise

    async def _get_signed(self, path: str, params: Dict) -> Dict:
        client = await self._get_client()
        # Bitunix: sort keys ascending ASCII, concatenate as key1value1key2value2 (no = or &)
        query_str = "".join(f"{k}{v}" for k, v in sorted(params.items()))
        headers = self._build_signed_headers(query_str=query_str)
        try:
            resp = await client.get(path, params=params, headers=headers)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"GET signed {path} failed: {e}")
            raise

    async def _post_signed(self, path: str, body: Dict) -> Dict:
        import json
        client = await self._get_client()
        body_str = json.dumps(body, separators=(",", ":"))
        headers = self._build_signed_headers(body=body_str)
        try:
            resp = await client.post(path, content=body_str, headers=headers)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"POST {path} failed: {e}")
            raise

    # ─────────────────────────────────────────────
    # Market Data
    # ─────────────────────────────────────────────

    async def get_klines(self, interval: str, limit: int = 200) -> List[Dict]:
        """
        Fetch OHLCV klines.
        interval: '1m', '5m', '15m', '1h', '4h', '6h', '1d'
        Returns list of {open_time, open, high, low, close, volume}
        """
        # Bitunix uses /api/v1/futures/market/kline
        path = "/api/v1/futures/market/kline"
        params = {
            "symbol": self.symbol,
            "interval": interval,
            "limit": limit,
        }
        try:
            data = await self._get(path, params)
            if isinstance(data, dict) and "data" in data:
                raw = data["data"]
            else:
                raw = data
            candles = []
            for c in raw:
                if isinstance(c, list):
                    candles.append({
                        "open_time": int(c[0]),
                        "open": float(c[1]),
                        "high": float(c[2]),
                        "low": float(c[3]),
                        "close": float(c[4]),
                        "volume": float(c[5]),
                    })
                elif isinstance(c, dict):
                    # Bitunix kline dict fields: time, open, high, low, close, baseVol, quoteVol
                    candles.append({
                        "open_time": int(c.get("time", c.get("openTime", c.get("ts", 0)))),
                        "open": float(c.get("open", 0)),
                        "high": float(c.get("high", 0)),
                        "low": float(c.get("low", 0)),
                        "close": float(c.get("close", 0)),
                        "volume": float(c.get("baseVol", c.get("volume", 0))),
                    })
            return sorted(candles, key=lambda x: x["open_time"])
        except Exception as e:
            logger.warning(f"Failed to get klines from Bitunix, using fallback: {e}")
            return await self._get_klines_fallback(interval, limit)

    async def _get_klines_fallback(self, interval: str, limit: int) -> List[Dict]:
        """Fallback to Binance public API for klines if Bitunix fails."""
        interval_map = {
            "1m": "1m", "5m": "5m", "15m": "15m",
            "1h": "1h", "4h": "4h", "6h": "6h", "1d": "1d"
        }
        binance_interval = interval_map.get(interval, "1h")
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"https://fapi.binance.com/fapi/v1/klines",
                params={"symbol": "BTCUSDT", "interval": binance_interval, "limit": limit}
            )
            resp.raise_for_status()
            raw = resp.json()
            return [
                {
                    "open_time": int(c[0]),
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": float(c[5]),
                }
                for c in raw
            ]

    async def get_ticker(self) -> Dict:
        """Get current mark price using /tickers endpoint (filter by symbol)."""
        path = "/api/v1/futures/market/tickers"
        try:
            data = await self._get(path, {})
            tickers = data.get("data", []) if isinstance(data, dict) else []
            ticker = next((t for t in tickers if t.get("symbol") == self.symbol), None)
            if ticker:
                price = float(ticker.get("markPrice") or ticker.get("lastPrice") or 0)
                return {
                    "price": price,
                    "mark_price": price,
                    "index_price": price,
                    "funding_rate": 0.0,  # not in /tickers response; FundingRateMonitor fetches externally
                }
            raise ValueError(f"{self.symbol} not found in tickers")
        except Exception as e:
            logger.warning(f"Failed to get ticker from Bitunix: {e}")
            return await self._get_ticker_fallback()

    async def _get_ticker_fallback(self) -> Dict:
        """Fallback to CryptoCompare (US-accessible, no API key needed)."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    "https://min-api.cryptocompare.com/data/price",
                    params={"fsym": "BTC", "tsyms": "USD"}
                )
                d = resp.json()
                price = float(d.get("USD", 0))
                return {"price": price, "mark_price": price, "index_price": price, "funding_rate": 0.0}
        except Exception as e:
            logger.warning(f"Ticker fallback also failed: {e}")
            return {"price": 0.0, "mark_price": 0.0, "index_price": 0.0, "funding_rate": 0.0}

    async def get_funding_rate(self) -> float:
        """Get current BTCUSDT funding rate from Bitunix (public endpoint)."""
        try:
            data = await self._get("/api/v1/futures/market/funding_rate", {"symbol": self.symbol})
            inner = data.get("data") or {}
            rate = inner.get("fundingRate")
            if rate is not None:
                return float(rate)
        except Exception as e:
            logger.debug(f"Bitunix funding rate fetch failed: {e}")
        return 0.0

    async def get_copy_trading_aum(self) -> float:
        """
        Get copy trading portfolio AUM (total assets under management).
        Uses the lead trader info endpoint which returns totalAssets.
        Falls back to 0.0 if not available.
        """
        if not self.api_key:
            return 0.0
        path = "/api/v1/futures/copy_trading/lead_trader/info"
        try:
            data = await self._get_signed(path, {})
            if isinstance(data, dict) and data.get("code") == 0:
                info = data.get("data") or {}
                # totalAssets = own equity + follower AUM pooled
                aum = float(info.get("totalAssets", 0) or info.get("aum", 0) or 0)
                logger.info(f"Copy trading AUM: ${aum:,.2f}")
                return aum
        except Exception as e:
            logger.warning(f"Could not fetch copy trading AUM: {e}")
        return 0.0

    async def get_account_balance(self) -> Dict:
        """
        Get effective trading balance.
        Prefers copy trading AUM (lead account total assets) so position sizing
        reflects the full portfolio, not just the personal futures sub-account.
        Falls back to standard futures account balance.
        """
        if not self.api_key:
            return {"balance": 0.0, "available": 0.0, "unrealized_pnl": 0.0}

        # Try copy trading AUM first
        aum = await self.get_copy_trading_aum()

        path = "/api/v1/futures/account"
        try:
            data = await self._get_signed(path, {"marginCoin": "USDT"})
            if isinstance(data, dict) and data.get("code") == 0:
                accounts = data.get("data", [])
                if isinstance(accounts, list) and accounts:
                    acct = accounts[0]
                else:
                    acct = accounts if isinstance(accounts, dict) else {}
                available = float(acct.get("available", 0))
                margin = float(acct.get("margin", 0))
                pnl = float(acct.get("crossUnrealizedPNL", 0))
                personal_balance = available + margin
                # Use AUM if it's larger (includes follower funds), else personal balance
                effective_balance = aum if aum > personal_balance else personal_balance
                if aum > 0:
                    logger.info(f"Using copy trading AUM ${aum:,.2f} (personal: ${personal_balance:,.2f})")
                return {
                    "balance": effective_balance,
                    "available": available,
                    "unrealized_pnl": pnl,
                }
            logger.warning(f"Balance API returned: {data}")
            return {"balance": 0.0, "available": 0.0, "unrealized_pnl": 0.0}
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            return {"balance": 0.0, "available": 0.0, "unrealized_pnl": 0.0}

    async def get_open_positions(self) -> List[Dict]:
        """Get all open positions."""
        if not self.api_key:
            return []
        path = "/api/v1/futures/position/get_pending_positions"
        try:
            data = await self._get_signed(path, {"symbol": self.symbol})
            if isinstance(data, dict) and "data" in data:
                return data["data"] or []
            return []
        except Exception as e:
            logger.error(f"Failed to get positions: {e}")
            return []

    async def get_history_positions(self, limit: int = 5) -> List[Dict]:
        """Fetch recently closed positions with actual PnL, fees, and prices from exchange."""
        if not self.api_key:
            return []
        path = "/api/v1/futures/position/get_history_positions"
        try:
            data = await self._get_signed(path, {"symbol": self.symbol, "limit": limit})
            if isinstance(data, dict) and data.get("code") == 0:
                inner = data.get("data") or {}
                if isinstance(inner, list):
                    return inner
                return inner.get("resultList") or inner.get("list") or []
            logger.warning(f"get_history_positions returned: {data}")
            return []
        except Exception as e:
            logger.error(f"Failed to get position history: {e}")
            return []

    async def place_order(
        self,
        side: str,
        quantity: float,
        leverage: int = 86,
        order_type: str = "MARKET",
        price: float = None,
    ) -> Dict:
        """
        Place a futures order.
        side: 'BUY' (long) or 'SELL' (short)
        """
        if not self.api_key:
            logger.warning("No API key — order not placed (paper trading mode)")
            return {"orderId": f"paper_{int(time.time())}", "status": "FILLED", "paper": True}

        path = "/api/v1/futures/trade/place_order"
        body = {
            "symbol": self.symbol,
            "side": side,
            "tradeSide": "OPEN",
            "orderType": order_type,
            "qty": str(quantity),
        }
        if order_type == "LIMIT" and price:
            body["price"] = str(price)

        try:
            result = await self._post_signed(path, body)
            logger.info(f"Order placed: {side} {quantity} BTC @ {order_type} — {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to place order: {e}")
            raise

    async def close_position(self, side: str, quantity: float, position_id: str = None) -> Dict:
        """Close a position by placing opposite reduce-only order.
        side: 'LONG'/'BUY' for long positions, 'SHORT'/'SELL' for shorts.
        position_id: required for HEDGE mode accounts."""
        # Normalize — exchange returns BUY/SELL, we accept LONG/SHORT too
        close_side = "SELL" if side in ("LONG", "BUY") else "BUY"

        if not self.api_key:
            return {"orderId": f"paper_close_{int(time.time())}", "paper": True}

        path = "/api/v1/futures/trade/place_order"
        body = {
            "symbol": self.symbol,
            "side": close_side,
            "tradeSide": "CLOSE",
            "orderType": "MARKET",
            "qty": str(quantity),
        }
        if position_id:
            body["positionId"] = position_id

        try:
            result = await self._post_signed(path, body)
            logger.info(f"Position closed: {side} {quantity} BTC → {result}")
            return result
        except Exception as e:
            logger.error(f"Failed to close position: {e}")
            raise

    async def set_leverage(self, leverage: int) -> Dict:
        if not self.api_key:
            return {}
        path = "/api/v1/futures/account/change_leverage"
        body = {
            "symbol": self.symbol,
            "marginCoin": "USDT",
            "leverage": leverage,
        }
        try:
            result = await self._post_signed(path, body)
            if result.get("code") == 0:
                logger.info(f"Leverage set to {leverage}x")
            else:
                logger.warning(f"Set leverage returned: {result}")
            return result
        except Exception as e:
            logger.warning(f"Set leverage failed (non-critical): {e}")
            return {}

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()


# Global client instance
_client_instance: Optional[BitunixClient] = None


def get_bitunix_client(api_key: str = None, api_secret: str = None) -> BitunixClient:
    global _client_instance
    if api_key or api_secret:
        return BitunixClient(api_key, api_secret)
    if _client_instance is None:
        _client_instance = BitunixClient()
    return _client_instance
