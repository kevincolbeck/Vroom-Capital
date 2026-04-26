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

    def _sign(self, params: str) -> str:
        return hmac.new(
            self.api_secret.encode("utf-8"),
            params.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()

    def _build_signed_headers(self, body: str = "") -> Dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        nonce = str(int(time.time() * 1000))
        sign_str = self.api_key + timestamp + nonce + body
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            sign_str.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        return {
            "api-key": self.api_key,
            "timestamp": timestamp,
            "nonce": nonce,
            "sign": signature,
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

    async def _post_signed(self, path: str, body: Dict) -> Dict:
        import json
        client = await self._get_client()
        body_str = json.dumps(body, separators=(",", ":"))
        headers = self._build_signed_headers(body_str)
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
                    candles.append({
                        "open_time": int(c.get("openTime", c.get("ts", 0))),
                        "open": float(c.get("open", 0)),
                        "high": float(c.get("high", 0)),
                        "low": float(c.get("low", 0)),
                        "close": float(c.get("close", 0)),
                        "volume": float(c.get("volume", 0)),
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
        """Get current mark price and index price."""
        path = "/api/v1/futures/market/ticker"
        try:
            data = await self._get(path, {"symbol": self.symbol})
            if isinstance(data, dict) and "data" in data:
                ticker = data["data"]
            else:
                ticker = data
            return {
                "price": float(ticker.get("lastPrice", ticker.get("price", ticker.get("markPrice", 0)))),
                "mark_price": float(ticker.get("markPrice", ticker.get("lastPrice", 0))),
                "index_price": float(ticker.get("indexPrice", ticker.get("lastPrice", 0))),
                "funding_rate": float(ticker.get("fundingRate", 0)),
            }
        except Exception as e:
            logger.warning(f"Failed to get ticker from Bitunix: {e}")
            return await self._get_ticker_fallback()

    async def _get_ticker_fallback(self) -> Dict:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                "https://fapi.binance.com/fapi/v1/premiumIndex",
                params={"symbol": "BTCUSDT"}
            )
            d = resp.json()
            return {
                "price": float(d.get("markPrice", 0)),
                "mark_price": float(d.get("markPrice", 0)),
                "index_price": float(d.get("indexPrice", 0)),
                "funding_rate": float(d.get("lastFundingRate", 0)),
            }

    async def get_funding_rate(self) -> float:
        """Get current funding rate."""
        try:
            ticker = await self.get_ticker()
            return ticker.get("funding_rate", 0.0)
        except Exception:
            return 0.0

    async def get_account_balance(self) -> Dict:
        """Get account USDT balance."""
        if not self.api_key:
            return {"balance": 0.0, "available": 0.0, "unrealized_pnl": 0.0}
        path = "/api/v1/futures/account/assets"
        try:
            data = await self._post_signed(path, {})
            if isinstance(data, dict) and "data" in data:
                assets = data["data"]
                if isinstance(assets, list):
                    for asset in assets:
                        if asset.get("currency", asset.get("asset", "")) in ("USDT", "USD"):
                            return {
                                "balance": float(asset.get("balance", asset.get("walletBalance", 0))),
                                "available": float(asset.get("available", asset.get("availableBalance", 0))),
                                "unrealized_pnl": float(asset.get("unrealizedPnl", 0)),
                            }
            return {"balance": 1000.0, "available": 1000.0, "unrealized_pnl": 0.0}
        except Exception as e:
            logger.error(f"Failed to get balance: {e}")
            return {"balance": 0.0, "available": 0.0, "unrealized_pnl": 0.0}

    async def get_open_positions(self) -> List[Dict]:
        """Get all open positions."""
        if not self.api_key:
            return []
        path = "/api/v1/futures/position/get_pending_positions"
        try:
            data = await self._post_signed(path, {"symbol": self.symbol})
            if isinstance(data, dict) and "data" in data:
                return data["data"] or []
            return []
        except Exception as e:
            logger.error(f"Failed to get positions: {e}")
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
            "orderType": order_type,
            "qty": str(quantity),
            "leverage": str(leverage),
            "marginMode": "CROSSED",
            "reduceOnly": False,
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

    async def close_position(self, side: str, quantity: float) -> Dict:
        """Close a position by placing opposite reduce-only order."""
        close_side = "SELL" if side == "LONG" else "BUY"
        if not self.api_key:
            return {"orderId": f"paper_close_{int(time.time())}", "paper": True}

        path = "/api/v1/futures/trade/place_order"
        body = {
            "symbol": self.symbol,
            "side": close_side,
            "orderType": "MARKET",
            "qty": str(quantity),
            "marginMode": "CROSSED",
            "reduceOnly": True,
        }
        try:
            result = await self._post_signed(path, body)
            logger.info(f"Position closed: {side} {quantity} BTC")
            return result
        except Exception as e:
            logger.error(f"Failed to close position: {e}")
            raise

    async def set_leverage(self, leverage: int) -> Dict:
        if not self.api_key:
            return {}
        path = "/api/v1/futures/account/set_leverage"
        body = {
            "symbol": self.symbol,
            "leverage": str(leverage),
            "marginMode": "CROSSED",
        }
        try:
            return await self._post_signed(path, body)
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
