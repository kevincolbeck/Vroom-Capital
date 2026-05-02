"""
Real-time taker trade flow monitor — Bybit BTCUSDT perpetual.

Subscribes to publicTrade.BTCUSDT and tracks taker buy vs sell volume
over rolling windows. Unlike the order book (which shows posted limit orders
that can be spoofed), aggTrade data reflects actual executed trades — someone
paid the spread to get in or out. That's real conviction.

  taker_buy_ratio_5m > 0.60: buyers paying up → bullish short-term pressure
  taker_buy_ratio_5m < 0.40: sellers paying down → bearish short-term pressure
  0.40–0.60: balanced / unclear

The 5m window aligns with our entry timeframe (scalp entries, 15–90min holds).
The 15m window provides trend context — is the pressure sustained or a spike?
"""
import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass
from typing import Dict, Optional
from loguru import logger


@dataclass(slots=True)
class _TradeEvent:
    ts:     float   # unix seconds
    side:   str     # "BUY" or "SELL"
    volume: float   # BTC size


class TradeFlowMonitor:
    """
    Rolling taker buy/sell volume tracker from Bybit's publicTrade WebSocket.
    get_live_state() is synchronous — safe to call from any coroutine.
    """

    def __init__(self):
        self._events: deque[_TradeEvent] = deque(maxlen=50_000)
        self._running:   bool = False
        self._connected: bool = False
        self._task: Optional[asyncio.Task] = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def start(self):
        if self._running:
            return
        self._running = True
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._run_loop(), name="trade_flow")
        logger.info("TradeFlowMonitor started (Bybit publicTrade.BTCUSDT)")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("TradeFlowMonitor stopped")

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_live_state(self) -> Dict:
        """Rolling taker buy/sell ratios. Never awaits."""
        now = time.time()
        events_5m  = [e for e in self._events if now - e.ts <= 300.0]
        events_15m = [e for e in self._events if now - e.ts <= 900.0]

        buy_5m   = sum(e.volume for e in events_5m  if e.side == "BUY")
        sell_5m  = sum(e.volume for e in events_5m  if e.side == "SELL")
        buy_15m  = sum(e.volume for e in events_15m if e.side == "BUY")
        sell_15m = sum(e.volume for e in events_15m if e.side == "SELL")

        total_5m  = buy_5m  + sell_5m
        total_15m = buy_15m + sell_15m

        tbr_5m  = buy_5m  / total_5m  if total_5m  > 0 else 0.5
        tbr_15m = buy_15m / total_15m if total_15m > 0 else 0.5

        return {
            "taker_buy_ratio_5m":  round(tbr_5m,  4),
            "taker_buy_ratio_15m": round(tbr_15m, 4),
            "buy_volume_5m":       round(buy_5m,  2),
            "sell_volume_5m":      round(sell_5m, 2),
            "total_volume_5m":     round(total_5m, 2),
            "connected":           self._connected,
        }

    # ── Internal ───────────────────────────────────────────────────────────────

    def _add_event(self, side: str, volume: float, ts_ms: int):
        self._events.appendleft(_TradeEvent(
            ts=ts_ms / 1000.0,
            side=side,
            volume=volume,
        ))

    async def _run_loop(self):
        delay = 3.0
        while self._running:
            try:
                await self._connect()
                delay = 3.0
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected = False
                logger.warning(f"TradeFlow stream error: {e} — retry in {delay:.0f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)

    async def _connect(self):
        import websockets
        url = "wss://stream.bybit.com/v5/public/linear"
        async with websockets.connect(url, ping_interval=20, ping_timeout=15) as ws:
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": ["publicTrade.BTCUSDT"],
            }))
            self._connected = True
            logger.info("TradeFlow stream connected (Bybit publicTrade.BTCUSDT)")
            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                    if msg.get("topic") == "publicTrade.BTCUSDT":
                        for t in msg.get("data", []):
                            side   = "BUY" if t.get("S") == "Buy" else "SELL"
                            volume = float(t.get("v", 0) or 0)
                            ts_ms  = int(t.get("T", 0) or 0)
                            if volume > 0 and ts_ms > 0:
                                self._add_event(side, volume, ts_ms)
                except Exception as e:
                    logger.debug(f"TradeFlow parse error: {e}")
        self._connected = False
