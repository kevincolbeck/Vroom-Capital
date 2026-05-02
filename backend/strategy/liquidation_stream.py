"""
Real-time liquidation event stream monitor.

Connects to Bybit and Binance WebSocket feeds for live liquidation events.
Tracks cascade state and Hawkes self-exciting point process intensity.

Bybit  topic: liquidation.BTCUSDT
  side="Sell" → LONG position liquidated (bearish)
  side="Buy"  → SHORT position liquidated (bullish squeeze)

Binance topic: btcusdt@forceOrder
  S="SELL" → LONG position liquidated (bearish)
  S="BUY"  → SHORT position liquidated (bullish squeeze)
"""
import asyncio
import json
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Dict, Optional
from loguru import logger


# ── Hawkes process parameters ────────────────────────────────────────────────
# λ(t) = μ + Σ α × btc_size × e^(-β(t - tᵢ))
#
# μ    baseline intensity when market is quiet (~1 small event / 20s)
# α    excitation per BTC liquidated — a 10 BTC event adds α*10 to intensity
# β    decay rate per second — half-life ≈ ln(2)/β ≈ 14 seconds
MU   = 0.05
ALPHA = 0.5
BETA  = 0.05

# Cascade detection thresholds
CASCADE_MIN_BTC_PER_MIN  = 5.0    # minimum BTC/min to be considered an active cascade
CASCADE_ACCELERATION_X   = 2.0    # current rate must be ≥ 2× the 5-min baseline
WINDOW_SECONDS           = 60     # rolling window for current rate
BASELINE_WINDOW_SECONDS  = 300    # 5-min window for baseline rate
DIRECTION_DOMINANCE_X    = 1.5    # one side must be 1.5× the other to declare direction


@dataclass(slots=True)
class _LiqEvent:
    ts:        float   # unix timestamp
    direction: str     # "LONG_LIQ" or "SHORT_LIQ"
    btc_size:  float
    price:     float


class LiquidationStreamMonitor:
    """
    Maintains persistent WebSocket connections to Bybit + Binance liquidation feeds.
    Safe to call get_live_state() from any coroutine — it never awaits.
    """

    def __init__(self):
        # newest events at index 0 (appendleft)
        self._events: deque[_LiqEvent] = deque(maxlen=10_000)
        self._running: bool = False
        self._bybit_task:   Optional[asyncio.Task] = None
        self._binance_task: Optional[asyncio.Task] = None
        self._connected_bybit:   bool = False
        self._connected_binance: bool = False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self):
        """Launch background WebSocket listeners. Call once at bot startup."""
        if self._running:
            return
        self._running = True
        loop = asyncio.get_event_loop()
        self._bybit_task   = loop.create_task(self._bybit_loop(),   name="liq_bybit")
        self._binance_task = loop.create_task(self._binance_loop(), name="liq_binance")
        logger.info("LiquidationStreamMonitor started (Bybit + Binance)")

    def stop(self):
        """Cancel background tasks."""
        self._running = False
        for t in (self._bybit_task, self._binance_task):
            if t:
                t.cancel()
        logger.info("LiquidationStreamMonitor stopped")

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_live_state(self) -> Dict:
        """
        Snapshot of current cascade state. Called by SignalEngine every tick.
        Never awaits — safe to call from sync or async context.
        """
        now = time.time()

        # Partition events into time windows
        events_1m  = [e for e in self._events if now - e.ts <= WINDOW_SECONDS]
        events_5m  = [e for e in self._events if now - e.ts <= BASELINE_WINDOW_SECONDS]
        events_15s = [e for e in self._events if now - e.ts <= 15.0]

        # BTC volume by direction
        long_liq_1m  = sum(e.btc_size for e in events_1m if e.direction == "LONG_LIQ")
        short_liq_1m = sum(e.btc_size for e in events_1m if e.direction == "SHORT_LIQ")
        total_1m     = long_liq_1m + short_liq_1m

        # 5-min baseline rate (BTC/min)
        total_5m      = sum(e.btc_size for e in events_5m)
        baseline_rate = (total_5m / (BASELINE_WINDOW_SECONDS / 60.0)) if total_5m > 0 else 0.1

        # Directional dominance
        cascade_direction: Optional[str] = None
        if total_1m > 0:
            if long_liq_1m >= short_liq_1m * DIRECTION_DOMINANCE_X:
                cascade_direction = "SHORT"   # longs wiped  → bearish cascade
            elif short_liq_1m >= long_liq_1m * DIRECTION_DOMINANCE_X:
                cascade_direction = "LONG"    # shorts squeezed → bullish cascade

        # Is a cascade live?
        above_min      = total_1m >= CASCADE_MIN_BTC_PER_MIN
        above_baseline = (total_1m / baseline_rate) >= CASCADE_ACCELERATION_X if baseline_rate > 0 else False
        cascade_live   = above_min and above_baseline

        # Hawkes intensity — how self-excited is the current liquidation rate?
        hawkes = self._hawkes_intensity(now)

        # Acceleration — is the last-15s pace higher than the 1-min average?
        rate_15s_annualized = sum(e.btc_size for e in events_15s) / (15.0 / 60.0)
        accelerating = cascade_live and (rate_15s_annualized > total_1m * CASCADE_ACCELERATION_X)

        return {
            "cascade_live":      cascade_live,
            "cascade_direction": cascade_direction,
            "liq_rate_btc_min":  round(total_1m,     2),
            "long_liq_btc_min":  round(long_liq_1m,  2),
            "short_liq_btc_min": round(short_liq_1m, 2),
            "baseline_btc_min":  round(baseline_rate, 2),
            "hawkes_intensity":  round(hawkes,        4),
            "accelerating":      accelerating,
            "event_count_1m":    len(events_1m),
            "connected":         self._connected_bybit or self._connected_binance,
        }

    # ── Hawkes process ────────────────────────────────────────────────────────

    def _hawkes_intensity(self, now: float) -> float:
        """λ(t) = μ + Σ α × btc_size × e^{-β(t − tᵢ)} for recent events."""
        intensity = MU
        for e in self._events:          # newest → oldest
            dt = now - e.ts
            if dt > BASELINE_WINDOW_SECONDS:
                break                   # negligible contribution, stop early
            intensity += ALPHA * e.btc_size * math.exp(-BETA * dt)
        return intensity

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _add_event(self, direction: str, btc_size: float, price: float):
        self._events.appendleft(_LiqEvent(
            ts=time.time(),
            direction=direction,
            btc_size=btc_size,
            price=price,
        ))

    # ── Bybit WebSocket ───────────────────────────────────────────────────────

    async def _bybit_loop(self):
        delay = 3.0
        while self._running:
            try:
                await self._bybit_connect()
                delay = 3.0   # reset backoff on clean exit
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected_bybit = False
                logger.warning(f"Bybit liq stream error: {e} — retry in {delay:.0f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)

    async def _bybit_connect(self):
        import websockets
        url = "wss://stream.bybit.com/v5/public/linear"
        async with websockets.connect(url, ping_interval=20, ping_timeout=15) as ws:
            await ws.send(json.dumps({
                "op": "subscribe",
                "args": ["liquidation.BTCUSDT"],
            }))
            self._connected_bybit = True
            logger.info("Bybit liquidation stream connected")
            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                    if msg.get("topic") == "liquidation.BTCUSDT":
                        d     = msg.get("data", {})
                        side  = d.get("side", "")
                        size  = float(d.get("size",  0) or 0)
                        price = float(d.get("price", 0) or 0)
                        if size > 0 and price > 0:
                            # Bybit: Sell = long liquidated; Buy = short liquidated
                            direction = "LONG_LIQ" if side == "Sell" else "SHORT_LIQ"
                            self._add_event(direction, size, price)
                except Exception as e:
                    logger.debug(f"Bybit liq parse: {e}")
        self._connected_bybit = False

    # ── Binance WebSocket ─────────────────────────────────────────────────────

    async def _binance_loop(self):
        delay = 3.0
        while self._running:
            try:
                await self._binance_connect()
                delay = 3.0
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected_binance = False
                logger.warning(f"Binance liq stream error: {e} — retry in {delay:.0f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)

    async def _binance_connect(self):
        import websockets
        url = "wss://fstream.binance.com/ws/btcusdt@forceOrder"
        async with websockets.connect(url, ping_interval=20, ping_timeout=15) as ws:
            self._connected_binance = True
            logger.info("Binance liquidation stream connected")
            async for raw in ws:
                if not self._running:
                    break
                try:
                    msg = json.loads(raw)
                    if msg.get("e") == "forceOrder":
                        o     = msg.get("o", {})
                        side  = o.get("S", "")
                        # prefer cumulative filled qty, fallback to order qty
                        size  = float(o.get("z", 0) or o.get("q", 0) or 0)
                        price = float(o.get("ap", 0) or o.get("p", 0) or 0)
                        if size > 0 and price > 0:
                            # Binance: SELL = long liquidated; BUY = short liquidated
                            direction = "LONG_LIQ" if side == "SELL" else "SHORT_LIQ"
                            self._add_event(direction, size, price)
                except Exception as e:
                    logger.debug(f"Binance liq parse: {e}")
        self._connected_binance = False
