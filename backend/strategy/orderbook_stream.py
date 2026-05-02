"""
Real-time L2 order book monitor — Binance USDT-M futures.

Uses the WebSocket diff depth stream + REST snapshot for a fully synchronized
live book. Detects bid/ask walls and instantaneous book imbalance.

Wall detection uses a local adaptive threshold: a level is a wall when its
size is ≥ WALL_MULTIPLIER × the mean of the ±5 surrounding levels AND meets
a minimum absolute size. This avoids false walls in thin regions of the book.

get_live_state(current_price, liq_target_price) is safe to call from any
coroutine — it never awaits.
"""
import asyncio
import json
from typing import Dict, Optional, Tuple
from loguru import logger

WALL_MULTIPLIER   = 3.0    # size must be ≥ N× local mean to count as a wall
MIN_WALL_BTC      = 30.0   # ignore levels smaller than this regardless
LARGE_WALL_BTC    = 150.0  # threshold for "large" wall scoring
IMBALANCE_LEVELS  = 15     # top N levels used for book imbalance ratio
WALL_SEARCH_PCT   = 2.0    # only scan within ±2% of current price

_SNAPSHOT_URL = "https://fstream.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=500"
_WS_URL       = "wss://fstream.binance.com/ws/btcusdt@depth@100ms"


class OrderBookMonitor:
    """
    Maintains a live Binance futures L2 order book via WebSocket diff stream.
    Thread-safe reads via get_live_state() — synchronous, no awaiting.
    """

    def __init__(self):
        self._bids: Dict[float, float] = {}   # price → BTC size
        self._asks: Dict[float, float] = {}
        self._last_update_id: int = 0
        self._synced: bool = False
        self._running: bool = False
        self._connected: bool = False
        self._task: Optional[asyncio.Task] = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def start(self):
        if self._running:
            return
        self._running = True
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._run_loop(), name="ob_binance")
        logger.info("OrderBookMonitor started (Binance futures)")

    def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info("OrderBookMonitor stopped")

    # ── Public API ─────────────────────────────────────────────────────────────

    def get_live_state(
        self,
        current_price: float,
        liq_target_price: Optional[float] = None,
    ) -> Dict:
        """
        Returns wall + imbalance metrics relative to current_price.
        liq_target_price: if provided, scans for blocking walls in the path.
        """
        if not self._synced or not self._bids or not self._asks:
            return self._empty_state()

        lo = current_price * (1.0 - WALL_SEARCH_PCT / 100.0)
        hi = current_price * (1.0 + WALL_SEARCH_PCT / 100.0)

        # Sorted nearest-first: highest bids descending, lowest asks ascending
        bids_near = sorted(
            [(p, s) for p, s in self._bids.items() if p >= lo],
            reverse=True,
        )
        asks_near = sorted(
            [(p, s) for p, s in self._asks.items() if p <= hi],
        )

        bid_wall_price, bid_wall_size = self._find_wall(bids_near)
        ask_wall_price, ask_wall_size = self._find_wall(asks_near)

        bid_wall_pct = (
            abs(current_price - bid_wall_price) / current_price * 100.0
            if bid_wall_price else 0.0
        )
        ask_wall_pct = (
            abs(ask_wall_price - current_price) / current_price * 100.0
            if ask_wall_price else 0.0
        )

        book_imbalance = self._compute_imbalance(bids_near, asks_near)

        blocking_wall_price = 0.0
        blocking_wall_size  = 0.0
        blocking_wall_pct   = 0.0
        if liq_target_price and liq_target_price > 0:
            blocking_wall_price, blocking_wall_size = self._find_blocking_wall(
                current_price, liq_target_price
            )
            if blocking_wall_price:
                blocking_wall_pct = (
                    abs(blocking_wall_price - current_price) / current_price * 100.0
                )

        return {
            "bid_wall_price":         round(bid_wall_price, 1),
            "bid_wall_size_btc":      round(bid_wall_size, 1),
            "bid_wall_pct":           round(bid_wall_pct, 3),
            "ask_wall_price":         round(ask_wall_price, 1),
            "ask_wall_size_btc":      round(ask_wall_size, 1),
            "ask_wall_pct":           round(ask_wall_pct, 3),
            "book_imbalance":         round(book_imbalance, 4),
            "blocking_wall_price":    round(blocking_wall_price, 1),
            "blocking_wall_size_btc": round(blocking_wall_size, 1),
            "blocking_wall_pct":      round(blocking_wall_pct, 3),
            "connected":              self._connected,
            "synced":                 self._synced,
        }

    # ── Wall + imbalance math ──────────────────────────────────────────────────

    def _find_wall(self, levels: list) -> Tuple[float, float]:
        """
        Nearest level whose size ≥ WALL_MULTIPLIER × local ±5-level mean.
        Nearest-first ordering means the first match is closest to price.
        """
        if len(levels) < 5:
            return 0.0, 0.0
        n = len(levels)
        for i, (price, size) in enumerate(levels):
            lo = max(0, i - 5)
            hi = min(n, i + 6)
            local_mean = sum(s for _, s in levels[lo:hi]) / (hi - lo)
            threshold = max(MIN_WALL_BTC, local_mean * WALL_MULTIPLIER)
            if size >= threshold:
                return price, size
        return 0.0, 0.0

    def _compute_imbalance(self, bids_near: list, asks_near: list) -> float:
        """
        (bid_depth - ask_depth) / total at top IMBALANCE_LEVELS levels.
        +1.0 = all bids (strong buy pressure). -1.0 = all asks (sell pressure).
        """
        n = IMBALANCE_LEVELS
        bid_depth = sum(s for _, s in bids_near[:n])
        ask_depth = sum(s for _, s in asks_near[:n])
        total = bid_depth + ask_depth
        return (bid_depth - ask_depth) / total if total > 0 else 0.0

    def _find_blocking_wall(
        self, current_price: float, liq_target: float
    ) -> Tuple[float, float]:
        """
        Largest wall (by BTC size) that sits between current_price and liq_target.
        Returns (price, size) or (0.0, 0.0) if path is clear.
        """
        if liq_target > current_price:
            # Need to move UP — ask walls in the path are blockers
            candidates = [
                (p, s) for p, s in self._asks.items()
                if current_price < p < liq_target and s >= MIN_WALL_BTC
            ]
        else:
            # Need to move DOWN — bid walls in the path are blockers
            candidates = [
                (p, s) for p, s in self._bids.items()
                if liq_target < p < current_price and s >= MIN_WALL_BTC
            ]
        return max(candidates, key=lambda x: x[1]) if candidates else (0.0, 0.0)

    # ── Book management ────────────────────────────────────────────────────────

    def _apply_delta(self, bids: list, asks: list):
        """Apply a diff update: qty=0 means remove that price level."""
        for price_str, qty_str in bids:
            p = float(price_str)
            q = float(qty_str)
            if q == 0:
                self._bids.pop(p, None)
            else:
                self._bids[p] = q
        for price_str, qty_str in asks:
            p = float(price_str)
            q = float(qty_str)
            if q == 0:
                self._asks.pop(p, None)
            else:
                self._asks[p] = q

    def _empty_state(self) -> Dict:
        return {
            "bid_wall_price": 0.0, "bid_wall_size_btc": 0.0, "bid_wall_pct": 0.0,
            "ask_wall_price": 0.0, "ask_wall_size_btc": 0.0, "ask_wall_pct": 0.0,
            "book_imbalance": 0.0,
            "blocking_wall_price": 0.0, "blocking_wall_size_btc": 0.0, "blocking_wall_pct": 0.0,
            "connected": self._connected, "synced": False,
        }

    # ── WebSocket + snapshot loop ──────────────────────────────────────────────

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
                self._synced = False
                logger.warning(f"OrderBook stream error: {e} — retry in {delay:.0f}s")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)

    async def _connect(self):
        import websockets
        async with websockets.connect(_WS_URL, ping_interval=20, ping_timeout=15) as ws:
            self._connected = True
            self._synced = False
            buffer: list = []
            logger.info("OrderBook WebSocket connected — fetching snapshot")

            # Kick off REST snapshot while simultaneously buffering WS events
            snap_task = asyncio.create_task(self._fetch_snapshot())

            async for raw in ws:
                if not self._running:
                    break
                msg = json.loads(raw)

                if not self._synced:
                    buffer.append(msg)

                    # Once snapshot arrives, synchronize and switch to live mode
                    if snap_task.done():
                        snap = snap_task.result()
                        if snap is None:
                            # Snapshot failed — retry entire connection
                            logger.warning("Snapshot fetch failed, reconnecting")
                            return

                        self._bids = {float(p): float(q) for p, q in snap["bids"]}
                        self._asks = {float(p): float(q) for p, q in snap["asks"]}
                        last_id = snap["lastUpdateId"]

                        # Apply buffered deltas in order, skipping stale ones
                        for evt in buffer:
                            if evt.get("u", 0) <= last_id:
                                continue
                            self._apply_delta(evt.get("b", []), evt.get("a", []))
                            last_id = evt["u"]

                        buffer.clear()
                        self._synced = True
                        logger.info(
                            f"OrderBook synced — {len(self._bids)} bid levels, "
                            f"{len(self._asks)} ask levels"
                        )
                else:
                    self._apply_delta(msg.get("b", []), msg.get("a", []))

        snap_task.cancel()
        self._connected = False

    async def _fetch_snapshot(self) -> Optional[Dict]:
        try:
            import httpx
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(_SNAPSHOT_URL)
                resp.raise_for_status()
                return resp.json()
        except Exception as e:
            logger.error(f"OrderBook snapshot fetch failed: {e}")
            return None
