"""
Bot Engine — Main autonomous trading loop.
Runs continuously, checking for signals and managing positions.
"""
import asyncio
import time as _time_module
from datetime import datetime
from typing import Optional, List, Dict
from loguru import logger

from backend.database import AsyncSessionLocal, BotState, BotStatus, Position, PositionStatus, BotLog, ZoneMemory
from backend.exchange.bitunix import get_bitunix_client
from backend.strategy.signal_engine import SignalEngine, TradeSignal
from backend.strategy.heikin_ashi import compute_heikin_ashi
from backend.trading.position_manager import PositionManager
from backend.copy_trading.manager import CopyTradingManager
from backend.config import settings
from sqlalchemy import select, update, delete


class BotEngine:

    LOOP_INTERVAL_SECONDS = 60      # Full signal scan when flat
    CANDLE_CHECK_SECONDS  = 15      # HA-based exit check (needs klines)
    PRICE_POLL_SECONDS    = 1       # Trailing stop check (ticker only)
    PRIME_SCAN_INTERVAL   = 15      # Seconds between 3M scans while primed
    PRIME_TIMEOUT_SECONDS = 1800    # 30 min — abandon primed entry if no 3M confirm

    def __init__(self):
        self.signal_engine = SignalEngine()
        self._running = False
        self._paused = False
        self._task: Optional[asyncio.Task] = None
        self._last_signal: Optional[Dict] = None
        self._current_position: Optional[Dict] = None
        self._manual_override: bool = False

        # Primed entry state — set when 6H/1H agree; wait for 3M HA to confirm
        self._primed_direction: Optional[str] = None
        self._primed_at: Optional[float] = None
        self._primed_size_modifier: float = 1.0
        self._primed_account_balance: float = 0.0
        self._primed_signal_data: Optional[Dict] = None

    # ─────────────────────────────────────────────────────────────────
    # Lifecycle
    # ─────────────────────────────────────────────────────────────────

    async def start(self):
        """Start the bot engine."""
        if self._running:
            logger.warning("Bot already running")
            return
        self._running = True
        self._paused = False
        await self._load_zone_state()
        self._task = asyncio.create_task(self._main_loop())
        await self._set_status(BotStatus.RUNNING)
        await self._log("INFO", "BOT", "Bot engine started")
        logger.info("Bot engine started")

    async def stop(self):
        """Stop the bot engine gracefully."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._set_status(BotStatus.STOPPED)
        await self._log("INFO", "BOT", "Bot engine stopped")
        logger.info("Bot engine stopped")

    async def pause(self):
        """Pause signal generation (keeps position monitoring active)."""
        self._paused = True
        await self._set_status(BotStatus.PAUSED)
        await self._log("INFO", "BOT", "Bot paused — position monitoring continues")

    async def resume(self):
        """Resume signal generation."""
        self._paused = False
        await self._set_status(BotStatus.RUNNING)
        await self._log("INFO", "BOT", "Bot resumed")

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def is_paused(self) -> bool:
        return self._paused

    # ─────────────────────────────────────────────────────────────────
    # Main Loop
    # ─────────────────────────────────────────────────────────────────

    async def _main_loop(self):
        """Main bot loop.

        When flat  : sniper mode — sleeps to 2s before the next 1H candle
                     boundary (UTC-aligned) then fires immediately at close.
                     Matches backtest timing: evaluate exactly at candle close.
        In position: 1s price-only trailing stop check,
                     15s candle-based HA exit check,
                     60s full signal refresh.
        """
        _time = _time_module
        last_candle_check = 0.0
        last_full_tick    = 0.0
        _first_flat_tick  = True   # evaluate immediately on startup, then boundary-align

        while self._running:
            try:
                now = _time.monotonic()
                has_open = await self._has_open_position()

                if has_open:
                    _first_flat_tick = False  # came from in-position; re-align on next flat
                    # Cancel any pending primed entry — position opened externally/manually
                    if self._primed_direction:
                        self._primed_direction = None

                    # 1s: trailing stop only (ticker, no klines)
                    await self._trailing_stop_tick()

                    # 15s: HA-based exits (needs klines)
                    if now - last_candle_check >= self.CANDLE_CHECK_SECONDS:
                        await self._position_tick()
                        last_candle_check = _time.monotonic()

                    # 60s: full signal tick even while in position
                    if now - last_full_tick >= self.LOOP_INTERVAL_SECONDS:
                        await self._tick()
                        last_full_tick = _time.monotonic()

                    await asyncio.sleep(self.PRICE_POLL_SECONDS)
                else:
                    if self._primed_direction:
                        # ── PRIMED: scan 3M every 15s for entry confirmation ──
                        elapsed = _time.time() - (self._primed_at or 0)
                        if elapsed >= self.PRIME_TIMEOUT_SECONDS:
                            logger.info(f"Primed {self._primed_direction} timed out after {elapsed:.0f}s — cancelled")
                            await self._log("INFO", "SIGNAL",
                                f"Primed {self._primed_direction} entry expired (30min timeout)")
                            self._primed_direction = None
                        else:
                            entered = await self._3m_entry_scan()
                            if not entered:
                                await asyncio.sleep(self.PRIME_SCAN_INTERVAL)
                    else:
                        # ── SNIPER: sleep to 2s before next 1H candle close ──
                        if _first_flat_tick:
                            # On startup (or recovery) evaluate once immediately so we
                            # don't sit idle until the next boundary.
                            _first_flat_tick = False
                        else:
                            # Cap at 5 min so the dashboard signal never goes fully stale.
                            secs_to_next = 3600 - (_time.time() % 3600)
                            sleep_for = max(secs_to_next - 2, 0)

                            if sleep_for > 300:
                                # Not near a boundary — sleep 5 min, refresh dashboard,
                                # then loop back to recalculate the remaining wait.
                                await asyncio.sleep(300)
                                await self._tick()        # dashboard refresh only
                                last_full_tick    = _time.monotonic()
                                last_candle_check = _time.monotonic()
                                continue                  # recompute secs_to_next next iteration

                            if sleep_for > 1:
                                logger.info(
                                    f"Sniper: {sleep_for:.0f}s to next 1H close "
                                    f"({secs_to_next:.0f}s remaining in candle)"
                                )
                                await asyncio.sleep(sleep_for)

                        # ── At (or within 2s of) the candle boundary ──────────
                        await self._tick()
                        last_full_tick    = _time.monotonic()
                        last_candle_check = _time.monotonic()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Bot loop error: {e}")
                await self._set_status(BotStatus.ERROR)
                await self._log("ERROR", "BOT", f"Bot loop error: {e}", details=str(e))
                await asyncio.sleep(30)
                await self._set_status(BotStatus.RUNNING)
                _first_flat_tick = True  # re-evaluate immediately after recovery
                continue

    async def _trailing_stop_tick(self):
        """1-second tick — ticker only, checks trailing stop. No klines fetched."""
        client = get_bitunix_client()
        try:
            ticker = await client.get_ticker()
            current_price = ticker["price"]
        except Exception:
            return

        async with AsyncSessionLocal() as db:
            pos_manager = PositionManager(client, db)
            open_positions = await pos_manager.get_open_positions()
            if not open_positions:
                return

            copy_manager = CopyTradingManager(db)
            for position in open_positions:
                position = await pos_manager.update_position(position, current_price)

                if not self._manual_override:
                    should_exit, exit_reason = self.signal_engine.check_trailing_stop(
                        position_side=position.side,
                        entry_price=position.entry_price,
                        current_price=current_price,
                        peak_profit_pct=position.peak_profit_pct,
                    )
                    if should_exit:
                        await pos_manager.close_position(position, current_price, exit_reason)
                        await copy_manager.close_copy_positions(position, exit_reason)
                        await self._update_bot_stats(db, position)

    async def _has_open_position(self) -> bool:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Position).where(Position.status == PositionStatus.OPEN).limit(1)
            )
            return result.scalar_one_or_none() is not None

    async def _position_tick(self):
        """Lightweight tick — just fetch price and check exits. No candle fetch."""
        client = get_bitunix_client()
        async with AsyncSessionLocal() as db:
            pos_manager = PositionManager(client, db)

            try:
                ticker = await client.get_ticker()
                current_price = ticker["price"]
            except Exception:
                return

            open_positions = await pos_manager.get_open_positions()
            if not open_positions:
                return

            # Need HA candles for exit signal — fetch only if we have positions
            try:
                candles_1h_raw = await client.get_klines("1h", limit=50)
                candles_6h_raw = await client.get_klines("6h", limit=20)
                ha_1h = compute_heikin_ashi(candles_1h_raw[:-1][-50:])
                ha_6h = compute_heikin_ashi(candles_6h_raw[:-1][-20:])
            except Exception:
                return

            # 3M candles for post-TP1 supplemental exit (failure is non-fatal)
            ha_3m: List[Dict] = []
            try:
                candles_3m_raw = await client.get_klines("3m", limit=20)
                ha_3m = compute_heikin_ashi(candles_3m_raw[:-1])
            except Exception:
                pass

            copy_manager = CopyTradingManager(db)
            for position in open_positions:
                position = await pos_manager.update_position(position, current_price)

                if not self._manual_override:
                    should_exit, exit_reason = self.signal_engine.get_exit_signal(
                        position_side=position.side,
                        entry_price=position.entry_price,
                        current_price=current_price,
                        peak_profit_pct=position.peak_profit_pct,
                        candles_1h_ha=ha_1h,
                        candles_6h_ha=ha_6h,
                    )
                    # 3M HA supplemental exit — only fires after TP1
                    if not should_exit and ha_3m:
                        should_exit, exit_reason = self.signal_engine.check_3m_exit(
                            position_side=position.side,
                            peak_profit_pct=position.peak_profit_pct,
                            ha_3m=ha_3m,
                        )
                    if should_exit:
                        await pos_manager.close_position(position, current_price, exit_reason)
                        await copy_manager.close_copy_positions(position, exit_reason)
                        await self._update_bot_stats(db, position)

    async def _tick(self):
        """Single tick of the bot loop."""
        client = get_bitunix_client()
        async with AsyncSessionLocal() as db:
            pos_manager = PositionManager(client, db)
            copy_manager = CopyTradingManager(db)

            # ─── Fetch market data ────────────────────────────────────
            try:
                ticker = await client.get_ticker()
                current_price = ticker["price"]
            except Exception as e:
                await self._log("ERROR", "DATA", f"Failed to fetch ticker: {e}")
                return

            try:
                candles_1h_raw = await client.get_klines("1h", limit=100)
                candles_6h_raw = await client.get_klines("6h", limit=50)
            except Exception as e:
                await self._log("ERROR", "DATA", f"Failed to fetch candles: {e}")
                return

            if len(candles_1h_raw) < 10 or len(candles_6h_raw) < 5:
                await self._log("WARNING", "DATA", "Insufficient candle data")
                return

            # ─── Compute HA candles for exit checks ──────────────────
            ha_1h = compute_heikin_ashi(candles_1h_raw[:-1][-50:])
            ha_6h = compute_heikin_ashi(candles_6h_raw[:-1][-20:])

            # 3M candles for post-TP1 supplemental exit
            ha_3m: List[Dict] = []
            try:
                candles_3m_raw = await client.get_klines("3m", limit=20)
                ha_3m = compute_heikin_ashi(candles_3m_raw[:-1])
            except Exception:
                pass

            # ─── Check and update open positions ──────────────────────
            open_positions = await pos_manager.get_open_positions()

            for position in open_positions:
                # Update P&L
                position = await pos_manager.update_position(position, current_price)

                # Check for exit signals (skip if manual override active)
                if not self._manual_override:
                    should_exit, exit_reason = self.signal_engine.get_exit_signal(
                        position_side=position.side,
                        entry_price=position.entry_price,
                        current_price=current_price,
                        peak_profit_pct=position.peak_profit_pct,
                        candles_1h_ha=ha_1h,
                        candles_6h_ha=ha_6h,
                    )
                    # 3M HA supplemental exit — only fires after TP1
                    if not should_exit and ha_3m:
                        should_exit, exit_reason = self.signal_engine.check_3m_exit(
                            position_side=position.side,
                            peak_profit_pct=position.peak_profit_pct,
                            ha_3m=ha_3m,
                        )

                    if should_exit:
                        await pos_manager.close_position(position, current_price, exit_reason)
                        await copy_manager.close_copy_positions(position, exit_reason)
                        await self._update_bot_stats(db, position)

            # ─── Signal generation (if not paused and no open position) ──
            if not self._paused and not self._manual_override:
                fresh_open = await pos_manager.get_open_positions()
                has_open = len(fresh_open) >= settings.max_concurrent_positions

                if not has_open:
                    active_side = None
                    signal = await self.signal_engine.generate_signal(
                        candles_1h=candles_1h_raw,
                        candles_6h=candles_6h_raw,
                        current_price=current_price,
                        active_position_side=active_side,
                    )

                    self._last_signal = signal.to_dict()

                    if signal.should_trade:
                        # Verify no position exists on exchange before priming
                        try:
                            ex_pos = await client.get_open_positions()
                            if ex_pos:
                                await self._log("WARNING", "RISK",
                                    "Exchange already has open position — skipping entry to avoid stacking")
                                signal.should_trade = False
                        except Exception:
                            pass

                    if signal.should_trade and not self._primed_direction:
                        # Get account balance
                        try:
                            balance_data = await client.get_account_balance()
                            account_balance = balance_data.get("available", 0)
                        except Exception:
                            account_balance = 0

                        if account_balance >= 5:
                            # Prime the entry — wait for 3M HA confirmation
                            self._primed_direction      = signal.direction
                            self._primed_at             = _time_module.time()
                            self._primed_size_modifier  = signal.position_size_modifier
                            self._primed_account_balance = account_balance
                            self._primed_signal_data    = signal.to_dict()
                            await self._log("INFO", "SIGNAL",
                                f"Primed {signal.direction} — waiting for 3M HA confirmation (30min timeout)")
                            logger.info(
                                f"Primed {signal.direction} entry — scanning 3M HA "
                                f"every {self.PRIME_SCAN_INTERVAL}s"
                            )
                        else:
                            await self._log("WARNING", "RISK",
                                f"Insufficient balance: ${account_balance:.2f} — skipping signal")
                else:
                    # Still run signal to keep data fresh for monitoring
                    signal = await self.signal_engine.generate_signal(
                        candles_1h=candles_1h_raw,
                        candles_6h=candles_6h_raw,
                        current_price=current_price,
                        active_position_side=fresh_open[0].side if fresh_open else None,
                    )
                    self._last_signal = signal.to_dict()

    async def _3m_entry_scan(self) -> bool:
        """
        Scan 3M HA for entry confirmation when in primed state.
        Returns True if a position was entered (primed state cleared).
        """
        if not self._primed_direction:
            return False

        client = get_bitunix_client()
        direction = self._primed_direction

        try:
            candles_3m = await client.get_klines("3m", limit=20)
            if len(candles_3m) < 3:
                return False
            ha_3m = compute_heikin_ashi(candles_3m[:-1])
            if not ha_3m:
                return False
            last_color = ha_3m[-1]["color"]
            # 3M HA must match our trade direction
            if direction == "LONG" and last_color != "GREEN":
                return False
            if direction == "SHORT" and last_color != "RED":
                return False
        except Exception as e:
            logger.debug(f"3M scan failed: {e}")
            return False

        # 3M HA confirmed — execute the entry
        async with AsyncSessionLocal() as db:
            pos_manager = PositionManager(client, db)
            copy_manager = CopyTradingManager(db)

            try:
                ticker = await client.get_ticker()
                current_price = ticker["price"]
            except Exception:
                return False

            # Final exchange position guard
            try:
                ex_pos = await client.get_open_positions()
                if ex_pos:
                    await self._log("WARNING", "RISK",
                        "Exchange already has position — cancelling primed entry")
                    self._primed_direction = None
                    return False
            except Exception:
                pass

            signal_data = self._primed_signal_data or {}
            new_position = await pos_manager.open_position(
                direction=direction,
                current_price=current_price,
                entry_reason=f"3M HA confirmed {direction} (primed signal)",
                signal_data=signal_data,
                account_balance=self._primed_account_balance,
                size_modifier=self._primed_size_modifier,
            )

            if new_position:
                await self._save_zone_state(db)
                if settings.copy_trading_enabled:
                    await copy_manager.open_copy_positions(new_position, signal_data)
                await self._log("INFO", "TRADE",
                    f"3M HA entry: {direction} at {current_price:.2f}")
                logger.info(f"3M HA entry executed: {direction} at {current_price:.2f}")

            # Clear primed state regardless of outcome
            self._primed_direction       = None
            self._primed_at              = None
            self._primed_signal_data     = None
            return new_position is not None

    # ─────────────────────────────────────────────────────────────────
    # Override Controls
    # ─────────────────────────────────────────────────────────────────

    async def emergency_close_all(self, reason: str = "Manual emergency close"):
        """Emergency close all positions — master and copy traders.
        Pauses the bot afterwards so it doesn't immediately re-enter."""
        # Pause first so the main loop doesn't open a new position while we close
        self._paused = True
        await self._set_status(BotStatus.PAUSED)

        client = get_bitunix_client()
        async with AsyncSessionLocal() as db:
            pos_manager = PositionManager(client, db)
            copy_manager = CopyTradingManager(db)

            try:
                ticker = await client.get_ticker()
                current_price = ticker["price"]
            except Exception:
                current_price = 0

            # Step 1: close via exchange positions directly (most reliable)
            try:
                ex_positions = await client.get_open_positions()
                for ep in (ex_positions or []):
                    # API fields: qty (size), side (LONG/SHORT), positionId
                    raw_qty = ep.get("qty")
                    side = ep.get("side", "LONG")
                    position_id = ep.get("positionId")
                    if raw_qty:
                        qty = abs(float(raw_qty))
                        if qty >= 0.0001:
                            result = await client.close_position(side=side, quantity=qty, position_id=position_id)
                            logger.info(f"Emergency exchange close: {side} {qty} → {result}")
            except Exception as e:
                logger.error(f"Emergency exchange close failed: {e}")

            # Step 2: mark all open DB positions closed
            open_positions = await pos_manager.get_open_positions()
            for position in open_positions:
                await pos_manager.close_position(
                    position, current_price, reason,
                    status=PositionStatus.EMERGENCY_CLOSED
                )
                await copy_manager.close_copy_positions(position, reason)

            await copy_manager.emergency_close_all(reason)
            await self._log("WARNING", "OVERRIDE", f"Emergency close all: {reason} — bot paused, resume when ready")
            logger.warning(f"Emergency close all executed: {reason}")

    async def force_open(
        self,
        direction: str,
        reason: str = "Manual override",
    ) -> bool:
        """Force open a position bypassing signal checks."""
        self._manual_override = True
        client = get_bitunix_client()

        try:
            async with AsyncSessionLocal() as db:
                pos_manager = PositionManager(client, db)
                copy_manager = CopyTradingManager(db)

                ticker = await client.get_ticker()
                current_price = ticker["price"]

                balance_data = await client.get_account_balance()
                account_balance = balance_data.get("available", 0)

                position = await pos_manager.open_position(
                    direction=direction,
                    current_price=current_price,
                    entry_reason=f"MANUAL OVERRIDE: {reason}",
                    signal_data={"zone_key": "manual", "strength": "MANUAL", "position_size_modifier": 1.0},
                    account_balance=account_balance,
                )

                if position and settings.copy_trading_enabled:
                    await copy_manager.open_copy_positions(
                        position,
                        {"zone_key": "manual", "strength": "MANUAL", "position_size_modifier": 1.0}
                    )

                await self._log("WARNING", "OVERRIDE", f"Force {direction} opened: {reason}")
                return position is not None
        except Exception as e:
            logger.error(f"Force open failed: {e}")
            return False
        finally:
            self._manual_override = False

    # ─────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────

    async def _set_status(self, status: BotStatus, error_msg: str = None):
        async with AsyncSessionLocal() as db:
            await db.execute(
                update(BotState).where(BotState.id == 1).values(
                    status=status,
                    error_message=error_msg,
                    uptime_start=datetime.utcnow() if status == BotStatus.RUNNING else None,
                    updated_at=datetime.utcnow(),
                )
            )
            await db.commit()

    async def _log(self, level: str, category: str, message: str, details: str = None):
        async with AsyncSessionLocal() as db:
            log = BotLog(level=level, category=category, message=message, details=details)
            db.add(log)
            await db.commit()

    async def _update_bot_stats(self, db, position: Position):
        gross_pnl = position.realized_pnl_usd or 0
        fees = position.fees_usd if position.fees_usd is not None else (position.position_size_usd or 0) * 0.0012
        pnl = gross_pnl - fees  # track net PnL after fees
        is_win = pnl > 0
        await db.execute(
            update(BotState).where(BotState.id == 1).values(
                total_trades=BotState.total_trades + 1,
                winning_trades=BotState.winning_trades + (1 if is_win else 0),
                total_pnl_usd=BotState.total_pnl_usd + pnl,
                last_signal=position.side,
                last_signal_time=datetime.utcnow(),
            )
        )
        await db.commit()

    async def _load_zone_state(self):
        """Restore zone cooldowns from DB into the in-memory zone tracker on startup."""
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(select(ZoneMemory))
                rows = result.scalars().all()
                loaded = 0
                for row in rows:
                    if row.cooldown_until and row.cooldown_until > datetime.utcnow():
                        zt = self.signal_engine.zone_tracker
                        state = zt.get_state(row.zone_key)
                        state[row.direction] = {
                            "count": row.signal_count,
                            "last_signal": row.last_signal_at,
                            "cooldown_until": row.cooldown_until,
                        }
                        loaded += 1
                if loaded:
                    logger.info(f"Restored {loaded} active zone cooldowns from DB")
        except Exception as e:
            logger.warning(f"Could not load zone state from DB: {e}")

    async def _save_zone_state(self, db):
        """Persist current in-memory zone tracker state to DB after a trade is entered."""
        try:
            await db.execute(delete(ZoneMemory))
            zt = self.signal_engine.zone_tracker
            for zone_key, dirs in zt._zone_state.items():
                for direction, state in dirs.items():
                    db.add(ZoneMemory(
                        zone_key=zone_key,
                        direction=direction,
                        signal_count=state.get("count", 0),
                        last_signal_at=state.get("last_signal"),
                        cooldown_until=state.get("cooldown_until"),
                    ))
            await db.commit()
        except Exception as e:
            logger.warning(f"Could not save zone state to DB: {e}")

    def get_status(self) -> Dict:
        return {
            "running": self._running,
            "paused": self._paused,
            "manual_override": self._manual_override,
            "last_signal": self._last_signal,
        }


# Global singleton
_bot_engine: Optional[BotEngine] = None


def get_bot_engine() -> BotEngine:
    global _bot_engine
    if _bot_engine is None:
        _bot_engine = BotEngine()
    return _bot_engine
