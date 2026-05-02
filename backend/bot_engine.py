"""
Bot Engine — Main autonomous trading loop.
Runs continuously, checking for signals and managing positions.
"""
import asyncio
import time as _time_module
from datetime import datetime
from typing import Optional, Dict
from loguru import logger

from backend.database import AsyncSessionLocal, BotState, BotStatus, Position, PositionStatus, BotLog, ZoneMemory
from backend.exchange.bitunix import get_bitunix_client
from backend.strategy.signal_engine import SignalEngine, TradeSignal
from backend.strategy.liquidation_stream import LiquidationStreamMonitor
from backend.strategy.orderbook_stream import OrderBookMonitor
from backend.strategy.trade_flow import TradeFlowMonitor
from backend.trading.position_manager import PositionManager
from backend.copy_trading.manager import CopyTradingManager
from backend.config import settings
from sqlalchemy import select, update, delete


class BotEngine:

    LOOP_INTERVAL_SECONDS = 60      # Full signal scan when flat
    CANDLE_CHECK_SECONDS  = 15      # HA-based exit check (needs klines)
    PRICE_POLL_SECONDS    = 0.1     # Trailing stop check (ticker only) — 10x/s (Bitunix limit)

    def __init__(self):
        self.liq_stream_monitor = LiquidationStreamMonitor()
        self.ob_monitor = OrderBookMonitor()
        self.trade_flow_monitor = TradeFlowMonitor()
        self.signal_engine = SignalEngine(
            liq_stream_monitor=self.liq_stream_monitor,
            ob_monitor=self.ob_monitor,
            trade_flow_monitor=self.trade_flow_monitor,
        )
        self._running = False
        self._paused = False
        self._task: Optional[asyncio.Task] = None
        self._last_signal: Optional[Dict] = None
        self._current_position: Optional[Dict] = None
        self._manual_override: bool = False
        self._last_reconcile_ts: float = 0.0
        self._liq_target: Optional[float] = None  # liq cluster TP price for open position
        self._outcome_task: Optional[asyncio.Task] = None

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
        self.liq_stream_monitor.start()
        self.ob_monitor.start()
        self.trade_flow_monitor.start()
        await self._load_zone_state()
        await self._restore_liq_target()
        self._task = asyncio.create_task(self._main_loop())
        self._outcome_task = asyncio.create_task(self._outcome_recorder_loop(), name="outcome_recorder")
        await self._set_status(BotStatus.RUNNING)
        await self._log("INFO", "BOT", "Bot engine started")
        logger.info("Bot engine started")

    async def stop(self):
        """Stop the bot engine gracefully."""
        self._running = False
        if self._outcome_task:
            self._outcome_task.cancel()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.liq_stream_monitor.stop()
        self.ob_monitor.stop()
        self.trade_flow_monitor.stop()
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

        When flat  : poll every CANDLE_CHECK_SECONDS (15s) — fresh klines keep
                     forming HA colors current; Hyblock's 60s internal cache
                     prevents API rate-limit issues on the extra ticks.
        In position: 1s price-only trailing stop + liq cluster TP check,
                     15s candle-based HA exit check,
                     60s full signal refresh.
        """
        _time = _time_module
        last_candle_check = 0.0
        last_full_tick    = 0.0

        while self._running:
            try:
                now = _time.monotonic()
                has_open = await self._has_open_position()

                if has_open:
                    # 1s: trailing stop + liq cluster TP (ticker only, no klines)
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
                    # When flat: re-evaluate entries every 15s so forming HA
                    # colors stay current (Hyblock uses its own 60s cache).
                    await self._tick()
                    last_full_tick    = _time.monotonic()
                    last_candle_check = _time.monotonic()
                    await asyncio.sleep(self.CANDLE_CHECK_SECONDS)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Bot loop error: {e}")
                await self._set_status(BotStatus.ERROR)
                await self._log("ERROR", "BOT", f"Bot loop error: {e}", details=str(e))
                await asyncio.sleep(30)
                await self._set_status(BotStatus.RUNNING)
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

                # Liq cluster TP — primary exit target (price-based, checked at 1s)
                if self._liq_target is not None:
                    hits = (
                        (position.side == "LONG"  and current_price >= self._liq_target) or
                        (position.side == "SHORT" and current_price <= self._liq_target)
                    )
                    if hits:
                        exit_reason = (
                            f"Liq cluster TP: ${current_price:,.0f} reached "
                            f"target ${self._liq_target:,.0f}"
                        )
                        await pos_manager.close_position(position, current_price, exit_reason)
                        self._liq_target = None
                        await copy_manager.close_copy_positions(position, exit_reason)
                        await self._update_bot_stats(db, position)
                        continue

                if not self._manual_override:
                    should_exit, exit_reason = self.signal_engine.check_trailing_stop(
                        position_side=position.side,
                        entry_price=position.entry_price,
                        current_price=current_price,
                        peak_profit_pct=position.peak_profit_pct,
                    )
                    if should_exit:
                        await pos_manager.close_position(position, current_price, exit_reason)
                        self._liq_target = None
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
        import time as _time
        client = get_bitunix_client()
        async with AsyncSessionLocal() as db:
            pos_manager = PositionManager(client, db)

            # Reconcile every 30s: if exchange is flat but DB shows OPEN, close the record
            now = _time.monotonic()
            if now - self._last_reconcile_ts >= 30.0:
                self._last_reconcile_ts = now
                try:
                    n = await pos_manager.reconcile_positions()
                    if n:
                        logger.info(f"Reconciled {n} manually-closed position(s) from exchange")
                        self._liq_target = None
                        return
                except Exception as e:
                    logger.warning(f"Position reconcile error: {e}")

            try:
                ticker = await client.get_ticker()
                current_price = ticker["price"]
            except Exception:
                return

            open_positions = await pos_manager.get_open_positions()
            if not open_positions:
                return

            copy_manager = CopyTradingManager(db)
            for position in open_positions:
                position = await pos_manager.update_position(position, current_price)

                if not self._manual_override:
                    should_exit, exit_reason = self.signal_engine.get_exit_signal(
                        position_side=position.side,
                        entry_price=position.entry_price,
                        current_price=current_price,
                        peak_profit_pct=position.peak_profit_pct,
                    )
                    if should_exit:
                        await pos_manager.close_position(position, current_price, exit_reason)
                        self._liq_target = None
                        await copy_manager.close_copy_positions(position, exit_reason)
                        await self._update_bot_stats(db, position)

    @staticmethod
    def _inject_forming_candle(candles: list, interval_seconds: int, current_price: float) -> list:
        """Bitunix returns only closed candles. If the last candle is from a previous
        period, append a synthetic forming candle so HA reflects the current price."""
        if not candles:
            return candles
        import time as _t
        now_ms = int(_t.time() * 1000)
        interval_ms = interval_seconds * 1000
        current_period_start = (now_ms // interval_ms) * interval_ms
        if candles[-1]["open_time"] < current_period_start:
            last_close = candles[-1]["close"]
            candles = candles + [{
                "open_time": current_period_start,
                "open":   last_close,
                "high":   max(last_close, current_price),
                "low":    min(last_close, current_price),
                "close":  current_price,
                "volume": 0,
            }]
        return candles

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

            try:
                candles_3m_raw = await client.get_klines("3m", limit=20)
            except Exception:
                candles_3m_raw = []

            # Bitunix omits the forming candle — inject it so HA is current
            candles_1h_raw = self._inject_forming_candle(candles_1h_raw, 3600,  current_price)
            candles_6h_raw = self._inject_forming_candle(candles_6h_raw, 21600, current_price)
            candles_3m_raw = self._inject_forming_candle(candles_3m_raw, 180,   current_price)

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
                    )
                    if should_exit:
                        await pos_manager.close_position(position, current_price, exit_reason)
                        self._liq_target = None
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
                        candles_3m=candles_3m_raw,
                        current_price=current_price,
                        active_position_side=active_side,
                    )

                    # Daily trade cap — run before writing to DB so block reason is captured
                    if signal.should_trade:
                        daily_count = await self._daily_trade_count(db, signal.direction)
                        daily_max = settings.daily_max_longs if signal.direction == "LONG" else settings.daily_max_shorts
                        if daily_count >= daily_max:
                            await self._log("WARNING", "RISK",
                                f"Daily {signal.direction} cap reached: {daily_count}/{daily_max} trades today — skipping")
                            signal.should_trade = False
                            signal.block_reasons.append(f"Daily {signal.direction} cap: {daily_count}/{daily_max}")

                    self._last_signal = signal.to_dict()
                    await self._log_signal_tick(signal)
                    await self._write_signal_tick(signal, current_price, fired=False)
                    asyncio.create_task(self._write_market_snapshot(current_price, signal))

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

                    if signal.should_trade:
                        try:
                            balance_data = await client.get_account_balance()
                            account_balance = balance_data.get("available", 0)
                        except Exception:
                            account_balance = 0

                        if account_balance >= 5:
                            new_position = await pos_manager.open_position(
                                direction=signal.direction,
                                current_price=current_price,
                                entry_reason=signal.entry_reason,
                                signal_data=signal.to_dict(),
                                account_balance=account_balance,
                                size_modifier=signal.position_size_modifier,
                            )
                            if new_position:
                                self._liq_target = signal.liq_target_price
                                if self._liq_target:
                                    logger.info(f"Liq cluster TP target set: ${self._liq_target:,.0f}")
                                # Record zone signal only on confirmed trade entry — not on monitoring ticks
                                self.signal_engine.zone_tracker.record_signal(signal.zone_key, signal.direction)
                                await self._save_zone_state(db)
                                if settings.copy_trading_enabled:
                                    await copy_manager.open_copy_positions(new_position, signal.to_dict())
                                _liq_str = f"${signal.liq_target_price:,.0f}" if signal.liq_target_price else "none"
                                await self._log("INFO", "TRADE",
                                    f"Entry: {signal.direction} @ ${current_price:,.2f} | "
                                    f"Conf={signal.confidence_score:.0f}% | LiqTP={_liq_str} | "
                                    f"HA: 3m={signal.ha_3m_color} 1h={signal.ha_1h_color} 6h={signal.ha_6h_color}")
                                logger.info(f"Entry executed: {signal.direction} at {current_price:.2f}")
                                await self._write_signal_tick(signal, current_price, fired=True)
                        else:
                            await self._log("WARNING", "RISK",
                                f"Insufficient balance: ${account_balance:.2f} — skipping signal")
                else:
                    # Still run signal to keep data fresh for monitoring
                    signal = await self.signal_engine.generate_signal(
                        candles_1h=candles_1h_raw,
                        candles_6h=candles_6h_raw,
                        candles_3m=candles_3m_raw,
                        current_price=current_price,
                        active_position_side=fresh_open[0].side if fresh_open else None,
                    )
                    self._last_signal = signal.to_dict()
                    await self._write_signal_tick(signal, current_price, fired=False)
                    asyncio.create_task(self._write_market_snapshot(current_price, signal))

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
            self._liq_target = None
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

    async def _restore_liq_target(self):
        """Restore liq cluster TP price from any open position in DB on startup."""
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(Position).where(Position.status == PositionStatus.OPEN).limit(1)
                )
                pos = result.scalar_one_or_none()
                if pos and pos.liq_target_price:
                    self._liq_target = pos.liq_target_price
                    logger.info(f"Restored liq cluster TP from DB: ${self._liq_target:,.0f}")
        except Exception as e:
            logger.warning(f"Could not restore liq target from DB: {e}")

    async def _log_signal_tick(self, signal: "TradeSignal"):
        """Write a per-tick signal summary to BotLog for post-trade analysis.

        Only logs when a direction was determined (HA aligned) — earlier blocks
        (6h neutral, velocity) are journalctl-only via logger.info in signal_engine.
        """
        if signal.direction is None:
            return

        mii   = (signal.hyblock_analysis or {}).get("market_imbalance_index", 0.0)
        obi   = (signal.hyblock_analysis or {}).get("obi_slope_direction", "NEUTRAL")
        fund  = (signal.funding_analysis  or {}).get("overall_sentiment", "NEUTRAL")
        liq_str = f"${signal.liq_target_price:,.0f}" if signal.liq_target_price else "none"
        ha_str  = f"3m={signal.ha_3m_color} 1h={signal.ha_1h_color} 6h={signal.ha_6h_color}"
        blocks  = " | ".join(signal.block_reasons[:2]) if signal.block_reasons else "none"

        if signal.should_trade:
            msg = (
                f"SIGNAL FIRED: {signal.direction} | "
                f"Conf={signal.confidence_score:.0f}% | MII={mii:+.2f} | "
                f"LiqTP={liq_str} | OBI={obi} | Funding={fund} | {ha_str}"
            )
            level = "INFO"
        else:
            msg = (
                f"BLOCKED {signal.direction}: {blocks} | "
                f"MII={mii:+.2f} | {ha_str} | Conf={signal.confidence_score:.0f}%"
            )
            level = "DEBUG"

        await self._log(level, "SIGNAL", msg)

    async def _write_signal_tick(self, signal: "TradeSignal", price: float, fired: bool = False):
        """
        Persist the full signal state to signal_ticks for future backtesting / ML training.
        Only called when direction is not None so neutral ticks are excluded.
        """
        if not signal.direction:
            return
        import json as _json
        from backend.database import SignalTick
        hyblock = signal.hyblock_analysis or {}
        funding = signal.funding_analysis  or {}
        liq     = hyblock.get("liq_clusters") or {}
        liq_lvls = hyblock.get("liq_levels") or {}
        tick = SignalTick(
            ts=datetime.utcnow(),
            price=price,
            direction=signal.direction,
            should_trade=signal.should_trade,
            fired=fired,
            confidence_score=round(signal.confidence_score, 1),
            position_size_modifier=signal.position_size_modifier,
            block_reasons=_json.dumps(signal.block_reasons) if signal.block_reasons else None,
            ha_6h_color=signal.ha_6h_color,
            ha_1h_color=signal.ha_1h_color,
            ha_3m_color=signal.ha_3m_color,
            ha_6h_trend=signal.ha_6h_trend,
            ha_6h_green_count=signal.ha_6h_green_count,
            ha_6h_red_count=signal.ha_6h_red_count,
            ha_1h_consecutive=signal.ha_1h_consecutive,
            mii=hyblock.get("market_imbalance_index"),
            obi_direction=hyblock.get("obi_slope_direction"),
            whale_sentiment=hyblock.get("whale_sentiment"),
            top_trader_sentiment=hyblock.get("top_trader_sentiment"),
            volume_delta_sentiment=hyblock.get("volume_delta_sentiment"),
            cascade_risk=hyblock.get("cascade_risk"),
            liq_above_pct=liq.get("above_pct"),
            liq_below_pct=liq.get("below_pct"),
            liq_target_price=signal.liq_target_price,
            funding_rate=funding.get("average_rate"),
            funding_sentiment=funding.get("overall_sentiment"),
            # Precision scalping signals
            mii_15m=hyblock.get("market_imbalance_index"),  # now 15m timeframe
            liq_level_nearest_long_pct=signal.liq_level_long_pct,
            liq_level_nearest_short_pct=signal.liq_level_short_pct,
            liq_level_long_size=signal.liq_level_long_size,
            liq_level_short_size=signal.liq_level_short_size,
            volume_ratio=signal.volume_ratio,
            buy_sell_count_ratio=signal.buy_sell_count_ratio,
            cascade_direction=signal.cascade_direction,
            cvd=hyblock.get("cvd"),
            oi_delta_pct=hyblock.get("oi_delta_pct"),
            # Exact cascade trigger prices
            liq_long_cluster_price=liq_lvls.get("long_cluster_price"),
            liq_short_cluster_price=liq_lvls.get("short_cluster_price"),
            # Liq level size/count oscillators
            liq_levels_size_delta=hyblock.get("liq_levels_size_delta"),
            liq_levels_count_delta=hyblock.get("liq_levels_count_delta"),
            # Heatmap cluster BTC sizes
            liq_above_size=liq.get("above_size"),
            liq_below_size=liq.get("below_size"),
            # MII sustained bars
            mii_sustained_bars=hyblock.get("mii_sustained_bars"),
            # WarriorAI-aligned HA scoring components
            ha_6h_body_pct=signal.ha_6h_body_pct,
            ha_1h_aligned_count=signal.ha_1h_aligned_count,
            # Retail/global positioning (contrarian signals)
            true_retail_long_pct=hyblock.get("true_retail_long_pct"),
            global_accounts_long_pct=hyblock.get("global_accounts_long_pct"),
            # Net long/short delta
            net_ls_delta=hyblock.get("net_ls_delta"),
            # Cumulative liq zone bias
            cumulative_liq_bias=hyblock.get("cumulative_liq_bias"),
            # Previous day structure
            prev_day_structure=hyblock.get("prev_day_structure"),
            # Round number zone proximity (nearest $5K level distance)
            round_number_dist_pct=min(
                abs(price - r) / price * 100
                for r in [65000, 70000, 75000, 80000, 85000, 90000, 95000, 100000]
            ),
            # 4H compression flag
            is_compressed=hyblock.get("is_compressed"),
            # Gap 2 — 3m velocity toward liq target
            velocity_toward_target=signal.velocity_toward_target,
            velocity_pct_3m=signal.velocity_pct_3m,
            # Gap 3 — 3m HA momentum burst
            ha_3m_aligned_count=signal.ha_3m_aligned_count,
            ha_3m_expanding=signal.ha_3m_expanding,
            # Spot/futures divergence
            cvd_spot=hyblock.get("cvd_spot"),
            basis_pct=(signal.spot_flow_analysis or {}).get("basis_pct"),
            # Walk-forward: block stage, mode, score breakdown
            block_stage=signal.block_stage,
            mode=signal.mode,
            score_breakdown=_json.dumps(signal.score_breakdown) if signal.score_breakdown else None,
            # Regime
            regime=signal.regime,
            regime_er=signal.regime_er,
            regime_vol_ratio=signal.regime_vol_ratio,
            # Order book
            ob_bid_wall_pct=(signal.ob_state or {}).get("bid_wall_pct"),
            ob_bid_wall_size_btc=(signal.ob_state or {}).get("bid_wall_size_btc"),
            ob_ask_wall_pct=(signal.ob_state or {}).get("ask_wall_pct"),
            ob_ask_wall_size_btc=(signal.ob_state or {}).get("ask_wall_size_btc"),
            ob_book_imbalance=(signal.ob_state or {}).get("book_imbalance"),
            ob_blocking_wall_size_btc=(signal.ob_state or {}).get("blocking_wall_size_btc"),
            # Trade flow
            tf_taker_buy_ratio_5m=(signal.trade_flow_state or {}).get("taker_buy_ratio_5m"),
            tf_taker_buy_ratio_15m=(signal.trade_flow_state or {}).get("taker_buy_ratio_15m"),
            tf_buy_volume_5m=(signal.trade_flow_state or {}).get("buy_volume_5m"),
            tf_sell_volume_5m=(signal.trade_flow_state or {}).get("sell_volume_5m"),
            # Live liq stream
            live_cascade_live=(signal.live_liq_state or {}).get("cascade_live"),
            live_cascade_direction=(signal.live_liq_state or {}).get("cascade_direction"),
            live_hawkes_intensity=(signal.live_liq_state or {}).get("hawkes_intensity"),
            live_liq_rate_btc_min=(signal.live_liq_state or {}).get("liq_rate_btc_min"),
            # Funding trajectory
            funding_trajectory=(signal.funding_trajectory_data or {}).get("trajectory"),
            funding_slope_per_min=(signal.funding_trajectory_data or {}).get("slope_per_min"),
            # 24h range
            dist_from_24h_high_pct=signal.dist_from_24h_high_pct,
            dist_from_24h_low_pct=signal.dist_from_24h_low_pct,
            # 6h HA levels
            ha_6h_high=signal.ha_6h_high,
            ha_6h_low=signal.ha_6h_low,
            ha_prev_6h_color=signal.ha_prev_6h_color,
        )
        try:
            async with AsyncSessionLocal() as db:
                db.add(tick)
                await db.commit()
        except Exception as e:
            logger.warning(f"SignalTick write failed: {e}")

    async def _write_market_snapshot(self, price: float, signal: "TradeSignal"):
        """Write a full market state snapshot row every tick for walk-forward backtesting."""
        from backend.database import MarketSnapshot
        tf   = signal.trade_flow_state or {}
        ob   = signal.ob_state or {}
        liq  = signal.live_liq_state or {}
        hb   = signal.hyblock_analysis or {}
        fn   = signal.funding_trajectory_data or {}
        fund = signal.funding_analysis or {}
        snap = MarketSnapshot(
            ts=datetime.utcnow(),
            price=price,
            ha_1h_color=signal.ha_1h_color,
            ha_6h_color=signal.ha_6h_color,
            ha_3m_color=signal.ha_3m_color,
            ha_1h_aligned_count=signal.ha_1h_aligned_count,
            ha_6h_body_pct=signal.ha_6h_body_pct,
            regime=signal.regime,
            regime_er=signal.regime_er,
            regime_vol_ratio=signal.regime_vol_ratio,
            tf_taker_buy_ratio_5m=tf.get("taker_buy_ratio_5m"),
            tf_taker_buy_ratio_15m=tf.get("taker_buy_ratio_15m"),
            tf_buy_volume_5m=tf.get("buy_volume_5m"),
            tf_sell_volume_5m=tf.get("sell_volume_5m"),
            tf_total_volume_5m=tf.get("total_volume_5m"),
            tf_connected=tf.get("connected"),
            ob_bid_wall_price=ob.get("bid_wall_price"),
            ob_bid_wall_size_btc=ob.get("bid_wall_size_btc"),
            ob_bid_wall_pct=ob.get("bid_wall_pct"),
            ob_ask_wall_price=ob.get("ask_wall_price"),
            ob_ask_wall_size_btc=ob.get("ask_wall_size_btc"),
            ob_ask_wall_pct=ob.get("ask_wall_pct"),
            ob_book_imbalance=ob.get("book_imbalance"),
            ob_synced=ob.get("synced"),
            liq_cascade_live=liq.get("cascade_live"),
            liq_cascade_direction=liq.get("cascade_direction"),
            liq_rate_btc_min=liq.get("liq_rate_btc_min"),
            liq_long_btc_min=liq.get("long_liq_btc_min"),
            liq_short_btc_min=liq.get("short_liq_btc_min"),
            liq_hawkes_intensity=liq.get("hawkes_intensity"),
            liq_accelerating=liq.get("accelerating"),
            liq_connected=liq.get("connected"),
            funding_rate=fund.get("average_rate"),
            funding_sentiment=fund.get("overall_sentiment"),
            funding_trajectory=fn.get("trajectory"),
            funding_slope_per_min=fn.get("slope_per_min"),
            mii=hb.get("market_imbalance_index"),
            obi_direction=hb.get("obi_slope_direction"),
            cascade_risk=hb.get("cascade_risk"),
            whale_sentiment=hb.get("whale_sentiment"),
        )
        try:
            async with AsyncSessionLocal() as db:
                db.add(snap)
                await db.commit()
        except Exception as e:
            logger.debug(f"MarketSnapshot write failed: {e}")

    async def _outcome_recorder_loop(self):
        """Background task: fills price outcomes for past signal ticks every 2 minutes."""
        while self._running:
            try:
                await asyncio.sleep(120)
                if not self._running:
                    break
                await self._fill_pending_outcomes()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Outcome recorder error: {e}")

    async def _fill_pending_outcomes(self):
        """Check signal ticks from last 25h; fill price outcomes for elapsed horizons."""
        from backend.database import SignalOutcome, SignalTick
        from datetime import timedelta
        from sqlalchemy import select as _select

        client = get_bitunix_client()
        try:
            ticker = await client.get_ticker()
            current_price = ticker["price"]
        except Exception:
            return

        now = datetime.utcnow()
        HORIZONS = [
            (15,       "price_15m", "return_15m_pct", "correct_15m", "ts_filled_15m"),
            (60,       "price_1h",  "return_1h_pct",  "correct_1h",  "ts_filled_1h"),
            (240,      "price_4h",  "return_4h_pct",  "correct_4h",  "ts_filled_4h"),
            (480,      "price_8h",  "return_8h_pct",  "correct_8h",  "ts_filled_8h"),
            (24 * 60,  "price_24h", "return_24h_pct", "correct_24h", "ts_filled_24h"),
        ]

        try:
            async with AsyncSessionLocal() as db:
                cutoff = now - timedelta(hours=25)
                result = await db.execute(
                    _select(SignalTick)
                    .where(SignalTick.ts >= cutoff)
                    .where(SignalTick.direction != None)
                )
                ticks = result.scalars().all()

                for tick in ticks:
                    out_result = await db.execute(
                        _select(SignalOutcome).where(SignalOutcome.signal_tick_id == tick.id)
                    )
                    outcome = out_result.scalar_one_or_none()
                    if outcome is None:
                        outcome = SignalOutcome(
                            signal_tick_id=tick.id,
                            entry_price=tick.price,
                            direction=tick.direction,
                        )
                        db.add(outcome)

                    minutes_elapsed = (now - tick.ts).total_seconds() / 60.0
                    for minutes, p_col, r_col, c_col, t_col in HORIZONS:
                        if minutes_elapsed >= minutes and getattr(outcome, p_col) is None:
                            setattr(outcome, p_col, current_price)
                            ret = (current_price - tick.price) / tick.price * 100 if tick.price else 0.0
                            setattr(outcome, r_col, round(ret, 4))
                            correct = (
                                (tick.direction == "LONG"  and ret > 0) or
                                (tick.direction == "SHORT" and ret < 0)
                            )
                            setattr(outcome, c_col, correct)
                            setattr(outcome, t_col, now)

                await db.commit()
        except Exception as e:
            logger.debug(f"_fill_pending_outcomes error: {e}")

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

    async def _daily_trade_count(self, db, direction: str) -> int:
        """Count positions opened today (UTC) in the given direction."""
        from sqlalchemy import func
        today_utc = datetime.utcnow().date()
        result = await db.execute(
            select(func.count()).select_from(Position).where(
                Position.side == direction,
                func.date(Position.opened_at) == today_utc,
            )
        )
        return result.scalar() or 0

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
