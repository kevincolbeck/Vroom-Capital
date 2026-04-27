"""
Bot Engine — Main autonomous trading loop.
Runs continuously, checking for signals and managing positions.
"""
import asyncio
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

    LOOP_INTERVAL_SECONDS = 60   # Check every 60 seconds

    def __init__(self):
        self.signal_engine = SignalEngine()
        self._running = False
        self._paused = False
        self._task: Optional[asyncio.Task] = None
        self._last_signal: Optional[Dict] = None
        self._current_position: Optional[Dict] = None
        self._manual_override: bool = False

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
        """Main bot loop — runs every 60 seconds."""
        while self._running:
            try:
                await self._tick()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Bot loop error: {e}")
                await self._set_status(BotStatus.ERROR)
                await self._log("ERROR", "BOT", f"Bot loop error: {e}", details=str(e))
                await asyncio.sleep(30)  # Wait before retry
                await self._set_status(BotStatus.RUNNING)
                continue

            await asyncio.sleep(self.LOOP_INTERVAL_SECONDS)

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
            ha_1h = compute_heikin_ashi(candles_1h_raw[-50:])
            ha_6h = compute_heikin_ashi(candles_6h_raw[-20:])

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
                        # Get account balance
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
                                await self._save_zone_state(db)
                                if settings.copy_trading_enabled:
                                    await copy_manager.open_copy_positions(new_position, signal.to_dict())
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

    # ─────────────────────────────────────────────────────────────────
    # Override Controls
    # ─────────────────────────────────────────────────────────────────

    async def emergency_close_all(self, reason: str = "Manual emergency close"):
        """Emergency close all positions — master and copy traders."""
        client = get_bitunix_client()
        async with AsyncSessionLocal() as db:
            pos_manager = PositionManager(client, db)
            copy_manager = CopyTradingManager(db)

            open_positions = await pos_manager.get_open_positions()

            try:
                ticker = await client.get_ticker()
                current_price = ticker["price"]
            except Exception:
                current_price = 0

            for position in open_positions:
                await pos_manager.close_position(
                    position, current_price, reason,
                    status=PositionStatus.EMERGENCY_CLOSED
                )
                await copy_manager.close_copy_positions(position, reason)

            await copy_manager.emergency_close_all(reason)
            await self._log("WARNING", "OVERRIDE", f"Emergency close all: {reason}")
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
        pnl = position.realized_pnl_usd or 0
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
