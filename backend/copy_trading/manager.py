"""
Copy Trading Manager
Distributes trade signals to all active copy traders.
"""
import asyncio
from datetime import datetime
from typing import List, Optional, Dict
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from backend.database import CopyTrader, CopyPosition, Position, PositionStatus, BotLog
from backend.exchange.bitunix import BitunixClient, get_bitunix_client
from backend.trading.risk_manager import RiskManager
from backend.config import settings


class CopyTradingManager:

    def __init__(self, db: AsyncSession):
        self.db = db
        self.risk_manager = RiskManager()

    async def get_active_traders(self) -> List[CopyTrader]:
        """Get all active copy traders."""
        result = await self.db.execute(
            select(CopyTrader).where(CopyTrader.is_active == True)
        )
        return result.scalars().all()

    async def open_copy_positions(
        self,
        master_position: Position,
        signal_data: Dict,
    ):
        """
        Open copy positions for all active traders when master opens.
        Runs concurrently to minimize slippage.
        """
        if not settings.copy_trading_enabled:
            return

        traders = await self.get_active_traders()
        if not traders:
            return

        logger.info(f"Distributing {master_position.side} signal to {len(traders)} copy traders")

        tasks = [
            self._open_trader_position(trader, master_position, signal_data)
            for trader in traders
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_count = sum(1 for r in results if not isinstance(r, Exception) and r is not None)
        logger.info(f"Copy trade opened for {success_count}/{len(traders)} traders")

    async def _open_trader_position(
        self,
        trader: CopyTrader,
        master_position: Position,
        signal_data: Dict,
    ) -> Optional[CopyPosition]:
        """Open a copy position for a single trader."""

        # Filter by copy direction preferences
        if master_position.side == "LONG" and not trader.copy_longs:
            return None
        if master_position.side == "SHORT" and not trader.copy_shorts:
            return None

        # Get trader's client
        trader_client = get_bitunix_client(trader.api_key, trader.api_secret)

        try:
            # Get trader balance
            balance_data = await trader_client.get_account_balance()
            trader_balance = balance_data.get("available", 0)

            if trader_balance < 10:
                await self._log("WARNING", "COPY_TRADE",
                    f"Trader {trader.nickname}: insufficient balance (${trader_balance:.2f})")
                return None

            # Calculate trader position size
            size_pct = trader.position_size_override_pct or settings.position_size_pct
            size_modifier = signal_data.get("position_size_modifier", 1.0)
            leverage = trader.leverage_override or settings.leverage

            margin_usd = trader_balance * size_pct * size_modifier
            if trader.max_position_usd:
                margin_usd = min(margin_usd, trader.max_position_usd)

            position_size_usd = margin_usd * leverage
            qty_btc = position_size_usd / master_position.entry_price

            # Set leverage
            await trader_client.set_leverage(leverage)

            # Place order
            order_side = "BUY" if master_position.side == "LONG" else "SELL"
            order_result = await trader_client.place_order(
                side=order_side,
                quantity=qty_btc,
                leverage=leverage,
            )

            order_id = str(order_result.get("orderId", "unknown"))

            # Create DB record
            copy_pos = CopyPosition(
                master_position_id=master_position.id,
                trader_id=trader.id,
                exchange_order_id=order_id,
                status=PositionStatus.OPEN,
                entry_price=master_position.entry_price,
                position_size_usd=position_size_usd,
                opened_at=datetime.utcnow(),
            )
            self.db.add(copy_pos)

            # Update trader stats
            trader.last_active = datetime.utcnow()
            trader.total_trades = (trader.total_trades or 0) + 1
            await self.db.commit()

            await self._log("INFO", "COPY_TRADE",
                f"Trader {trader.nickname}: Opened {master_position.side} "
                f"${margin_usd:.0f} margin @ {leverage}x | Order: {order_id}"
            )

            return copy_pos

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Copy trade failed for {trader.nickname}: {error_msg}")

            # Record failed copy position
            copy_pos = CopyPosition(
                master_position_id=master_position.id,
                trader_id=trader.id,
                status=PositionStatus.CLOSED,
                position_size_usd=0,
                error_message=error_msg,
            )
            self.db.add(copy_pos)
            await self.db.commit()

            await self._log("ERROR", "COPY_TRADE",
                f"Trader {trader.nickname}: Failed to open position — {error_msg}"
            )
            return None

    async def close_copy_positions(
        self,
        master_position: Position,
        close_reason: str,
    ):
        """Close all copy positions linked to a master position."""
        result = await self.db.execute(
            select(CopyPosition).where(
                CopyPosition.master_position_id == master_position.id,
                CopyPosition.status == PositionStatus.OPEN,
            )
        )
        copy_positions = result.scalars().all()

        if not copy_positions:
            return

        logger.info(f"Closing {len(copy_positions)} copy positions for master #{master_position.id}")

        tasks = [
            self._close_trader_position(cp, master_position, close_reason)
            for cp in copy_positions
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _close_trader_position(
        self,
        copy_pos: CopyPosition,
        master_position: Position,
        reason: str,
    ):
        """Close a single copy position."""
        # Get trader
        result = await self.db.execute(
            select(CopyTrader).where(CopyTrader.id == copy_pos.trader_id)
        )
        trader = result.scalar_one_or_none()
        if not trader:
            return

        trader_client = get_bitunix_client(trader.api_key, trader.api_secret)

        try:
            # Get current price from master
            ticker = await trader_client.get_ticker()
            current_price = ticker.get("price", master_position.current_price or master_position.entry_price)

            qty_btc = copy_pos.position_size_usd / current_price

            await trader_client.close_position(
                side=master_position.side,
                quantity=qty_btc,
            )

            # Calculate P&L
            if master_position.side == "LONG":
                pnl_pct = ((current_price - copy_pos.entry_price) / copy_pos.entry_price) * 100
            else:
                pnl_pct = ((copy_pos.entry_price - current_price) / copy_pos.entry_price) * 100

            pnl_usd = copy_pos.position_size_usd * (pnl_pct / 100)

            copy_pos.status = PositionStatus.CLOSED
            copy_pos.exit_price = current_price
            copy_pos.realized_pnl_usd = pnl_usd
            copy_pos.closed_at = datetime.utcnow()

            # Update trader stats
            trader.total_pnl_usd = (trader.total_pnl_usd or 0) + pnl_usd
            if pnl_usd > 0:
                trader.win_trades = (trader.win_trades or 0) + 1

            await self.db.commit()

            await self._log("INFO", "COPY_TRADE",
                f"Trader {trader.nickname}: Closed {master_position.side} "
                f"PnL: {pnl_pct:+.1f}% (${pnl_usd:+.2f}) | {reason}"
            )

        except Exception as e:
            logger.error(f"Failed to close copy position for {trader.nickname}: {e}")
            copy_pos.error_message = str(e)
            await self.db.commit()

    async def emergency_close_all(self, reason: str = "Emergency stop"):
        """Emergency close all copy positions across all traders."""
        result = await self.db.execute(
            select(CopyPosition).where(CopyPosition.status == PositionStatus.OPEN)
        )
        open_positions = result.scalars().all()

        for cp in open_positions:
            result2 = await self.db.execute(
                select(CopyTrader).where(CopyTrader.id == cp.trader_id)
            )
            trader = result2.scalar_one_or_none()
            if not trader:
                continue

            try:
                client = get_bitunix_client(trader.api_key, trader.api_secret)
                open_ex = await client.get_open_positions()
                for pos in open_ex:
                    qty = float(pos.get("size", pos.get("qty", 0)))
                    side = pos.get("side", "LONG")
                    if qty > 0:
                        await client.close_position(side, qty)

                cp.status = PositionStatus.EMERGENCY_CLOSED
                cp.closed_at = datetime.utcnow()
                cp.error_message = reason
            except Exception as e:
                logger.error(f"Emergency close failed for trader {cp.trader_id}: {e}")

        await self.db.commit()
        logger.warning(f"Emergency close executed for {len(open_positions)} copy positions")

    async def _log(self, level: str, category: str, message: str):
        log = BotLog(level=level, category=category, message=message)
        self.db.add(log)
        await self.db.commit()
