"""
Position Manager
Manages the lifecycle of trading positions including entry, monitoring, and exit.
"""
from datetime import datetime
from typing import Optional, Dict, List
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from backend.database import Position, PositionStatus, BotLog
from backend.exchange.bitunix import BitunixClient
from backend.trading.risk_manager import RiskManager
from backend.config import settings


class PositionManager:

    def __init__(self, client: BitunixClient, db: AsyncSession):
        self.client = client
        self.db = db
        self.risk_manager = RiskManager()

    async def open_position(
        self,
        direction: str,
        current_price: float,
        entry_reason: str,
        signal_data: Dict,
        account_balance: float,
        size_modifier: float = 1.0,
    ) -> Optional[Position]:
        """Open a new position based on the signal."""

        # Calculate position parameters
        pos_params = self.risk_manager.calculate_position(
            account_balance=account_balance,
            current_price=current_price,
            direction=direction,
            size_modifier=size_modifier,
        )

        if not pos_params["is_valid"]:
            await self._log(
                "WARNING", "POSITION",
                f"Position validation failed: {pos_params['validation_message']}"
            )
            return None

        # Set leverage on exchange
        try:
            await self.client.set_leverage(pos_params["leverage"])
        except Exception as e:
            logger.warning(f"Set leverage failed: {e}")

        # Place order
        order_side = "BUY" if direction == "LONG" else "SELL"
        try:
            order_result = await self.client.place_order(
                side=order_side,
                quantity=pos_params["quantity_btc"],
                leverage=pos_params["leverage"],
            )
        except Exception as e:
            await self._log("ERROR", "ORDER", f"Failed to place {direction} order: {e}")
            return None

        data = order_result.get("data") or {}
        order_id = str(order_result.get("orderId") or data.get("orderId") or data.get("orderID") or "unknown")

        # Create DB record
        position = Position(
            exchange_order_id=order_id,
            side=direction,
            status=PositionStatus.OPEN,
            entry_price=current_price,
            current_price=current_price,
            position_size_usd=pos_params["position_size_usd"],
            margin_used_usd=pos_params["margin_usd"],
            leverage=pos_params["leverage"],
            liquidation_price=pos_params["liquidation_price"],
            unrealized_pnl_pct=0.0,
            peak_profit_pct=0.0,
            zone=signal_data.get("zone_key", ""),
            signal_strength=signal_data.get("strength", ""),
            entry_reason=entry_reason,
            ha_6h_color=signal_data.get("ha_6h_color", ""),
            ha_1h_color=signal_data.get("ha_1h_color", ""),
            funding_rate_at_entry=signal_data.get("funding", {}).get("average_rate", None),
            opened_at=datetime.utcnow(),
        )

        self.db.add(position)
        await self.db.commit()
        await self.db.refresh(position)

        await self._log(
            "INFO", "POSITION",
            f"Opened {direction} @ ${current_price:,.0f} | "
            f"Size: ${pos_params['margin_usd']:.0f} margin | "
            f"Liq: ${pos_params['liquidation_price']:,.0f} | "
            f"Order: {order_id}",
            details=str(pos_params)
        )

        return position

    async def update_position(self, position: Position, current_price: float) -> Position:
        """Update position with current price and P&L."""
        pnl = self.risk_manager.calculate_pnl(
            direction=position.side,
            entry_price=position.entry_price,
            current_price=current_price,
            margin_usd=position.margin_used_usd,
            leverage=position.leverage,
        )

        position.current_price = current_price
        position.unrealized_pnl_pct = pnl["pnl_pct"]

        # Track peak profit
        if pnl["pnl_pct"] > position.peak_profit_pct:
            position.peak_profit_pct = pnl["pnl_pct"]

        await self.db.commit()
        return position

    async def close_position(
        self,
        position: Position,
        current_price: float,
        reason: str,
        status: PositionStatus = PositionStatus.CLOSED,
    ) -> Position:
        """Close a position and record the result."""
        pnl = self.risk_manager.calculate_pnl(
            direction=position.side,
            entry_price=position.entry_price,
            current_price=current_price,
            margin_usd=position.margin_used_usd,
            leverage=position.leverage,
        )

        # Close on exchange
        try:
            await self.client.close_position(
                side=position.side,
                quantity=position.position_size_usd / current_price,
            )
        except Exception as e:
            await self._log("ERROR", "POSITION", f"Failed to close position on exchange: {e}")

        position.exit_price = current_price
        position.realized_pnl_pct = pnl["pnl_pct"]
        position.realized_pnl_usd = pnl["pnl_usd"]
        position.exit_reason = reason
        position.status = status
        position.closed_at = datetime.utcnow()

        await self.db.commit()
        await self.db.refresh(position)

        emoji = "✅" if pnl["pnl_pct"] > 0 else "❌"
        await self._log(
            "INFO", "POSITION",
            f"{emoji} Closed {position.side} @ ${current_price:,.0f} | "
            f"PnL: {pnl['pnl_pct']:+.1f}% (${pnl['pnl_usd']:+.2f}) | "
            f"Reason: {reason}"
        )

        return position

    async def get_open_positions(self) -> List[Position]:
        """Get all open positions from DB."""
        result = await self.db.execute(
            select(Position).where(Position.status == PositionStatus.OPEN)
        )
        return result.scalars().all()

    async def _log(self, level: str, category: str, message: str, details: str = None):
        """Write a log entry to the database."""
        log = BotLog(level=level, category=category, message=message, details=details)
        self.db.add(log)
        await self.db.commit()
        if level == "ERROR":
            logger.error(f"[{category}] {message}")
        elif level == "WARNING":
            logger.warning(f"[{category}] {message}")
        else:
            logger.info(f"[{category}] {message}")
