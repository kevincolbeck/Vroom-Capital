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

        # Response: {"code": 0, "data": {"orderId": "...", "clientId": "..."}}
        data = order_result.get("data") or {}
        order_id = str(data.get("orderId") or order_result.get("orderId") or "unknown")

        # Fetch actual fill price from exchange (avoids CryptoCompare fallback price mismatch)
        import asyncio as _asyncio
        fill_price = current_price
        try:
            await _asyncio.sleep(2)  # give exchange time to register the position
            ex_positions = await self.client.get_open_positions()
            if ex_positions:
                p = ex_positions[0]
                # API field: avgOpenPrice (average entry price)
                raw = p.get("avgOpenPrice")
                if raw:
                    fill_price = float(raw)
                    logger.info(f"Actual fill price from exchange: ${fill_price:,.2f} (ticker was ${current_price:,.2f})")
        except Exception as e:
            logger.warning(f"Could not fetch fill price from exchange, using ticker: {e}")

        # Create DB record
        position = Position(
            exchange_order_id=order_id,
            side=direction,
            status=PositionStatus.OPEN,
            entry_price=fill_price,
            current_price=fill_price,
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
            f"Opened {direction} @ ${fill_price:,.2f} | "
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

        # Close on exchange — fetch actual position from exchange for qty and positionId
        exchange_closed = False
        try:
            ex_positions = await self.client.get_open_positions()
            qty = None
            position_id = None
            for ep in (ex_positions or []):
                # API fields: qty (position size), positionId (required for HEDGE mode close)
                raw_qty = ep.get("qty")
                if raw_qty:
                    qty = abs(float(raw_qty))
                    position_id = ep.get("positionId")
                    break
            if not qty or qty < 0.0001:
                qty = position.position_size_usd / current_price
            result = await self.client.close_position(side=position.side, quantity=qty, position_id=position_id)
            logger.info(f"Exchange close result: {result}")
            if isinstance(result, dict) and result.get("code") == 0:
                exchange_closed = True
            else:
                await self._log("WARNING", "POSITION", f"Exchange close returned non-success: {result}")
        except Exception as e:
            await self._log("ERROR", "POSITION", f"Failed to close position on exchange: {e}")

        if not exchange_closed:
            await self._log("WARNING", "POSITION", "Exchange close may have failed — DB marked closed anyway")

        position.exit_price = current_price
        position.realized_pnl_pct = pnl["pnl_pct"]
        position.realized_pnl_usd = pnl["pnl_usd"]
        position.exit_reason = reason
        position.status = status
        position.closed_at = datetime.utcnow()

        await self.db.commit()

        # Reconcile with actual Bitunix position history (exact prices, PnL, fees)
        import asyncio as _asyncio
        try:
            await _asyncio.sleep(3)
            history = await self.client.get_history_positions(limit=5)
            if history:
                h = history[0]  # most recent closed position
                close_price  = float(h.get("closePrice") or h.get("avgClosePrice") or 0)
                entry_price  = float(h.get("entryPrice") or h.get("avgOpenPrice") or 0)
                realized_pnl = float(h.get("realizedPNL") or h.get("realizedPnl") or h.get("pnl") or 0)
                fees         = float(h.get("fee") or h.get("tradeFee") or 0)

                if close_price > 0:
                    position.exit_price = close_price
                    logger.info(f"Reconciled exit price from exchange: ${close_price:,.2f} (was ${current_price:,.2f})")
                if entry_price > 0 and abs(entry_price - position.entry_price) > 1:
                    position.entry_price = entry_price
                    logger.info(f"Reconciled entry price from exchange: ${entry_price:,.2f}")
                if realized_pnl != 0:
                    # Bitunix realizedPNL excludes fees — store gross, fees separate
                    position.realized_pnl_usd = realized_pnl
                    margin = position.margin_used_usd or 1
                    position.realized_pnl_pct = round(realized_pnl / margin * 100, 2)
                if fees != 0:
                    position.fees_usd = abs(fees)

                await self.db.commit()
                logger.info(
                    f"Exchange reconcile — exit=${close_price:,.2f}, "
                    f"PnL=${realized_pnl:+.4f}, fees=${abs(fees):.4f}"
                )
        except Exception as e:
            logger.warning(f"Could not reconcile with exchange history: {e}")

        await self.db.refresh(position)

        fees_str = f" | Fees: -${abs(position.fees_usd):.4f}" if position.fees_usd else ""
        net_pnl = (position.realized_pnl_usd or 0) - (position.fees_usd or 0)
        emoji = "✅" if net_pnl > 0 else "❌"
        await self._log(
            "INFO", "POSITION",
            f"{emoji} Closed {position.side} @ ${position.exit_price:,.2f} | "
            f"Gross: ${position.realized_pnl_usd:+.4f}{fees_str} | "
            f"Net: ${net_pnl:+.4f} | Reason: {reason}"
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
