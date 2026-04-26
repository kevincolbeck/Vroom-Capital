"""
Risk Manager
Calculates position sizes, liquidation prices, and validates trade risk.
"""
from typing import Dict, Optional, Tuple
from loguru import logger
from backend.config import settings


class RiskManager:
    def __init__(self):
        self.leverage = settings.leverage
        self.position_size_pct = settings.position_size_pct
        self.liquidation_buffer_usd = settings.liquidation_buffer_usd

    def calculate_position(
        self,
        account_balance: float,
        current_price: float,
        direction: str,
        size_modifier: float = 1.0,
        leverage_override: Optional[int] = None,
        size_pct_override: Optional[float] = None,
    ) -> Dict:
        """
        Calculate all position parameters for a trade.

        Returns:
            {
              "margin_usd": float,           # USDT to use as margin
              "position_size_usd": float,    # Notional value
              "quantity_btc": float,         # BTC amount to buy/sell
              "leverage": int,
              "liquidation_price": float,
              "liquidation_buffer_actual": float,
              "tp1_price": float,
              "tp2_price": float,
              "risk_reward_1h": float,
              "is_valid": bool,
              "validation_message": str,
            }
        """
        lev = leverage_override or self.leverage
        MAINT_RATE = 0.005
        MAX_NOTIONAL = 5_000_000.0

        # Size FROM the buffer: position is sized so liq is always $buffer away from entry
        target_buf = self.liquidation_buffer_usd * size_modifier
        quantity_btc = account_balance * size_modifier / (target_buf + current_price * MAINT_RATE)
        position_size_usd = quantity_btc * current_price
        if position_size_usd > MAX_NOTIONAL:
            position_size_usd = MAX_NOTIONAL
            quantity_btc = position_size_usd / current_price
        margin_usd = position_size_usd / lev

        # Liquidation price: exactly target_buf away from entry by construction
        if direction == "LONG":
            liquidation_price = current_price - target_buf
            tp1_price = current_price + self.liquidation_buffer_usd * settings.tp1_pct
            tp2_price = current_price + self.liquidation_buffer_usd * settings.tp2_pct
        else:
            liquidation_price = current_price + target_buf
            tp1_price = current_price - self.liquidation_buffer_usd * settings.tp1_pct
            tp2_price = current_price - self.liquidation_buffer_usd * settings.tp2_pct

        liquidation_buffer = target_buf

        # Validate
        is_valid = True
        validation_message = "Position valid"

        if quantity_btc < 0.001:
            is_valid = False
            validation_message = f"Position too small: {quantity_btc:.6f} BTC < 0.001 BTC minimum order size"
        elif margin_usd > account_balance:
            is_valid = False
            validation_message = f"Margin ${margin_usd:.2f} exceeds balance ${account_balance:.2f}"

        return {
            "margin_usd": round(margin_usd, 2),
            "position_size_usd": round(position_size_usd, 2),
            "quantity_btc": round(quantity_btc, 6),
            "leverage": lev,
            "liquidation_price": round(liquidation_price, 2),
            "liquidation_buffer_actual": round(liquidation_buffer, 2),
            "tp1_price": round(tp1_price, 2),
            "tp2_price": round(tp2_price, 2),
            "risk_reward": round((settings.tp1_pct * 100) / (liquidation_buffer / current_price * 100 * lev), 2),
            "is_valid": is_valid,
            "validation_message": validation_message,
        }

    def calculate_pnl(
        self,
        direction: str,
        entry_price: float,
        current_price: float,
        margin_usd: float,
        leverage: int,
    ) -> Dict:
        """Calculate current P&L for an open position."""
        if direction == "LONG":
            price_change_pct = (current_price - entry_price) / entry_price
        else:
            price_change_pct = (entry_price - current_price) / entry_price

        pnl_pct = price_change_pct * leverage * 100
        pnl_usd = margin_usd * (pnl_pct / 100)

        return {
            "pnl_pct": round(pnl_pct, 2),
            "pnl_usd": round(pnl_usd, 2),
            "price_change_pct": round(price_change_pct * 100, 4),
        }

    def is_near_liquidation(
        self,
        direction: str,
        current_price: float,
        liquidation_price: float,
        warning_pct: float = 0.5,
    ) -> Tuple[bool, float]:
        """
        Check if position is dangerously close to liquidation.
        Returns (is_danger: bool, pct_to_liq: float)
        """
        if direction == "LONG":
            pct_to_liq = ((current_price - liquidation_price) / current_price) * 100
        else:
            pct_to_liq = ((liquidation_price - current_price) / current_price) * 100

        return pct_to_liq < warning_pct, pct_to_liq

    def get_copy_trade_size(
        self,
        trader_balance: float,
        master_margin_pct: float,
        leverage_override: Optional[int] = None,
        size_pct_override: Optional[float] = None,
        max_position_usd: Optional[float] = None,
    ) -> Dict:
        """
        Calculate position size for a copy trader.
        Proportional to their account balance.
        """
        size_pct = size_pct_override or master_margin_pct
        margin_usd = trader_balance * size_pct

        if max_position_usd:
            margin_usd = min(margin_usd, max_position_usd)

        lev = leverage_override or self.leverage

        return {
            "margin_usd": round(margin_usd, 2),
            "leverage": lev,
            "size_pct": round(size_pct, 4),
        }
