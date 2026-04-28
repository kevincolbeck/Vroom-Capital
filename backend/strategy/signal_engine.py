"""
Master Signal Engine
Aggregates all strategy components to produce a final trade signal.
"""
import asyncio
from datetime import datetime
from typing import Dict, Optional, Tuple, List
from loguru import logger

from backend.strategy.heikin_ashi import (
    compute_heikin_ashi, get_trend, get_candle_color,
    count_consecutive_opposite, detect_reversal
)
from backend.strategy.zones import ZoneTracker, get_zone_key, get_zone_position
from backend.strategy.time_filter import check_time_filter, get_time_context
from backend.strategy.velocity import check_velocity_filter, compute_velocity
from backend.strategy.funding_rate import FundingRateMonitor
from backend.strategy.macro_calendar import MacroCalendar
from backend.strategy.liquidation_monitor import LiquidationMonitor
from backend.strategy.order_flow import SpotOrderFlowMonitor
from backend.config import settings


class TradeSignal:
    def __init__(self):
        self.direction: Optional[str] = None  # 'LONG' | 'SHORT' | None
        self.strength: str = "WEAK"           # 'STRONG' | 'WEAK' | 'BLOCKED'
        self.confidence_score: float = 0.0    # 0-100
        self.should_trade: bool = False
        self.position_size_modifier: float = 1.0
        self.entry_reason: str = ""
        self.block_reasons: List[str] = []
        self.warnings: List[str] = []

        # Component results
        self.ha_6h_color: str = "NEUTRAL"
        self.ha_1h_color: str = "NEUTRAL"
        self.ha_6h_trend: str = "NEUTRAL"
        self.zone_key: str = ""
        self.zone_position: str = "MID"
        self.velocity_data: Dict = {}
        self.funding_analysis: Dict = {}
        self.macro_context: Dict = {}
        self.time_context: Dict = {}
        self.liquidation_analysis: Dict = {}
        self.spot_flow_analysis: Dict = {}
        self.current_price: float = 0.0

        self.generated_at: datetime = datetime.utcnow()

    def to_dict(self) -> Dict:
        return {
            "direction": self.direction,
            "strength": self.strength,
            "confidence_score": round(self.confidence_score, 1),
            "should_trade": self.should_trade,
            "position_size_modifier": self.position_size_modifier,
            "entry_reason": self.entry_reason,
            "block_reasons": self.block_reasons,
            "warnings": self.warnings,
            "ha_6h_color": self.ha_6h_color,
            "ha_1h_color": self.ha_1h_color,
            "ha_6h_trend": self.ha_6h_trend,
            "zone_key": self.zone_key,
            "zone_position": self.zone_position,
            "velocity": self.velocity_data,
            "funding": self.funding_analysis,
            "macro": self.macro_context,
            "time": self.time_context,
            "liquidation": self.liquidation_analysis,
            "spot_flow": self.spot_flow_analysis,
            "current_price": self.current_price,
            "generated_at": self.generated_at.isoformat(),
        }


class SignalEngine:
    """
    Runs all strategy filters and produces a consolidated trade signal.
    """

    def __init__(self):
        self.zone_tracker = ZoneTracker(
            zone_size=settings.zone_size_usd,
            cooldown_minutes=settings.zone_cooldown_minutes,
        )
        self.funding_monitor = FundingRateMonitor()
        self.macro_calendar = MacroCalendar()
        self.liquidation_monitor = LiquidationMonitor()
        self.order_flow_monitor = SpotOrderFlowMonitor()
        self._prev_price: Optional[float] = None
        self._first_break_zones: Dict[str, Dict] = {}

    async def generate_signal(
        self,
        candles_1h: List[Dict],
        candles_6h: List[Dict],
        current_price: float,
        active_position_side: Optional[str] = None,
    ) -> TradeSignal:
        """
        Main signal generation function.
        Call this every minute from the bot loop.

        Returns a TradeSignal with all analysis results.
        """
        signal = TradeSignal()
        signal.current_price = current_price

        if not candles_1h or not candles_6h:
            signal.block_reasons.append("Insufficient candle data")
            return signal

        # ─── Step 1: Compute Heikin Ashi ───────────────────────────────────
        # Drop the last candle (currently forming, not yet closed) so we only
        # analyze confirmed closed candles — matches what you see on a chart.
        ha_1h = compute_heikin_ashi(candles_1h[:-1][-50:])
        ha_6h = compute_heikin_ashi(candles_6h[:-1][-30:])

        signal.ha_1h_color = get_candle_color(ha_1h)
        signal.ha_6h_color = get_candle_color(ha_6h)
        signal.ha_6h_trend = get_trend(ha_6h, lookback=3)

        # ─── Step 2: Determine preliminary direction ────────────────────────
        # Both HA timeframes must agree AND 6h trend must be confirmed by
        # multiple candles — prevents re-entry right after a reversal when
        # the new forming candle briefly shows the prior color.
        bullish_trend = signal.ha_6h_trend in ("BULLISH", "STRONG_BULLISH")
        bearish_trend = signal.ha_6h_trend in ("BEARISH", "STRONG_BEARISH")

        if signal.ha_1h_color == "GREEN" and signal.ha_6h_color == "GREEN" and bullish_trend:
            candidate_direction = "LONG"
        elif signal.ha_1h_color == "RED" and signal.ha_6h_color == "RED" and bearish_trend:
            candidate_direction = "SHORT"
        else:
            signal.block_reasons.append(
                f"HA not confirmed: 1h={signal.ha_1h_color}, 6h={signal.ha_6h_color}, trend={signal.ha_6h_trend} — wait for alignment"
            )
            signal.direction = None
            signal.strength = "BLOCKED"
            signal.velocity_data = compute_velocity(candles_1h)
            signal.time_context = get_time_context()
            signal.macro_context = self.macro_calendar.get_macro_context()
            return signal

        # ─── Step 3: Zone analysis ─────────────────────────────────────────
        zone_key = get_zone_key(current_price, settings.zone_size_usd)
        zone_position = get_zone_position(current_price, settings.zone_size_usd)
        signal.zone_key = zone_key
        signal.zone_position = zone_position

        # Check zone cooldown
        if self.zone_tracker.is_in_cooldown(zone_key, candidate_direction):
            remaining = self.zone_tracker.get_cooldown_remaining(zone_key, candidate_direction)
            signal.block_reasons.append(
                f"Zone {zone_key} {candidate_direction} in cooldown — {remaining/60:.0f}min remaining"
            )
            signal.direction = candidate_direction
            signal.strength = "BLOCKED"
            return signal

        # ─── Step 4: Second-break (dwarf) rule ─────────────────────────────
        if self._prev_price is not None:
            from backend.strategy.zones import is_whole_number_break
            break_dir = is_whole_number_break(current_price, self._prev_price, settings.zone_size_usd)
            if break_dir:
                if not self.zone_tracker.has_had_first_break(zone_key, break_dir):
                    self.zone_tracker.record_first_break(zone_key, break_dir)
                    signal.block_reasons.append(
                        f"First break of {zone_key} ({break_dir}) — throwing the dwarf, waiting for confirmation"
                    )
                    signal.direction = candidate_direction
                    signal.strength = "BLOCKED"
                    return signal

        self._prev_price = current_price

        # ─── Step 5: Time filter ──────────────────────────────────────────
        time_allowed, time_reason = check_time_filter(candidate_direction)
        time_context = get_time_context()
        signal.time_context = time_context

        if not time_allowed:
            signal.block_reasons.append(time_reason)
            signal.direction = candidate_direction
            signal.strength = "BLOCKED"
            return signal

        # ─── Step 6: Velocity filter ──────────────────────────────────────
        vel_allowed, vel_reason, vel_data = check_velocity_filter(
            candles_1h,
            candidate_direction,
            threshold_pct=settings.velocity_threshold_pct,
            window_hours=settings.velocity_window_hours,
        )
        signal.velocity_data = vel_data

        if not vel_allowed:
            signal.block_reasons.append(vel_reason)
            signal.direction = candidate_direction
            signal.strength = "BLOCKED"
            return signal

        # Kick off I/O-bound tasks concurrently now that all sync filters passed.
        # Both are network fetches — running them in parallel saves ~6-8s per cycle.
        liq_task  = asyncio.create_task(self.liquidation_monitor.fetch_all(current_price))
        spot_task = asyncio.create_task(self.order_flow_monitor.fetch_all(current_price))

        # ─── Step 7: Funding rate check ───────────────────────────────────
        try:
            funding_rates = await self.funding_monitor.fetch_all()
            funding_analysis = self.funding_monitor.analyze_funding(funding_rates)
        except Exception as e:
            logger.warning(f"Funding rate fetch failed: {e}")
            funding_analysis = {"overall_sentiment": "NEUTRAL", "signal_strength": "WEAK",
                                "average_rate": 0.0, "rates": {}, "description": "Data unavailable",
                                "position_modifier": 1.0}
        signal.funding_analysis = funding_analysis

        funding_ok, funding_reason = self.funding_monitor.get_trade_confirmation(
            candidate_direction, funding_analysis
        )
        if not funding_ok:
            liq_task.cancel()
            spot_task.cancel()
            signal.block_reasons.append(funding_reason)
            signal.direction = candidate_direction
            signal.strength = "BLOCKED"
            return signal

        # Only warn when funding is mildly against our direction
        _fs = funding_analysis.get("overall_sentiment", "NEUTRAL")
        _funding_against = (
            (candidate_direction == "LONG"  and _fs == "BEARISH_CONTRARIAN") or
            (candidate_direction == "SHORT" and _fs == "BULLISH_CONTRARIAN")
        )
        if _funding_against and funding_analysis.get("signal_strength") == "MODERATE":
            signal.warnings.append(f"Funding caution: {funding_analysis.get('description', '')}")

        # ─── Step 8: Macro calendar ───────────────────────────────────────
        macro_context = self.macro_calendar.get_macro_context()
        signal.macro_context = macro_context

        if not macro_context["should_trade"]:
            liq_task.cancel()
            spot_task.cancel()
            signal.block_reasons.append(f"Macro block: {macro_context.get('block_description', macro_context['fomc_description'])}")
            signal.direction = candidate_direction
            signal.strength = "BLOCKED"
            return signal

        # ─── Step 8.5: Liquidation positioning analysis ───────────────────
        try:
            liq_data = await liq_task
            liq_score_delta, liq_desc = self.liquidation_monitor.get_trade_context(
                candidate_direction, current_price, liq_data
            )
            signal.liquidation_analysis = {
                **{k: v for k, v in liq_data.items() if not k.startswith("_")},
                "trade_context": liq_desc,
                "score_delta": liq_score_delta,
            }
            if liq_score_delta > 0:
                signal.warnings.append(f"Positioning edge: {liq_desc}")
        except Exception as e:
            logger.warning(f"Liquidation analysis failed: {e}")
            liq_data = {}
            liq_score_delta = 0.0

        # ─── Step 8.6: Spot order flow analysis ──────────────────────────
        spot_score_delta = 0.0
        try:
            # spot_task warmed the cache; re-call with real oi_trend so divergence is accurate.
            await spot_task
            oi_trend = liq_data.get("oi_trend", "FLAT")
            spot_data = await self.order_flow_monitor.fetch_all(current_price, oi_trend)
            spot_score_delta, spot_desc = self.order_flow_monitor.get_trade_context(
                candidate_direction, current_price, spot_data
            )
            signal.spot_flow_analysis = {
                **spot_data,
                "trade_context": spot_desc,
                "score_delta": spot_score_delta,
            }
            if spot_score_delta != 0:
                signal.warnings.append(f"Spot flow: {spot_desc}")
        except Exception as e:
            logger.warning(f"Spot order flow analysis failed: {e}")
            spot_score_delta = 0.0

        # ─── Step 9: Calculate confidence score ───────────────────────────
        score = 50.0  # Base

        # HA alignment bonus
        if signal.ha_6h_trend in ("STRONG_BULLISH", "STRONG_BEARISH"):
            score += 20.0
        elif signal.ha_6h_trend in ("BULLISH", "BEARISH"):
            score += 10.0

        # Zone position bonus
        if (candidate_direction == "SHORT" and zone_position == "TOP"):
            score += 10.0
        elif (candidate_direction == "LONG" and zone_position == "BOTTOM"):
            score += 10.0

        # Funding rate score — confirming means crowd is on the wrong side (squeeze setup)
        funding_sentiment = funding_analysis.get("overall_sentiment", "NEUTRAL")
        funding_confirms = (
            (candidate_direction == "LONG"  and funding_sentiment == "BULLISH_CONTRARIAN") or
            (candidate_direction == "SHORT" and funding_sentiment == "BEARISH_CONTRARIAN")
        )
        if funding_confirms:
            score += 10.0  # crowd is on the other side — high conviction
        elif funding_sentiment == "NEUTRAL":
            score += 5.0   # balanced market
        # mildly contradicting: +0 (trade still allowed, just smaller)

        # Macro bonus
        score += (macro_context.get("position_size_modifier", 1.0) - 0.5) * 20.0

        # Time window bonus (active session = better)
        if time_context.get("risk_level") == "LOW":
            score += 5.0

        # Liquidation positioning bonus/penalty
        score += liq_score_delta

        # Spot order flow bonus/penalty
        score += spot_score_delta

        score = min(100.0, max(0.0, score))
        signal.confidence_score = score

        # ─── Step 10: Determine strength ─────────────────────────────────
        if score >= 70.0 and signal.ha_6h_trend in ("STRONG_BULLISH", "STRONG_BEARISH", "BULLISH", "BEARISH"):
            signal.strength = "STRONG"
        else:
            signal.strength = "WEAK"

        # ─── Step 11: Compute final position size modifier ────────────────
        # Funding modifier is direction-aware:
        #   confirming (crowd on wrong side, squeeze setup) → full size
        #   neutral                                         → full size
        #   mildly contradicting (moderate crowding our way) → 0.75x
        # Strongly contradicting is already blocked in Step 7.
        if funding_confirms or funding_sentiment == "NEUTRAL":
            funding_modifier = 1.0
        else:
            funding_modifier = funding_analysis.get("position_modifier", 1.0)

        combined_modifier = funding_modifier * macro_context.get("position_size_modifier", 1.0)
        signal.position_size_modifier = max(0.25, combined_modifier)

        # ─── Final: Build entry reason ────────────────────────────────────
        signal.direction = candidate_direction
        signal.should_trade = True
        signal.entry_reason = (
            f"{candidate_direction} signal | Zone: {zone_key} ({zone_position}) | "
            f"HA: 6h={signal.ha_6h_color} 1h={signal.ha_1h_color} | "
            f"Confidence: {score:.0f}% | "
            f"Funding: {funding_analysis.get('average_rate', 0)*100:.3f}% | "
            f"Size modifier: {signal.position_size_modifier:.2f}x"
        )

        # Record the zone signal
        self.zone_tracker.record_signal(zone_key, candidate_direction)

        logger.info(f"Signal generated: {signal.direction} ({signal.strength}) — {signal.entry_reason}")
        return signal

    def check_trailing_stop(
        self,
        position_side: str,
        entry_price: float,
        current_price: float,
        peak_profit_pct: float,
    ) -> Tuple[bool, str]:
        """Price-only trailing stop check — no candles needed. Called every 1s."""
        if position_side == "LONG":
            pnl_pct = ((current_price - entry_price) / entry_price) * 100.0 * settings.leverage
        else:
            pnl_pct = ((entry_price - current_price) / entry_price) * 100.0 * settings.leverage

        if pnl_pct >= settings.tp1_pct * 100:
            trail_pct = (
                settings.trailing_after_tp1_peak_high_pct
                if peak_profit_pct >= settings.trailing_peak_threshold_pct
                else settings.trailing_after_tp1_peak_low_pct
            )
            drawdown_from_peak = peak_profit_pct - pnl_pct
            if drawdown_from_peak >= trail_pct:
                return True, (
                    f"Trailing stop triggered: peak={peak_profit_pct:.1f}%, "
                    f"current={pnl_pct:.1f}%, "
                    f"drawdown={drawdown_from_peak:.1f}% > trail={trail_pct}%"
                )
        elif pnl_pct >= (settings.tp1_pct * 100 - 1):
            if peak_profit_pct >= settings.tp1_pct * 100 and (peak_profit_pct - pnl_pct) >= 1.0:
                return True, (
                    f"TP1 trailing stop: reached {peak_profit_pct:.1f}%, now at {pnl_pct:.1f}%"
                )

        return False, "Hold — trailing stop not triggered"

    def get_exit_signal(
        self,
        position_side: str,
        entry_price: float,
        current_price: float,
        peak_profit_pct: float,
        candles_1h_ha: List[Dict],
        candles_6h_ha: List[Dict],
    ) -> Tuple[bool, str]:
        """
        Check if an open position should be closed.
        Returns (should_exit: bool, reason: str)
        """
        if not candles_1h_ha or not candles_6h_ha:
            return False, "Insufficient data for exit check"

        # Calculate current unrealized P&L
        if position_side == "LONG":
            pnl_pct = ((current_price - entry_price) / entry_price) * 100.0 * settings.leverage
        else:
            pnl_pct = ((entry_price - current_price) / entry_price) * 100.0 * settings.leverage

        current_pnl_pct = pnl_pct

        # ─── Trailing stop logic ──────────────────────────────────────────
        if current_pnl_pct >= settings.tp1_pct * 100:
            # We've hit TP1 (20% profit)
            if peak_profit_pct >= settings.trailing_peak_threshold_pct:
                # Peak was 25%+, trail at -5% from peak
                trail_pct = settings.trailing_after_tp1_peak_high_pct
            else:
                # Peak under 25%, trail at -1% from peak
                trail_pct = settings.trailing_after_tp1_peak_low_pct

            drawdown_from_peak = peak_profit_pct - current_pnl_pct
            if drawdown_from_peak >= trail_pct:
                return True, (
                    f"Trailing stop triggered: peak={peak_profit_pct:.1f}%, "
                    f"current={current_pnl_pct:.1f}%, "
                    f"drawdown={drawdown_from_peak:.1f}% > trail={trail_pct}%"
                )

        elif current_pnl_pct >= (settings.tp1_pct * 100 - 1):
            # Near TP1 but fell back — protect gains with 19% trailing
            if peak_profit_pct >= settings.tp1_pct * 100 and (peak_profit_pct - current_pnl_pct) >= 1.0:
                return True, (
                    f"TP1 trailing stop: reached {peak_profit_pct:.1f}%, now at {current_pnl_pct:.1f}%"
                )

        # ─── 4-candle emergency close ─────────────────────────────────────
        consecutive_opp = count_consecutive_opposite(candles_1h_ha, position_side)
        if consecutive_opp >= settings.emergency_candles:
            return True, (
                f"Emergency close: {consecutive_opp} consecutive opposing 1h HA candles "
                f"— thesis is broken"
            )

        # ─── 6h reversal check ───────────────────────────────────────────
        if detect_reversal(candles_6h_ha, position_side):
            return True, "6h Heikin Ashi reversal detected — macro trend changing"

        return False, "Hold — no exit signal"
