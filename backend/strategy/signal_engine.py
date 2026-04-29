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
    count_consecutive_opposite, detect_reversal, drop_in_progress
)
from backend.strategy.zones import ZoneTracker, get_zone_key, get_zone_position
from backend.strategy.time_filter import check_time_filter, get_time_context
from backend.strategy.velocity import check_velocity_filter, compute_velocity
from backend.strategy.funding_rate import FundingRateMonitor
from backend.strategy.macro_calendar import MacroCalendar
from backend.strategy.liquidation_monitor import LiquidationMonitor
from backend.strategy.order_flow import SpotOrderFlowMonitor
from backend.strategy.hyblock import HyblockMonitor
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
        self.hyblock_analysis: Dict = {}
        self.current_price: float = 0.0

        # 3M HA
        self.ha_3m_color: str = "NEUTRAL"

        # Liq cluster take-profit target (set at entry, None if no cluster gate)
        self.liq_target_price: Optional[float] = None

        # Dashboard counters
        self.ha_6h_green_count: int = 0
        self.ha_6h_red_count: int = 0
        self.ha_1h_consecutive: int = 0

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
            "hyblock": self.hyblock_analysis,
            "current_price": self.current_price,
            "ha_3m_color": self.ha_3m_color,
            "liq_target_price": self.liq_target_price,
            "ha_6h_green_count": self.ha_6h_green_count,
            "ha_6h_red_count": self.ha_6h_red_count,
            "ha_1h_consecutive": self.ha_1h_consecutive,
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
        self.hyblock_monitor = HyblockMonitor()
        self._prev_price: Optional[float] = None
        self._first_break_zones: Dict[str, Dict] = {}

    async def generate_signal(
        self,
        candles_1h: List[Dict],
        candles_6h: List[Dict],
        current_price: float,
        active_position_side: Optional[str] = None,
        candles_3m: Optional[List[Dict]] = None,
    ) -> TradeSignal:
        """
        Main signal generation function.
        Call this every minute from the bot loop.

        Entry is triggered by MII + liq cluster (primary), confirmed by
        3m/1h/6h HA current forming candle alignment.

        Returns a TradeSignal with all analysis results.
        """
        signal = TradeSignal()
        signal.current_price = current_price

        if not candles_1h or not candles_6h:
            signal.block_reasons.append("Insufficient candle data")
            return signal

        # ─── Step 1: Compute Heikin Ashi using CURRENT FORMING candle ──────
        # For entry: include the current forming candle (candles[-1]) so
        # sub-minute entries fire without waiting for an hourly close.
        # For exits (get_exit_signal): drop_in_progress is still used to
        # avoid exiting on a forming candle that hasn't confirmed.
        ha_1h = compute_heikin_ashi(candles_1h[-50:])
        ha_6h = compute_heikin_ashi(candles_6h[-30:])
        ha_3m = compute_heikin_ashi((candles_3m or [])[-20:])

        signal.ha_1h_color = get_candle_color(ha_1h)
        signal.ha_6h_color = get_candle_color(ha_6h)
        signal.ha_3m_color = get_candle_color(ha_3m) if ha_3m else "NEUTRAL"
        signal.ha_6h_trend = get_trend(ha_6h, lookback=3)

        # 6H ratio: color distribution of last 3 6H candles (including forming)
        _last3_6h = ha_6h[-3:] if len(ha_6h) >= 3 else ha_6h
        signal.ha_6h_green_count = sum(1 for c in _last3_6h if c["color"] == "GREEN")
        signal.ha_6h_red_count   = len(_last3_6h) - signal.ha_6h_green_count

        # 1H consecutive streak
        if ha_1h:
            _streak_color = ha_1h[-1]["color"]
            _streak = 0
            for _c in reversed(ha_1h):
                if _c["color"] == _streak_color:
                    _streak += 1
                else:
                    break
            signal.ha_1h_consecutive = _streak

        # ─── Step 2: Direction from 6h forming candle ──────────────────────
        if signal.ha_6h_color == "GREEN":
            candidate_direction = "LONG"
        elif signal.ha_6h_color == "RED":
            candidate_direction = "SHORT"
        else:
            signal.block_reasons.append("6h HA neutral — no directional bias")
            signal.strength = "BLOCKED"
            signal.velocity_data = compute_velocity(candles_1h)
            signal.time_context = get_time_context()
            signal.macro_context = self.macro_calendar.get_macro_context()
            return signal

        # ─── Step 3: 1h and 3m must agree with 6h direction ────────────────
        ha_1h_ok = signal.ha_1h_color == signal.ha_6h_color
        ha_3m_ok = (not ha_3m) or (signal.ha_3m_color == signal.ha_6h_color)

        if not ha_1h_ok or not ha_3m_ok:
            signal.block_reasons.append(
                f"HA not aligned: 3m={signal.ha_3m_color}, 1h={signal.ha_1h_color}, 6h={signal.ha_6h_color}"
            )
            signal.direction = candidate_direction
            signal.strength = "BLOCKED"
            signal.velocity_data = compute_velocity(candles_1h)
            signal.time_context = get_time_context()
            signal.macro_context = self.macro_calendar.get_macro_context()
            return signal

        # ─── Step 4: Zone analysis ─────────────────────────────────────────
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
        liq_task     = asyncio.create_task(self.liquidation_monitor.fetch_all(current_price))
        spot_task    = asyncio.create_task(self.order_flow_monitor.fetch_all(current_price))
        hyblock_task = asyncio.create_task(self.hyblock_monitor.fetch_all(current_price))

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
            hyblock_task.cancel()
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
            hyblock_task.cancel()
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

        # ─── Step 8.7: Hyblock Capital signals ───────────────────────────
        hyblock_score_delta = 0.0
        hyblock_block = False
        try:
            hyblock_data = await hyblock_task
            hyblock_score_delta, hyblock_desc, hyblock_warnings, hyblock_block = \
                self.hyblock_monitor.get_trade_context(candidate_direction, hyblock_data)
            signal.hyblock_analysis = {
                **{k: v for k, v in hyblock_data.items() if k != "raw"},
                "trade_context": hyblock_desc,
                "score_delta": hyblock_score_delta,
            }
            for w in hyblock_warnings:
                signal.warnings.append(f"Hyblock: {w}")
            if hyblock_desc and hyblock_desc != "No strong Hyblock signals":
                signal.warnings.append(f"Hyblock: {hyblock_desc}")
        except Exception as e:
            logger.warning(f"Hyblock signal failed: {e}")
            hyblock_data = {}
            hyblock_block = False

        if hyblock_block:
            signal.block_reasons.append("Hyblock: CRITICAL cascade risk — entry blocked")
            signal.direction = candidate_direction
            signal.strength = "BLOCKED"
            return signal

        # ─── Step 8.8: MII primary trigger gate ──────────────────────────
        # Market Imbalance Index is our primary entry trigger.
        # Must confirm direction: > +threshold for LONG, < -threshold for SHORT.
        mii = hyblock_data.get("market_imbalance_index", 0.0)
        _mii_threshold = settings.mii_entry_threshold
        if candidate_direction == "LONG" and mii < _mii_threshold:
            signal.block_reasons.append(
                f"MII {mii:+.2f} not bullish enough for LONG — need >{_mii_threshold:.2f}"
            )
            signal.direction = candidate_direction
            signal.strength = "BLOCKED"
            return signal
        elif candidate_direction == "SHORT" and mii > -_mii_threshold:
            signal.block_reasons.append(
                f"MII {mii:+.2f} not bearish enough for SHORT — need <-{_mii_threshold:.2f}"
            )
            signal.direction = candidate_direction
            signal.strength = "BLOCKED"
            return signal

        # ─── Step 8.9: Liq cluster gate ──────────────────────────────────
        # Must have a liquidation cluster in trade direction:
        #   - within liq_cluster_max_pct % of current price
        #   - at least min_liq_cluster_btc BTC (filters noise)
        # The nearest cluster price becomes the take-profit target.
        _min_btc = settings.min_liq_cluster_btc
        _max_pct  = settings.liq_cluster_max_pct
        liq_clusters = hyblock_data.get("liq_clusters", {})

        if candidate_direction == "LONG":
            _above_size  = liq_clusters.get("above_size", 0.0) or 0.0
            _above_pct   = liq_clusters.get("above_pct")
            _above_price = liq_clusters.get("above_price")
            _ok = (
                _above_pct is not None
                and _above_pct <= _max_pct
                and _above_size >= _min_btc
            )
            if not _ok:
                signal.block_reasons.append(
                    f"Liq cluster gate: {_above_size:.0f} BTC at "
                    f"{_above_pct}% above — need >={_min_btc:.0f} BTC within {_max_pct:.1f}%"
                )
                signal.direction = candidate_direction
                signal.strength = "BLOCKED"
                return signal
            signal.liq_target_price = _above_price

        elif candidate_direction == "SHORT":
            _below_size  = liq_clusters.get("below_size", 0.0) or 0.0
            _below_pct   = liq_clusters.get("below_pct")
            _below_price = liq_clusters.get("below_price")
            _ok = (
                _below_pct is not None
                and _below_pct <= _max_pct
                and _below_size >= _min_btc
            )
            if not _ok:
                signal.block_reasons.append(
                    f"Liq cluster gate: {_below_size:.0f} BTC at "
                    f"{_below_pct}% below — need >={_min_btc:.0f} BTC within {_max_pct:.1f}%"
                )
                signal.direction = candidate_direction
                signal.strength = "BLOCKED"
                return signal
            signal.liq_target_price = _below_price

        # ─── Step 9: Calculate confidence score ───────────────────────────
        score = 50.0  # Base
        _breakdown: list[str] = ["base=50"]

        # HA trend bonus — direction-aware: trend must match the trade direction
        _trend = signal.ha_6h_trend
        _bullish_trend = _trend in ("STRONG_BULLISH", "BULLISH")
        _bearish_trend = _trend in ("STRONG_BEARISH", "BEARISH")
        _strong = _trend in ("STRONG_BULLISH", "STRONG_BEARISH")
        if candidate_direction == "LONG":
            if _bullish_trend:
                ha_pts = 20.0 if _strong else 10.0
            elif _bearish_trend:
                ha_pts = -10.0 if _strong else -5.0  # contra-trend scalp — reduce confidence
            else:
                ha_pts = 0.0
        else:  # SHORT
            if _bearish_trend:
                ha_pts = 20.0 if _strong else 10.0
            elif _bullish_trend:
                ha_pts = -10.0 if _strong else -5.0
            else:
                ha_pts = 0.0
        score += ha_pts
        _breakdown.append(f"HA_trend={_trend}({ha_pts:+.0f})")

        # Zone position bonus
        if (candidate_direction == "SHORT" and zone_position == "TOP"):
            score += 10.0
            _breakdown.append("zone=TOP(+10)")
        elif (candidate_direction == "LONG" and zone_position == "BOTTOM"):
            score += 10.0
            _breakdown.append("zone=BOTTOM(+10)")
        else:
            _breakdown.append(f"zone={zone_position}(+0)")

        # Funding rate score — confirming means crowd is on the wrong side (squeeze setup)
        funding_sentiment = funding_analysis.get("overall_sentiment", "NEUTRAL")
        funding_confirms = (
            (candidate_direction == "LONG"  and funding_sentiment == "BULLISH_CONTRARIAN") or
            (candidate_direction == "SHORT" and funding_sentiment == "BEARISH_CONTRARIAN")
        )
        if funding_confirms:
            score += 10.0
            _breakdown.append(f"funding={funding_sentiment}(+10)")
        elif funding_sentiment == "NEUTRAL":
            score += 5.0
            _breakdown.append("funding=NEUTRAL(+5)")
        else:
            _breakdown.append(f"funding={funding_sentiment}(+0)")

        # Macro bonus
        macro_mod = macro_context.get("position_size_modifier", 1.0)
        macro_pts = (macro_mod - 0.5) * 20.0
        score += macro_pts
        _breakdown.append(f"macro=mod{macro_mod:.2f}({macro_pts:+.0f})")

        # Time window bonus (active session = better)
        if time_context.get("risk_level") == "LOW":
            score += 5.0
            _breakdown.append("time=LOW(+5)")
        else:
            _breakdown.append(f"time={time_context.get('risk_level','?')}(+0)")

        # Liquidation positioning bonus/penalty
        score += liq_score_delta
        _breakdown.append(f"liq({liq_score_delta:+.1f})")

        # Spot order flow bonus/penalty
        score += spot_score_delta
        _breakdown.append(f"spot({spot_score_delta:+.1f})")

        # Hyblock signals bonus/penalty
        score += hyblock_score_delta
        _breakdown.append(f"hyblock({hyblock_score_delta:+.1f})")

        score = min(100.0, max(0.0, score))
        signal.confidence_score = score
        logger.info(f"Score breakdown [{candidate_direction}]: {' | '.join(_breakdown)} → raw={score:.1f}%")

        # ─── Step 10: Determine strength ─────────────────────────────────
        if score >= 70.0 and signal.ha_6h_trend in ("STRONG_BULLISH", "STRONG_BEARISH", "BULLISH", "BEARISH"):
            signal.strength = "STRONG"
        else:
            signal.strength = "WEAK"

        # ─── Step 10.5: Minimum confidence gate ──────────────────────────
        if score < 75.0:
            signal.block_reasons.append(
                f"Confidence {score:.0f}% below 75% threshold — no trade"
            )
            signal.direction = candidate_direction
            signal.strength = "BLOCKED"
            return signal

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
        _liq_target_str = (
            f"${signal.liq_target_price:,.0f}" if signal.liq_target_price else "none"
        )
        signal.entry_reason = (
            f"{candidate_direction} | MII={mii:+.2f} | "
            f"HA: 3m={signal.ha_3m_color} 1h={signal.ha_1h_color} 6h={signal.ha_6h_color} | "
            f"LiqTarget={_liq_target_str} | Zone={zone_key} ({zone_position}) | "
            f"Confidence={score:.0f}% | Funding={funding_analysis.get('average_rate', 0)*100:.3f}% | "
            f"Size={signal.position_size_modifier:.2f}x"
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

        # Gate on peak having reached TP1 — not on current PnL.
        # This ensures the trail fires even if price has crashed back below 19%.
        if peak_profit_pct >= settings.tp1_pct * 100:
            trail_pct = (
                settings.trailing_after_tp1_peak_high_pct
                if peak_profit_pct >= settings.trailing_peak_threshold_pct
                else settings.trailing_after_tp1_peak_low_pct
            )
            drawdown_from_peak = peak_profit_pct - pnl_pct
            if drawdown_from_peak >= trail_pct:
                return True, (
                    f"Trailing stop: peak={peak_profit_pct:.1f}%, "
                    f"current={pnl_pct:.1f}%, "
                    f"drawdown={drawdown_from_peak:.1f}% >= trail={trail_pct}%"
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
        # Gate on peak having reached TP1 — not on current PnL.
        # This ensures the trail fires even if price has crashed back below 19%.
        if peak_profit_pct >= settings.tp1_pct * 100:
            trail_pct = (
                settings.trailing_after_tp1_peak_high_pct
                if peak_profit_pct >= settings.trailing_peak_threshold_pct
                else settings.trailing_after_tp1_peak_low_pct
            )
            drawdown_from_peak = peak_profit_pct - current_pnl_pct
            if drawdown_from_peak >= trail_pct:
                return True, (
                    f"Trailing stop: peak={peak_profit_pct:.1f}%, "
                    f"current={current_pnl_pct:.1f}%, "
                    f"drawdown={drawdown_from_peak:.1f}% >= trail={trail_pct}%"
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

    def check_3m_exit(
        self,
        position_side: str,
        peak_profit_pct: float,
        ha_3m: List[Dict],
    ) -> Tuple[bool, str]:
        """
        3M HA exit — supplemental to the trailing stop, only fires after TP1.

        After TP1 (peak >= 20%): 2 consecutive opposing 3M candles → exit
        After TP2 (peak >= 30%): 1 opposing 3M candle → exit
        """
        if not ha_3m:
            return False, "No 3M data"

        tp1_threshold = settings.tp1_pct * 100   # 20.0%
        tp2_threshold = settings.tp2_pct * 100   # 30.0%

        if peak_profit_pct < tp1_threshold:
            return False, "Pre-TP1 — 3M exit not active"

        opp_count = count_consecutive_opposite(ha_3m, position_side)

        if peak_profit_pct >= tp2_threshold:
            if opp_count >= 1:
                return True, (
                    f"3M HA exit after TP2 (peak={peak_profit_pct:.1f}%): "
                    f"{opp_count} opposing 3M candle(s)"
                )
        else:
            if opp_count >= 2:
                return True, (
                    f"3M HA exit after TP1 (peak={peak_profit_pct:.1f}%): "
                    f"{opp_count} consecutive opposing 3M candles"
                )

        return False, "Hold — 3M HA exit not triggered"
