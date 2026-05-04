"""
Master Signal Engine
Aggregates all strategy components to produce a final trade signal.
"""
import asyncio
from datetime import datetime
from typing import Dict, Optional, Tuple, List
from loguru import logger

from backend.strategy.heikin_ashi import (
    compute_heikin_ashi, get_trend, get_candle_color, drop_in_progress
)
from backend.strategy.zones import ZoneTracker, get_zone_key, get_zone_position
from backend.strategy.velocity import check_velocity_filter, compute_velocity
from backend.strategy.funding_rate import FundingRateMonitor
from backend.strategy.macro_calendar import MacroCalendar
from backend.strategy.liquidation_monitor import LiquidationMonitor
from backend.strategy.order_flow import SpotOrderFlowMonitor
from backend.strategy.hyblock import HyblockMonitor
from backend.strategy.liquidation_stream import LiquidationStreamMonitor
from backend.strategy.orderbook_stream import OrderBookMonitor, LARGE_WALL_BTC, MIN_WALL_BTC
from backend.strategy.trade_flow import TradeFlowMonitor
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
        self.liquidation_analysis: Dict = {}
        self.spot_flow_analysis: Dict = {}
        self.hyblock_analysis: Dict = {}
        self.current_price: float = 0.0

        # 3M HA
        self.ha_3m_color: str = "NEUTRAL"

        # Liq cluster take-profit target (set at entry, None if no cluster gate)
        self.liq_target_price: Optional[float] = None

        # Precision scalping signals
        self.cascade_direction: Optional[str] = None
        self.liq_level_long_pct: Optional[float] = None   # LONG cluster % below price
        self.liq_level_short_pct: Optional[float] = None  # SHORT cluster % above price
        self.liq_level_long_size: float = 0.0
        self.liq_level_short_size: float = 0.0
        self.volume_ratio: float = 0.0
        self.buy_sell_count_ratio: float = 0.0

        # Dashboard counters
        self.ha_6h_green_count: int = 0
        self.ha_6h_red_count: int = 0
        self.ha_1h_consecutive: int = 0

        # WarriorAI-aligned HA scoring components
        self.ha_6h_body_pct: float = 0.0         # 6h HA body as % of range (0-100)
        self.ha_prev_6h_color: str = "NEUTRAL"   # previous completed 6h HA color
        self.ha_1h_aligned_count: int = 0        # count of last 4 1h HA candles matching direction
        self.ha_6h_high: float = 0.0             # completed 6h HA candle high
        self.ha_6h_low: float = 0.0              # completed 6h HA candle low
        self.wick_fade_mode: bool = False        # True when counter-trend wick fade at 24h extreme

        # Gap 2: 3m price velocity toward liq target
        self.velocity_toward_target: Optional[bool] = None  # True=toward, False=away, None=N/A
        self.velocity_pct_3m: float = 0.0                   # abs magnitude of 3m price move %

        # Gap 3: 3m HA momentum burst components
        self.ha_3m_aligned_count: int = 0    # out of last 3 3m candles, how many match direction
        self.ha_3m_expanding: bool = False   # True if last body > first body in last 3 candles

        # Layer 2: live order book state snapshot at signal time
        self.ob_state: Dict = {}

        # Layer 3: market regime
        self.regime:           str   = "UNKNOWN"   # TRENDING / RANGING / HIGH_VOL / NEUTRAL
        self.regime_er:        float = 0.5         # Efficiency Ratio 0-1
        self.regime_vol_ratio: float = 1.0         # recent ATR / baseline ATR

        # Layer 5: aggTrade taker flow snapshot
        self.trade_flow_state: Dict = {}

        # Walk-forward data collection
        self.block_stage: str = ""          # which gate blocked this signal
        self.mode: str = "HA_TREND"         # HA_TREND or WICK_FADE
        self.score_breakdown: List[str] = []  # full scoring breakdown
        self.dist_from_24h_high_pct: float = 0.0
        self.dist_from_24h_low_pct: float = 0.0
        self.live_liq_state: Dict = {}      # LiquidationStreamMonitor.get_live_state() snapshot
        self.funding_trajectory_data: Dict = {}  # get_trajectory() result

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
            "liquidation": self.liquidation_analysis,
            "spot_flow": self.spot_flow_analysis,
            "hyblock": self.hyblock_analysis,
            "current_price": self.current_price,
            "ha_3m_color": self.ha_3m_color,
            "liq_target_price": self.liq_target_price,
            "cascade_direction": self.cascade_direction,
            "liq_level_long_pct": self.liq_level_long_pct,
            "liq_level_short_pct": self.liq_level_short_pct,
            "liq_level_long_size": self.liq_level_long_size,
            "liq_level_short_size": self.liq_level_short_size,
            "volume_ratio": self.volume_ratio,
            "buy_sell_count_ratio": self.buy_sell_count_ratio,
            "ha_6h_green_count": self.ha_6h_green_count,
            "ha_6h_red_count": self.ha_6h_red_count,
            "ha_1h_consecutive": self.ha_1h_consecutive,
            "ha_6h_body_pct": round(self.ha_6h_body_pct, 1),
            "ha_prev_6h_color": self.ha_prev_6h_color,
            "ha_1h_aligned_count": self.ha_1h_aligned_count,
            "ha_6h_high": round(self.ha_6h_high, 1),
            "ha_6h_low": round(self.ha_6h_low, 1),
            "wick_fade_mode": self.wick_fade_mode,
            # Gap 2 — 3m velocity toward liq target
            "velocity_toward_target": self.velocity_toward_target,
            "velocity_pct_3m": round(self.velocity_pct_3m, 4),
            # Gap 3 — 3m momentum burst
            "ha_3m_aligned_count": self.ha_3m_aligned_count,
            "ha_3m_expanding": self.ha_3m_expanding,
            "regime":           self.regime,
            "regime_er":        round(self.regime_er, 3),
            "regime_vol_ratio": round(self.regime_vol_ratio, 3),
            "trade_flow":       self.trade_flow_state,
            "generated_at": self.generated_at.isoformat(),
            # Walk-forward data collection
            "block_stage": self.block_stage,
            "mode": self.mode,
            "score_breakdown": self.score_breakdown,
            "dist_from_24h_high_pct": round(self.dist_from_24h_high_pct, 4),
            "dist_from_24h_low_pct": round(self.dist_from_24h_low_pct, 4),
            "live_liq_state": self.live_liq_state,
            "funding_trajectory": self.funding_trajectory_data,
        }


class SignalEngine:
    """
    Runs all strategy filters and produces a consolidated trade signal.
    """

    def __init__(
        self,
        liq_stream_monitor: Optional["LiquidationStreamMonitor"] = None,
        ob_monitor: Optional["OrderBookMonitor"] = None,
        trade_flow_monitor: Optional["TradeFlowMonitor"] = None,
    ):
        self.zone_tracker = ZoneTracker(
            zone_size=settings.zone_size_usd,
            cooldown_minutes=settings.zone_cooldown_minutes,
        )
        self.funding_monitor = FundingRateMonitor()
        self.macro_calendar = MacroCalendar()
        self.liquidation_monitor = LiquidationMonitor()
        self.order_flow_monitor = SpotOrderFlowMonitor()
        self.hyblock_monitor = HyblockMonitor()
        self.liq_stream_monitor: Optional[LiquidationStreamMonitor] = liq_stream_monitor
        self.ob_monitor: Optional[OrderBookMonitor] = ob_monitor
        self.trade_flow_monitor: Optional[TradeFlowMonitor] = trade_flow_monitor
        self._prev_price: Optional[float] = None
        self._first_break_zones: Dict[str, Dict] = {}

    @staticmethod
    def _compute_regime(candles_1h: list) -> dict:
        """
        Efficiency Ratio (ER) + ATR volatility regime from 1h candles.

        ER = |P_end - P_start| / Σ|ΔP_i| over ER_LOOKBACK candles.
          ER → 1.0: price moved in a straight line → momentum is sticky → HA_TREND reliable
          ER → 0.0: price zigzagged with no net progress → mean-reversion rules → wick fades reliable

        Volatility ratio = mean(last 5 ATRs) / mean(last 24 ATRs).
        HIGH_VOL is checked before ER: a breakout has high ER AND high vol; we want to
        classify it as HIGH_VOL first to enforce smaller size regardless of direction.
        """
        ER_LOOKBACK  = 20   # 20h window — captures the current session's character
        VOL_SHORT    = 5    # recent 5-candle ATR (last ~5h)
        VOL_LOOKBACK = 24   # 24-candle baseline (one full day)

        candle_data = [
            (c.get("close", 0.0), c.get("high", 0.0), c.get("low", 0.0))
            for c in candles_1h
            if c.get("close", 0.0) > 0 and c.get("high", 0.0) > 0 and c.get("low", 0.0) > 0
        ]
        if len(candle_data) < ER_LOOKBACK + 1:
            return {"regime": "UNKNOWN", "er": 0.5, "vol_ratio": 1.0}

        closes = [x[0] for x in candle_data]
        highs  = [x[1] for x in candle_data]
        lows   = [x[2] for x in candle_data]

        # ── Efficiency Ratio ──────────────────────────────────────────────────
        er_window = closes[-(ER_LOOKBACK + 1):]
        net_move  = abs(er_window[-1] - er_window[0])
        path_len  = sum(abs(er_window[i] - er_window[i - 1]) for i in range(1, len(er_window)))
        er = net_move / path_len if path_len > 0 else 0.0

        # ── Volatility ratio (ATR proxy: high - low per candle) ───────────────
        if len(highs) >= VOL_LOOKBACK:
            atrs = [highs[i] - lows[i] for i in range(-VOL_LOOKBACK, 0)]
            recent_atr   = sum(atrs[-VOL_SHORT:]) / VOL_SHORT
            baseline_atr = sum(atrs) / len(atrs)
            vol_ratio = recent_atr / baseline_atr if baseline_atr > 0 else 1.0
        else:
            vol_ratio = 1.0

        # HIGH_VOL supersedes ER classification — a breakout candle spike is risky
        # for both modes regardless of whether it looks "trending" on ER alone.
        if vol_ratio > 1.8:
            regime = "HIGH_VOL"
        elif er > 0.35:
            regime = "TRENDING"
        elif er < 0.15:
            regime = "RANGING"
        else:
            regime = "NEUTRAL"

        return {"regime": regime, "er": round(er, 3), "vol_ratio": round(vol_ratio, 3)}

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
            signal.block_stage = "NO_DATA"
            return signal

        # ─── Step 1: Compute Heikin Ashi ──────────────────────────────────
        # 6H direction uses the last COMPLETED candle (WarriorAI model).
        # The in-progress 6h candle is included in the HA chain computation
        # for accuracy but excluded from direction/scoring reads.
        # 1H and 3M use the current forming candle (momentum is real-time).
        ha_1h = compute_heikin_ashi(candles_1h[-50:])
        ha_6h = compute_heikin_ashi(candles_6h[-30:])
        ha_3m = compute_heikin_ashi((candles_3m or [])[-20:])

        # Last completed 6h candle = ha_6h[-2] (ha_6h[-1] is still forming)
        _ha_6h_done = ha_6h[:-1] if len(ha_6h) >= 2 else ha_6h

        signal.ha_1h_color = get_candle_color(ha_1h)
        signal.ha_6h_color = get_candle_color(_ha_6h_done)   # completed candle
        signal.ha_3m_color = get_candle_color(ha_3m) if ha_3m else "NEUTRAL"
        signal.ha_6h_trend = get_trend(_ha_6h_done, lookback=3)

        # 6H ratio: color distribution of last 3 COMPLETED 6H candles
        _last3_6h = _ha_6h_done[-3:] if len(_ha_6h_done) >= 3 else _ha_6h_done
        signal.ha_6h_green_count = sum(1 for c in _last3_6h if c["color"] == "GREEN")
        signal.ha_6h_red_count   = len(_last3_6h) - signal.ha_6h_green_count

        # 6H HA body strength, trend confirm, and price levels — from completed candle
        if _ha_6h_done:
            _c6h = _ha_6h_done[-1]
            _6h_range = _c6h["ha_high"] - _c6h["ha_low"]
            signal.ha_6h_body_pct = (_c6h["body"] / _6h_range * 100.0) if _6h_range > 0 else 0.0
            signal.ha_prev_6h_color = _ha_6h_done[-2]["color"] if len(_ha_6h_done) >= 2 else "NEUTRAL"
            signal.ha_6h_high = _c6h["ha_high"]
            signal.ha_6h_low  = _c6h["ha_low"]

        # 1H consecutive streak (kept for display)
        if ha_1h:
            _streak_color = ha_1h[-1]["color"]
            _streak = 0
            for _c in reversed(ha_1h):
                if _c["color"] == _streak_color:
                    _streak += 1
                else:
                    break
            signal.ha_1h_consecutive = _streak

        # ─── Step 2: Preliminary direction from HA (for zone/velocity only) ───
        # HA is now context and scoring only — liquidation cascade levels (fetched
        # from Hyblock) will override this direction after async data arrives.
        _ha_colors = [signal.ha_6h_color, signal.ha_1h_color]
        if ha_3m:
            _ha_colors.append(signal.ha_3m_color)
        _green_votes = _ha_colors.count("GREEN")
        _red_votes   = _ha_colors.count("RED")

        if _green_votes > _red_votes:
            candidate_direction = "LONG"
        elif _red_votes > _green_votes:
            candidate_direction = "SHORT"
        else:
            # Split — default LONG; cascade direction will override after fetch
            candidate_direction = "LONG"

        # Saved for zone/velocity re-check if micro votes flip direction in Step 8.75
        _prelim_dir = candidate_direction

        # ── Market regime — computed once, used throughout scoring ───────────
        _regime_data     = SignalEngine._compute_regime(candles_1h)
        _regime          = _regime_data["regime"]
        _regime_modifier = 1.0   # may be reduced in HIGH_VOL; applied to final size

        logger.info(
            f"[{candidate_direction}] HA preliminary ({_green_votes}G/{_red_votes}R): "
            f"3m={signal.ha_3m_color} 1h={signal.ha_1h_color} 6h={signal.ha_6h_color} | "
            f"regime={_regime} ER={_regime_data['er']:.3f} vol={_regime_data['vol_ratio']:.2f}"
        )

        # Start Hyblock fetch early so it runs concurrently with sync filters below
        hyblock_task = asyncio.create_task(self.hyblock_monitor.fetch_all(current_price))

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
            signal.block_stage = "ZONE_COOLDOWN"
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
                    signal.block_stage = "DWARF"
                    return signal

        self._prev_price = current_price

        # ─── Step 5 (removed): Time filter disabled — liq cluster entry strategy
        #     fires during US session hours which were previously blocked.

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
            signal.block_stage = "VELOCITY"
            return signal

        # Kick off remaining I/O-bound tasks (hyblock_task already running since Step 2)
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

        # Funding informs score only — no hard blocks.
        # Warn when funding is mildly against direction (Scenario 1: "cautious, reduce size").
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
            signal.block_stage = "MACRO"
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
        hyblock_data: Dict = {}
        try:
            hyblock_data = await hyblock_task
            signal.hyblock_analysis = {
                **{k: v for k, v in hyblock_data.items() if k != "raw"},
            }
        except Exception as e:
            logger.warning(f"Hyblock fetch failed: {e}")

        # ─── Step 8.75: Direction from microstructure votes (HA = fallback only) ──
        # OBI + cascade + whale + taker flow drive direction.
        # HA votes are used only when microstructure is split/absent.
        mii = hyblock_data.get("market_imbalance_index", 0.0)
        _liq_levels = hyblock_data.get("liq_levels", {})
        cascade_dir = _liq_levels.get("cascade_direction")
        obi_dir     = hyblock_data.get("obi_slope_direction", "NEUTRAL")
        whale       = hyblock_data.get("whale_sentiment", "NEUTRAL")
        casc_risk   = hyblock_data.get("cascade_risk", "LOW")

        # Gather taker buy ratio for direction vote (live, non-blocking)
        _tbr_5m_dir = 0.5
        if self.trade_flow_monitor is not None:
            _tf_dir_tmp = self.trade_flow_monitor.get_live_state()
            _tbr_5m_dir = _tf_dir_tmp.get("taker_buy_ratio_5m", 0.5)

        # Live cascade direction vote
        _live_casc_dir_tmp = None
        if self.liq_stream_monitor is not None:
            _ls_dir_tmp = self.liq_stream_monitor.get_live_state()
            if _ls_dir_tmp.get("cascade_live"):
                _live_casc_dir_tmp = _ls_dir_tmp.get("cascade_direction")

        _micro_long = (
            (1 if obi_dir == "BULLISH" else 0) +
            (1 if cascade_dir == "LONG" else 0) +
            (1 if whale == "BULLISH" else 0) +
            (1 if _tbr_5m_dir > 0.60 else 0) +
            (2 if _live_casc_dir_tmp == "LONG" else 0)
        )
        _micro_short = (
            (1 if obi_dir == "BEARISH" else 0) +
            (1 if cascade_dir == "SHORT" else 0) +
            (1 if whale == "BEARISH" else 0) +
            (1 if _tbr_5m_dir < 0.40 else 0) +
            (2 if _live_casc_dir_tmp == "SHORT" else 0)
        )

        if _micro_long > _micro_short:
            candidate_direction = "LONG"
            logger.info(
                f"[LONG] Micro votes: {_micro_long}L/{_micro_short}S "
                f"(OBI={obi_dir} casc={cascade_dir} whale={whale} tbr={_tbr_5m_dir:.2f} live={_live_casc_dir_tmp})"
            )
        elif _micro_short > _micro_long:
            candidate_direction = "SHORT"
            logger.info(
                f"[SHORT] Micro votes: {_micro_short}S/{_micro_long}L "
                f"(OBI={obi_dir} casc={cascade_dir} whale={whale} tbr={_tbr_5m_dir:.2f} live={_live_casc_dir_tmp})"
            )
        else:
            # Micro tied — fall back to HA votes
            if _green_votes > _red_votes:
                candidate_direction = "LONG"
            elif _red_votes > _green_votes:
                candidate_direction = "SHORT"
            elif cascade_dir:
                candidate_direction = cascade_dir
            else:
                signal.block_reasons.append(
                    f"Micro tied ({_micro_long}L/{_micro_short}S) and HA tied "
                    f"({_green_votes}G/{_red_votes}R) — no direction"
                )
                signal.direction = candidate_direction
                signal.strength = "BLOCKED"
                signal.block_stage = "HYBLOCK_BLOCK"
                return signal
            logger.info(
                f"[{candidate_direction}] Micro tied ({_micro_long}L/{_micro_short}S) — "
                f"HA fallback ({_green_votes}G/{_red_votes}R | MII={mii:+.2f})"
            )

        # If micro votes flipped direction from HA preliminary, re-check zone cooldown
        # for the new direction (the Step 4 check only covered the preliminary direction).
        if candidate_direction != _prelim_dir:
            if self.zone_tracker.is_in_cooldown(zone_key, candidate_direction):
                remaining = self.zone_tracker.get_cooldown_remaining(zone_key, candidate_direction)
                signal.block_reasons.append(
                    f"Zone {zone_key} {candidate_direction} in cooldown — {remaining/60:.0f}min remaining"
                )
                signal.direction = candidate_direction
                signal.strength = "BLOCKED"
                signal.block_stage = "ZONE_COOLDOWN"
                return signal

        # Store precision signal state on the trade signal
        signal.cascade_direction = cascade_dir
        signal.liq_level_long_pct   = _liq_levels.get("long_cluster_pct")
        signal.liq_level_short_pct  = _liq_levels.get("short_cluster_pct")
        signal.liq_level_long_size  = _liq_levels.get("long_cluster_size", 0.0) or 0.0
        signal.liq_level_short_size = _liq_levels.get("short_cluster_size", 0.0) or 0.0
        signal.volume_ratio         = hyblock_data.get("volume_ratio", 0.0) or 0.0
        signal.buy_sell_count_ratio = hyblock_data.get("buy_sell_count_ratio", 0.0) or 0.0

        # ─── Step 8.76: Range extreme + counter-trend detection ───────────────
        # Records whether price is at a 24h extreme with microstructure opposing
        # the 6h HA trend. No longer a hard mode switch — just sets _wick_fade_mode
        # for DB tracking and scoring context. No ha_1h_color gate: microstructure
        # votes (Step 8.75) already determined direction; this is informational.
        _24h_candles = candles_1h[-24:] if len(candles_1h) >= 24 else candles_1h
        _24h_high = max((c.get("high", 0) for c in _24h_candles), default=0.0)
        _24h_low  = min((c.get("low", float("inf")) for c in _24h_candles), default=current_price)
        _dist_from_24h_high_pct = (_24h_high - current_price) / _24h_high * 100 if _24h_high > 0 else 999.0
        _dist_from_24h_low_pct  = (current_price - _24h_low)  / _24h_low  * 100 if _24h_low  > 0 else 999.0

        _range_prox = settings.range_extreme_proximity_pct

        _wick_fade_mode = (
            (
                candidate_direction == "SHORT"
                and signal.ha_6h_color == "GREEN"
                and _dist_from_24h_high_pct <= _range_prox
                and obi_dir == "BEARISH"
                and casc_risk in ("MEDIUM", "HIGH", "CRITICAL")
            ) or (
                candidate_direction == "LONG"
                and signal.ha_6h_color == "RED"
                and _dist_from_24h_low_pct <= _range_prox
                and obi_dir == "BULLISH"
                and casc_risk in ("MEDIUM", "HIGH", "CRITICAL")
            )
        )
        if _wick_fade_mode:
            _wf_dist = _dist_from_24h_high_pct if candidate_direction == "SHORT" else _dist_from_24h_low_pct
            _wf_ref  = _24h_high if candidate_direction == "SHORT" else _24h_low
            logger.info(
                f"[{candidate_direction}] COUNTER-TREND EXTREME — "
                f"{_wf_dist:.2f}% from 24h {'high' if candidate_direction == 'SHORT' else 'low'} "
                f"${_wf_ref:,.0f} | 6h={'GREEN' if candidate_direction == 'SHORT' else 'RED'} | "
                f"OBI={obi_dir} | Whale={whale} | CascRisk={casc_risk}"
            )

        signal.wick_fade_mode = _wick_fade_mode
        signal.mode = "WICK_FADE" if _wick_fade_mode else "HA_TREND"
        signal.dist_from_24h_high_pct = _dist_from_24h_high_pct
        signal.dist_from_24h_low_pct = _dist_from_24h_low_pct

        # If direction flipped from HA preliminary, re-validate velocity for the final direction.
        # Wick fades skip this: the big move IS the setup, not a reason to block the counter-trade.
        if candidate_direction != _prelim_dir and not _wick_fade_mode:
            _vel2_ok, _vel2_reason, _vel2_data = check_velocity_filter(
                candles_1h,
                candidate_direction,
                threshold_pct=settings.velocity_threshold_pct,
                window_hours=settings.velocity_window_hours,
            )
            if not _vel2_ok:
                signal.velocity_data = _vel2_data
                signal.block_reasons.append(_vel2_reason)
                signal.direction = candidate_direction
                signal.strength = "BLOCKED"
                signal.block_stage = "VELOCITY"
                return signal

        # Score hyblock signals now that direction is final
        try:
            hyblock_score_delta, hyblock_desc, hyblock_warnings, hyblock_block = \
                self.hyblock_monitor.get_trade_context(candidate_direction, hyblock_data)
            signal.hyblock_analysis.update({
                "trade_context": hyblock_desc,
                "score_delta": hyblock_score_delta,
            })
            for w in hyblock_warnings:
                signal.warnings.append(f"Hyblock: {w}")
            if hyblock_desc and hyblock_desc != "No strong Hyblock signals":
                signal.warnings.append(f"Hyblock: {hyblock_desc}")
        except Exception as e:
            logger.warning(f"Hyblock scoring failed: {e}")

        if hyblock_block:
            signal.block_reasons.append("Hyblock: CRITICAL cascade risk — entry blocked")
            signal.direction = candidate_direction
            signal.strength = "BLOCKED"
            signal.block_stage = "HYBLOCK_BLOCK"
            return signal

        # ─── Step 8.9: Fused liq cluster gate (skipped for wick fades) ─────────
        # OR gate: passes if EITHER source has a qualifying directional cluster.
        #   liq_clusters (liquidationHeatmap) — multi-exchange, zone-based
        #   liq_levels   (liquidationLevels)  — single-exchange, exact per-price
        # TP price: prefers exact level price (precision exit), falls back to
        # heatmap midpoint. Both confirming = highest conviction entry.
        # Wick fades bypass this: the 24h extreme + Hyblock OBI is the entry signal.
        if not _wick_fade_mode:
            _min_btc    = settings.min_liq_cluster_btc
            liq_clusters = hyblock_data.get("liq_clusters", {})

            if candidate_direction == "LONG":
                # Exact levels: SHORT cluster above price — sole hard gate
                _lvl_size  = _liq_levels.get("short_cluster_size", 0.0) or 0.0
                _lvl_pct   = _liq_levels.get("short_cluster_pct")
                _lvl_price = _liq_levels.get("short_cluster_price")
                _lvl_ok    = _lvl_pct is not None and _lvl_size >= _min_btc
                # Heatmap: context only — included in log, contributes to scorer
                _hm_size  = liq_clusters.get("above_size", 0.0) or 0.0
                _hm_pct   = liq_clusters.get("above_pct")

                if not _lvl_ok:
                    _detail = f"levels={_lvl_size:.0f}BTC@{_lvl_pct}% (need >={_min_btc:.0f} BTC)"
                    signal.block_reasons.append(f"Liq cluster gate: no qualifying SHORT cluster above — {_detail}")
                    logger.info(f"[LONG] BLOCKED — liq cluster gate: {_detail}")
                    signal.direction = candidate_direction
                    signal.strength = "BLOCKED"
                    signal.block_stage = "LIQ_GATE"
                    return signal

                signal.liq_target_price = _lvl_price
                _hm_note = f" heatmap={_hm_size:.0f}BTC@{_hm_pct}%" if _hm_pct is not None else ""
                _tp_note = f" TP=${signal.liq_target_price:,.0f}" if signal.liq_target_price else ""
                logger.info(f"[LONG] Liq gate passed: levels={_lvl_size:.0f}BTC@{_lvl_pct}%{_hm_note}{_tp_note}")

            elif candidate_direction == "SHORT":
                # Exact levels: LONG cluster below price — sole hard gate
                _lvl_size  = _liq_levels.get("long_cluster_size", 0.0) or 0.0
                _lvl_pct   = _liq_levels.get("long_cluster_pct")
                _lvl_price = _liq_levels.get("long_cluster_price")
                _lvl_ok    = _lvl_pct is not None and _lvl_size >= _min_btc
                # Heatmap: context only — included in log, contributes to scorer
                _hm_size  = liq_clusters.get("below_size", 0.0) or 0.0
                _hm_pct   = liq_clusters.get("below_pct")

                if not _lvl_ok:
                    _detail = f"levels={_lvl_size:.0f}BTC@{_lvl_pct}% (need >={_min_btc:.0f} BTC)"
                    signal.block_reasons.append(f"Liq cluster gate: no qualifying LONG cluster below — {_detail}")
                    logger.info(f"[SHORT] BLOCKED — liq cluster gate: {_detail}")
                    signal.direction = candidate_direction
                    signal.strength = "BLOCKED"
                    signal.block_stage = "LIQ_GATE"
                    return signal

                signal.liq_target_price = _lvl_price
                _hm_note = f" heatmap={_hm_size:.0f}BTC@{_hm_pct}%" if _hm_pct is not None else ""
                _tp_note = f" TP=${signal.liq_target_price:,.0f}" if signal.liq_target_price else ""
                logger.info(f"[SHORT] Liq gate passed: levels={_lvl_size:.0f}BTC@{_lvl_pct}%{_hm_note}{_tp_note}")

        # ─── Step 8.95: 6h HA price level gate (skipped for wick fades) ─────
        # WarriorAI enters only when price has returned to the completed 6h HA level
        # (supply/demand zone). SHORT entries require price near the 6h HA high;
        # LONG entries require price near the 6h HA low.
        # Wick fades bypass this: their entry trigger is the 24h range extreme, not the 6h level.
        if not _wick_fade_mode and signal.ha_6h_high > 0 and signal.ha_6h_low > 0:
            _prox_pct = settings.ha_6h_level_proximity_pct
            if candidate_direction == "SHORT":
                _dist_from_high_pct = (signal.ha_6h_high - current_price) / signal.ha_6h_high * 100
                if _dist_from_high_pct > _prox_pct:
                    signal.block_reasons.append(
                        f"6h level gate: ${current_price:,.0f} is {_dist_from_high_pct:.1f}% "
                        f"below 6h HA high ${signal.ha_6h_high:,.0f} — need within {_prox_pct:.1f}%"
                    )
                    logger.info(
                        f"[SHORT] BLOCKED — 6h level gate: {_dist_from_high_pct:.1f}% below "
                        f"6h high ${signal.ha_6h_high:,.0f} (need <={_prox_pct:.1f}%)"
                    )
                    signal.direction = candidate_direction
                    signal.strength = "BLOCKED"
                    signal.block_stage = "HA_LEVEL_GATE"
                    return signal
            elif candidate_direction == "LONG":
                _dist_from_low_pct = (current_price - signal.ha_6h_low) / signal.ha_6h_low * 100
                if _dist_from_low_pct > _prox_pct:
                    signal.block_reasons.append(
                        f"6h level gate: ${current_price:,.0f} is {_dist_from_low_pct:.1f}% "
                        f"above 6h HA low ${signal.ha_6h_low:,.0f} — need within {_prox_pct:.1f}%"
                    )
                    logger.info(
                        f"[LONG] BLOCKED — 6h level gate: {_dist_from_low_pct:.1f}% above "
                        f"6h low ${signal.ha_6h_low:,.0f} (need <={_prox_pct:.1f}%)"
                    )
                    signal.direction = candidate_direction
                    signal.strength = "BLOCKED"
                    signal.block_stage = "HA_LEVEL_GATE"
                    return signal

        # ─── Step 9: Calculate confidence score ───────────────────────────
        # Architecture: microstructure-first.
        #   Primary:   hyblock_score_delta (OBI, MII, whale, CVD, cascade, retail,
        #              liq clusters, OI, PDH/PDL — comprehensive real-time data)
        #              + live_cascade, taker_flow, OB walls, liq_prox, funding, macro
        #   Secondary: TREND CONTEXT composite (HA candles) — max +25, floor -20.
        #              Cannot drive a trade alone; confirms/attenuates primary signals.
        # Threshold: 80
        score = 0.0
        _breakdown: list[str] = []

        # Whether this trade is counter-trend relative to the 6h HA direction.
        # Used by OB, taker-flow, and regime scoring in place of _wick_fade_mode.
        _counter_trend = (
            (candidate_direction == "SHORT" and signal.ha_6h_color == "GREEN") or
            (candidate_direction == "LONG"  and signal.ha_6h_color == "RED")
        )

        # ── TREND CONTEXT composite (max +25, floor -20) ──────────────────────
        # All HA/candle data collapsed into one capped composite score.
        _tc = 0.0

        _6h_aligned = (
            (candidate_direction == "LONG"  and signal.ha_6h_color == "GREEN") or
            (candidate_direction == "SHORT" and signal.ha_6h_color == "RED")
        )
        _prev_matches = (
            signal.ha_prev_6h_color == signal.ha_6h_color
            and signal.ha_6h_color != "NEUTRAL"
        )
        if _6h_aligned:
            _tc += 8.0
            if signal.ha_6h_body_pct >= 20.0:
                _tc += 5.0
            if _prev_matches:
                _tc += 3.0
        else:
            _tc -= 8.0
            if signal.ha_6h_body_pct >= 20.0:
                _tc -= 3.0
        _breakdown.append(
            f"tc_6h={'align' if _6h_aligned else 'opp'}/{signal.ha_6h_body_pct:.0f}%/{'conf' if _prev_matches else 'no'}"
        )

        _1h_target = "GREEN" if candidate_direction == "LONG" else "RED"
        _last4_1h  = ha_1h[-4:] if len(ha_1h) >= 4 else ha_1h
        _aligned_1h = sum(1 for c in _last4_1h if c["color"] == _1h_target)
        signal.ha_1h_aligned_count = _aligned_1h
        _n_1h = len(_last4_1h)
        if _aligned_1h == _n_1h:
            _tc += 8.0
        elif _aligned_1h >= _n_1h - 1:
            _tc += 4.0
        elif _aligned_1h <= 1 and _n_1h >= 3:
            _tc -= 8.0
        elif _aligned_1h == 0:
            _tc -= 4.0
        _breakdown.append(f"tc_1h={_aligned_1h}/{_n_1h}")

        if ha_3m:
            _3m_align = (
                (candidate_direction == "LONG"  and signal.ha_3m_color == "GREEN") or
                (candidate_direction == "SHORT" and signal.ha_3m_color == "RED")
            )
            _3m_opp = (
                (candidate_direction == "LONG"  and signal.ha_3m_color == "RED") or
                (candidate_direction == "SHORT" and signal.ha_3m_color == "GREEN")
            )
            if _3m_align:
                _tc += 4.0
            elif _3m_opp:
                _tc -= 3.0

            # 3m burst entry timing (small bonus inside composite)
            _last3_3m  = ha_3m[-3:] if len(ha_3m) >= 3 else ha_3m
            _3m_color_tgt = "GREEN" if candidate_direction == "LONG" else "RED"
            _3m_aligned_cnt = sum(1 for c in _last3_3m if c["color"] == _3m_color_tgt)
            _3m_bodies = [c.get("body", 0.0) for c in _last3_3m]
            _3m_expanding = len(_3m_bodies) >= 2 and _3m_bodies[-1] > _3m_bodies[0]
            signal.ha_3m_aligned_count = _3m_aligned_cnt
            signal.ha_3m_expanding     = _3m_expanding
            if _3m_aligned_cnt == len(_last3_3m) and _3m_expanding:
                _tc += 3.0
            elif _3m_aligned_cnt == len(_last3_3m):
                _tc += 1.0
            _breakdown.append(
                f"tc_3m={'align' if _3m_align else ('opp' if _3m_opp else 'neut')}/"
                f"{_3m_aligned_cnt}/{len(_last3_3m)}/{'exp' if _3m_expanding else 'flat'}"
            )

        _tc = min(25.0, max(-20.0, _tc))
        score += _tc
        _breakdown.append(f"trend_ctx={_tc:+.0f}")

        # ── Liq cascade level proximity + size bonus ──────────────────────────
        # Small precision bonus — already a hard gate above, so just a tiebreaker here.
        if candidate_direction == "LONG":
            _casc_pct  = signal.liq_level_short_pct  # SHORT cluster above → LONG
            _casc_size = signal.liq_level_short_size
        else:
            _casc_pct  = signal.liq_level_long_pct   # LONG cluster below → SHORT
            _casc_size = signal.liq_level_long_size

        if _casc_pct is not None:
            # Tight proximity scoring for scalp strategy — distant clusters irrelevant
            # LONG: SHORT cluster above = short squeeze magnet; SHORT: LONG below = cascade target
            # Closer = more imminent trigger = much higher urgency
            if _casc_pct <= 0.3:
                _prox_pts = 8.0
            elif _casc_pct <= 0.5:
                _prox_pts = 6.0
            elif _casc_pct <= 1.0:
                _prox_pts = 3.0
            elif _casc_pct <= 1.5:
                _prox_pts = 1.0
            else:
                _prox_pts = 0.0  # >1.5% away: gate may pass but cluster offers no urgency
            if _casc_size > 3000:
                _prox_pts += 3.0
            elif _casc_size > 1000:
                _prox_pts += 2.0
            elif _casc_size > 300:
                _prox_pts += 1.0
            score += _prox_pts
            _breakdown.append(f"cascade={_casc_pct:.1f}%/{_casc_size:.0f}BTC({_prox_pts:+.0f})")
        else:
            _breakdown.append("cascade=none(+0)")

        # ── Cascade direction alignment ────────────────────────────────────────
        if cascade_dir:
            if cascade_dir == candidate_direction:
                score += 7.0
                _breakdown.append(f"casc_align=YES(+7)")
            else:
                score -= 6.0
                _breakdown.append(f"casc_align=NO(-6)")

        # ── 24h range extreme bonus ────────────────────────────────────────────
        # Bonus when direction + OBI agree at the 24h high/low extreme.
        # Counter-trend trades benefit most; trend entries at the extreme also score.
        if candidate_direction == "SHORT" and _dist_from_24h_high_pct <= _range_prox and obi_dir == "BEARISH":
            _ext_pts = 15.0 if _dist_from_24h_high_pct <= 0.3 else (10.0 if _dist_from_24h_high_pct <= 0.7 else 7.0)
            score += _ext_pts
            _breakdown.append(f"24h_top={_dist_from_24h_high_pct:.2f}%(+{_ext_pts:.0f})")
        elif candidate_direction == "LONG" and _dist_from_24h_low_pct <= _range_prox and obi_dir == "BULLISH":
            _ext_pts = 15.0 if _dist_from_24h_low_pct <= 0.3 else (10.0 if _dist_from_24h_low_pct <= 0.7 else 7.0)
            score += _ext_pts
            _breakdown.append(f"24h_bottom={_dist_from_24h_low_pct:.2f}%(+{_ext_pts:.0f})")
        elif candidate_direction == "LONG" and _dist_from_24h_high_pct <= _range_prox and obi_dir == "BEARISH":
            score -= 10.0
            _breakdown.append(f"24h_top_vs_long(-10)")
        elif candidate_direction == "SHORT" and _dist_from_24h_low_pct <= _range_prox and obi_dir == "BULLISH":
            score -= 10.0
            _breakdown.append(f"24h_bottom_vs_short(-10)")

        # ── 3m price velocity toward liq target ──────────────────────────────
        if candles_3m and len(candles_3m) >= 3 and signal.liq_target_price:
            _recent_3m = candles_3m[-5:]
            _closes_3m = [c.get("close", 0.0) for c in _recent_3m if c.get("close", 0.0) > 0]
            if len(_closes_3m) >= 2:
                _price_delta_3m = _closes_3m[-1] - _closes_3m[0]
                _toward_target = (
                    _price_delta_3m > 0 if signal.liq_target_price > current_price
                    else _price_delta_3m < 0
                )
                _vel_pct_3m = abs(_price_delta_3m) / current_price * 100
                signal.velocity_toward_target = _toward_target
                signal.velocity_pct_3m = _vel_pct_3m
                if _toward_target:
                    _vel_pts = 5.0 if _vel_pct_3m >= 0.1 else 2.0
                    score += _vel_pts
                    _breakdown.append(f"vel=toward({_vel_pts:+.0f})")
                else:
                    _vel_pts = -5.0 if _vel_pct_3m >= 0.1 else -2.0
                    score += _vel_pts
                    _breakdown.append(f"vel=away({_vel_pts:+.0f})")
            else:
                _breakdown.append("vel=n/a(+0)")

        # ── Zone position bonus ───────────────────────────────────────────────
        if (candidate_direction == "SHORT" and zone_position == "TOP"):
            score += 5.0
            _breakdown.append("zone=TOP(+5)")
        elif (candidate_direction == "LONG" and zone_position == "BOTTOM"):
            score += 5.0
            _breakdown.append("zone=BOTTOM(+5)")
        else:
            _breakdown.append(f"zone={zone_position}(+0)")

        # ── Round number zone penalty ─────────────────────────────────────────
        # $5K psychological levels ($70K, $75K, $80K, $85K, $90K, $95K, $100K)
        _round_levels = [65000, 70000, 75000, 80000, 85000, 90000, 95000, 100000]
        _nearest_round = min(_round_levels, key=lambda r: abs(current_price - r))
        _round_dist_pct = abs(current_price - _nearest_round) / current_price * 100
        if _round_dist_pct < 0.5:
            score -= 8.0
            _breakdown.append(f"round_zone=${_nearest_round/1000:.0f}K@{_round_dist_pct:.2f}%(-8)")
        else:
            _breakdown.append(f"round_zone=clear(+0)")

        # ── Funding rate ──────────────────────────────────────────────────────
        funding_sentiment = funding_analysis.get("overall_sentiment", "NEUTRAL")
        funding_confirms = (
            (candidate_direction == "LONG"  and funding_sentiment == "BULLISH_CONTRARIAN") or
            (candidate_direction == "SHORT" and funding_sentiment == "BEARISH_CONTRARIAN")
        )
        if funding_confirms:
            score += 5.0
            _breakdown.append(f"funding={funding_sentiment}(+5)")
        elif funding_sentiment == "NEUTRAL":
            score += 2.0
            _breakdown.append("funding=NEUTRAL(+2)")
        else:
            _breakdown.append(f"funding={funding_sentiment}(+0)")

        # ── Funding rate trajectory ───────────────────────────────────────────
        # Amplifies or attenuates the existing funding signal — never standalone.
        # RISING = funding moving positive = longs getting more overcrowded.
        # FALLING = funding moving negative = shorts getting more overcrowded.
        _traj = self.funding_monitor.get_trajectory()
        signal.funding_trajectory_data = _traj
        _traj_dir = _traj["trajectory"]
        _traj_pts = 0.0
        if _traj_dir != "FLAT":
            if candidate_direction == "LONG":
                if _traj_dir == "FALLING" and _fs == "BULLISH_CONTRARIAN":
                    _traj_pts = 4.0   # squeeze pressure building (shorts more crowded)
                elif _traj_dir == "RISING" and _fs == "BEARISH_CONTRARIAN":
                    _traj_pts = -3.0  # long crowding intensifying
                elif _traj_dir == "FALLING" and _fs == "BEARISH_CONTRARIAN":
                    _traj_pts = 2.0   # long headwind easing
                elif _traj_dir == "RISING" and _fs == "BULLISH_CONTRARIAN":
                    _traj_pts = -2.0  # short tailwind easing
            else:  # SHORT
                if _traj_dir == "RISING" and _fs == "BEARISH_CONTRARIAN":
                    _traj_pts = 4.0   # squeeze pressure building (longs more crowded)
                elif _traj_dir == "FALLING" and _fs == "BULLISH_CONTRARIAN":
                    _traj_pts = -3.0  # short crowding intensifying
                elif _traj_dir == "RISING" and _fs == "BULLISH_CONTRARIAN":
                    _traj_pts = 2.0   # short headwind easing
                elif _traj_dir == "FALLING" and _fs == "BEARISH_CONTRARIAN":
                    _traj_pts = -2.0  # long tailwind easing
        if _traj_pts != 0.0:
            score += _traj_pts
            _breakdown.append(f"funding_traj={_traj_dir}({_traj_pts:+.0f})")

        # ── Macro ─────────────────────────────────────────────────────────────
        macro_mod = macro_context.get("position_size_modifier", 1.0)
        macro_pts = (macro_mod - 0.5) * 14.0   # max +7 at mod=1.5, typical +7 at mod=1.0
        score += macro_pts
        _breakdown.append(f"macro=mod{macro_mod:.2f}({macro_pts:+.0f})")

        # ── Liquidation positioning bonus/penalty ─────────────────────────────
        score += liq_score_delta
        _breakdown.append(f"liq({liq_score_delta:+.1f})")

        # ── Spot order flow ───────────────────────────────────────────────────
        score += spot_score_delta
        _breakdown.append(f"spot({spot_score_delta:+.1f})")

        # ── Hyblock signals (MII, whale, OBI, CVD, OI, retail, PDL, liq bias) ─
        score += hyblock_score_delta
        _breakdown.append(f"hyblock({hyblock_score_delta:+.1f})")

        # ── L2 order book: walls + imbalance corroboration ────────────────────
        # Scoring is mode-aware:
        #
        # HA_TREND: wall BEHIND entry = floor/ceiling support; wall IN PATH = blocker.
        #   Raw book imbalance should agree with Hyblock OBI (both use order book data
        #   but OBI is smoothed/aggregated; raw imbalance is instantaneous).
        #   Agreement reinforces; divergence warns that something has changed.
        #
        # COUNTER-TREND: wall AT/ABOVE entry (SHORT) or AT/BELOW (LONG) is the rejection
        #   level we are fading — it IS the counter-trend confirmation in order book terms.
        #   The key divergence signal: Hyblock OBI still shows trend direction (BULLISH
        #   for counter-trend SHORT) but raw book imbalance has already flipped — this is
        #   the classic "distribution top / accumulation bottom" fingerprint.
        if self.ob_monitor is not None:
            _ob = self.ob_monitor.get_live_state(current_price, signal.liq_target_price)
            _ob_imb        = _ob["book_imbalance"]
            _bid_wall_pct  = _ob["bid_wall_pct"]
            _bid_wall_sz   = _ob["bid_wall_size_btc"]
            _ask_wall_pct  = _ob["ask_wall_pct"]
            _ask_wall_sz   = _ob["ask_wall_size_btc"]
            _block_sz      = _ob["blocking_wall_size_btc"]
            _ob_pts        = 0.0

            if _counter_trend:
                # ── WICK_FADE order book scoring ──────────────────────────────
                if candidate_direction == "SHORT":
                    # Ask wall close above = the rejection ceiling we're fading
                    if _ask_wall_sz > 0:
                        if _ask_wall_pct <= 0.3 and _ask_wall_sz >= LARGE_WALL_BTC:
                            _ob_pts += 10.0; _breakdown.append(f"ob_ask_wall@{_ask_wall_pct:.2f}%/{_ask_wall_sz:.0f}BTC(+10)")
                        elif _ask_wall_pct <= 0.3:
                            _ob_pts += 6.0;  _breakdown.append(f"ob_ask_wall@{_ask_wall_pct:.2f}%/{_ask_wall_sz:.0f}BTC(+6)")
                        elif _ask_wall_pct <= 0.6 and _ask_wall_sz >= LARGE_WALL_BTC:
                            _ob_pts += 6.0;  _breakdown.append(f"ob_ask_wall@{_ask_wall_pct:.2f}%/{_ask_wall_sz:.0f}BTC(+6)")
                        elif _ask_wall_pct <= 0.6:
                            _ob_pts += 3.0;  _breakdown.append(f"ob_ask_wall@{_ask_wall_pct:.2f}%/{_ask_wall_sz:.0f}BTC(+3)")
                        elif _ask_wall_pct <= 1.0:
                            _ob_pts += 1.0;  _breakdown.append(f"ob_ask_wall@{_ask_wall_pct:.2f}%/{_ask_wall_sz:.0f}BTC(+1)")
                    # Blocking bid wall below (in path to TP)
                    if _block_sz >= LARGE_WALL_BTC:
                        _ob_pts -= 8.0; _breakdown.append(f"ob_block={_block_sz:.0f}BTC(-8)")
                    elif _block_sz >= MIN_WALL_BTC:
                        _ob_pts -= 4.0; _breakdown.append(f"ob_block={_block_sz:.0f}BTC(-4)")
                    # Raw imbalance: selling pressure at the high is ideal
                    if _ob_imb < -0.20:
                        _ob_pts += 5.0; _breakdown.append(f"ob_imb={_ob_imb:+.2f}(sell_pressure+5)")
                    elif _ob_imb > 0.35:
                        _ob_pts -= 5.0; _breakdown.append(f"ob_imb={_ob_imb:+.2f}(still_buying-5)")
                    # OBI divergence: Hyblock OBI still bullish but raw book turning bearish
                    # = distribution top fingerprint = ideal wick fade SHORT confirmation
                    if obi_dir == "BULLISH" and _ob_imb < -0.20:
                        _ob_pts += 5.0; _breakdown.append("ob_obi_div=top(+5)")
                    elif obi_dir == "BEARISH" and _ob_imb < -0.20:
                        _ob_pts += 2.0; _breakdown.append("ob_obi_agree=short(+2)")
                    elif obi_dir == "BULLISH" and _ob_imb > 0.35:
                        _ob_pts -= 3.0; _breakdown.append("ob_obi_agree=bull/no_top(-3)")

                else:  # WICK_FADE LONG
                    # Bid wall close below = the support floor we're bouncing from
                    if _bid_wall_sz > 0:
                        if _bid_wall_pct <= 0.3 and _bid_wall_sz >= LARGE_WALL_BTC:
                            _ob_pts += 10.0; _breakdown.append(f"ob_bid_wall@{_bid_wall_pct:.2f}%/{_bid_wall_sz:.0f}BTC(+10)")
                        elif _bid_wall_pct <= 0.3:
                            _ob_pts += 6.0;  _breakdown.append(f"ob_bid_wall@{_bid_wall_pct:.2f}%/{_bid_wall_sz:.0f}BTC(+6)")
                        elif _bid_wall_pct <= 0.6 and _bid_wall_sz >= LARGE_WALL_BTC:
                            _ob_pts += 6.0;  _breakdown.append(f"ob_bid_wall@{_bid_wall_pct:.2f}%/{_bid_wall_sz:.0f}BTC(+6)")
                        elif _bid_wall_pct <= 0.6:
                            _ob_pts += 3.0;  _breakdown.append(f"ob_bid_wall@{_bid_wall_pct:.2f}%/{_bid_wall_sz:.0f}BTC(+3)")
                        elif _bid_wall_pct <= 1.0:
                            _ob_pts += 1.0;  _breakdown.append(f"ob_bid_wall@{_bid_wall_pct:.2f}%/{_bid_wall_sz:.0f}BTC(+1)")
                    # Blocking ask wall above (in path to TP)
                    if _block_sz >= LARGE_WALL_BTC:
                        _ob_pts -= 8.0; _breakdown.append(f"ob_block={_block_sz:.0f}BTC(-8)")
                    elif _block_sz >= MIN_WALL_BTC:
                        _ob_pts -= 4.0; _breakdown.append(f"ob_block={_block_sz:.0f}BTC(-4)")
                    # Raw imbalance: buying pressure at the low is ideal
                    if _ob_imb > 0.20:
                        _ob_pts += 5.0; _breakdown.append(f"ob_imb={_ob_imb:+.2f}(buy_pressure+5)")
                    elif _ob_imb < -0.35:
                        _ob_pts -= 5.0; _breakdown.append(f"ob_imb={_ob_imb:+.2f}(still_selling-5)")
                    # OBI divergence: Hyblock OBI still bearish but raw book turning bullish
                    # = accumulation bottom fingerprint = ideal wick fade LONG confirmation
                    if obi_dir == "BEARISH" and _ob_imb > 0.20:
                        _ob_pts += 5.0; _breakdown.append("ob_obi_div=bottom(+5)")
                    elif obi_dir == "BULLISH" and _ob_imb > 0.20:
                        _ob_pts += 2.0; _breakdown.append("ob_obi_agree=bull(+2)")
                    elif obi_dir == "BEARISH" and _ob_imb < -0.35:
                        _ob_pts -= 3.0; _breakdown.append("ob_obi_agree=bear/no_bottom(-3)")

            else:
                # ── HA_TREND order book scoring ───────────────────────────────
                if candidate_direction == "LONG":
                    # Bid wall behind (below entry) = floor — if wrong, price bounces
                    if _bid_wall_sz > 0:
                        if _bid_wall_pct <= 0.5 and _bid_wall_sz >= LARGE_WALL_BTC:
                            _ob_pts += 8.0; _breakdown.append(f"ob_floor@{_bid_wall_pct:.2f}%/{_bid_wall_sz:.0f}BTC(+8)")
                        elif _bid_wall_pct <= 0.5:
                            _ob_pts += 4.0; _breakdown.append(f"ob_floor@{_bid_wall_pct:.2f}%/{_bid_wall_sz:.0f}BTC(+4)")
                        elif _bid_wall_pct <= 1.0 and _bid_wall_sz >= LARGE_WALL_BTC:
                            _ob_pts += 4.0; _breakdown.append(f"ob_floor@{_bid_wall_pct:.2f}%/{_bid_wall_sz:.0f}BTC(+4)")
                        elif _bid_wall_pct <= 1.0:
                            _ob_pts += 2.0; _breakdown.append(f"ob_floor@{_bid_wall_pct:.2f}%/{_bid_wall_sz:.0f}BTC(+2)")
                    # Ask wall blocking path to TP
                    if _block_sz >= LARGE_WALL_BTC:
                        _ob_pts -= 10.0; _breakdown.append(f"ob_block={_block_sz:.0f}BTC(-10)")
                    elif _block_sz >= MIN_WALL_BTC:
                        _ob_pts -= 5.0;  _breakdown.append(f"ob_block={_block_sz:.0f}BTC(-5)")
                    # OBI corroboration: raw imbalance vs Hyblock OBI agreement
                    if _ob_imb > 0.35 and obi_dir == "BULLISH":
                        _ob_pts += 6.0; _breakdown.append(f"ob_obi_corr=bull({_ob_imb:+.2f}+6)")
                    elif _ob_imb > 0.20 and obi_dir == "BULLISH":
                        _ob_pts += 3.0; _breakdown.append(f"ob_obi_corr=bull({_ob_imb:+.2f}+3)")
                    elif _ob_imb < -0.20 and obi_dir == "BULLISH":
                        _ob_pts -= 6.0; _breakdown.append(f"ob_obi_div=bull/imb_neg({_ob_imb:+.2f}-6)")
                    elif _ob_imb > 0.35 and obi_dir == "NEUTRAL":
                        _ob_pts += 3.0; _breakdown.append(f"ob_imb=bull_raw({_ob_imb:+.2f}+3)")
                    elif _ob_imb < -0.35 and obi_dir == "NEUTRAL":
                        _ob_pts -= 3.0; _breakdown.append(f"ob_imb=bear_raw({_ob_imb:+.2f}-3)")

                else:  # HA_TREND SHORT
                    # Ask wall behind (above entry) = ceiling — if wrong, price rejects
                    if _ask_wall_sz > 0:
                        if _ask_wall_pct <= 0.5 and _ask_wall_sz >= LARGE_WALL_BTC:
                            _ob_pts += 8.0; _breakdown.append(f"ob_ceil@{_ask_wall_pct:.2f}%/{_ask_wall_sz:.0f}BTC(+8)")
                        elif _ask_wall_pct <= 0.5:
                            _ob_pts += 4.0; _breakdown.append(f"ob_ceil@{_ask_wall_pct:.2f}%/{_ask_wall_sz:.0f}BTC(+4)")
                        elif _ask_wall_pct <= 1.0 and _ask_wall_sz >= LARGE_WALL_BTC:
                            _ob_pts += 4.0; _breakdown.append(f"ob_ceil@{_ask_wall_pct:.2f}%/{_ask_wall_sz:.0f}BTC(+4)")
                        elif _ask_wall_pct <= 1.0:
                            _ob_pts += 2.0; _breakdown.append(f"ob_ceil@{_ask_wall_pct:.2f}%/{_ask_wall_sz:.0f}BTC(+2)")
                    # Bid wall blocking path to TP
                    if _block_sz >= LARGE_WALL_BTC:
                        _ob_pts -= 10.0; _breakdown.append(f"ob_block={_block_sz:.0f}BTC(-10)")
                    elif _block_sz >= MIN_WALL_BTC:
                        _ob_pts -= 5.0;  _breakdown.append(f"ob_block={_block_sz:.0f}BTC(-5)")
                    # OBI corroboration
                    if _ob_imb < -0.35 and obi_dir == "BEARISH":
                        _ob_pts += 6.0; _breakdown.append(f"ob_obi_corr=bear({_ob_imb:+.2f}+6)")
                    elif _ob_imb < -0.20 and obi_dir == "BEARISH":
                        _ob_pts += 3.0; _breakdown.append(f"ob_obi_corr=bear({_ob_imb:+.2f}+3)")
                    elif _ob_imb > 0.20 and obi_dir == "BEARISH":
                        _ob_pts -= 6.0; _breakdown.append(f"ob_obi_div=bear/imb_pos({_ob_imb:+.2f}-6)")
                    elif _ob_imb < -0.35 and obi_dir == "NEUTRAL":
                        _ob_pts += 3.0; _breakdown.append(f"ob_imb=bear_raw({_ob_imb:+.2f}+3)")
                    elif _ob_imb > 0.35 and obi_dir == "NEUTRAL":
                        _ob_pts -= 3.0; _breakdown.append(f"ob_imb=bull_raw({_ob_imb:+.2f}-3)")

            score += _ob_pts
            signal.ob_state = _ob

        # ── aggTrade taker flow ───────────────────────────────────────────────
        # Executed futures taker volume — buyers/sellers who paid the spread to enter.
        # Unlike OBI (can be spoofed) and CVD (delayed), this is raw executed conviction.
        # 5m: current pressure; 15m: sustained vs spike context.
        # Counter-trend entries: flow reversing AT the extreme is the key entry confirmation.
        if self.trade_flow_monitor is not None:
            _tf = self.trade_flow_monitor.get_live_state()
            signal.trade_flow_state = _tf
            tbr_5m  = _tf.get("taker_buy_ratio_5m",  0.5)
            tbr_15m = _tf.get("taker_buy_ratio_15m", 0.5)
            _tf_pts = 0.0

            _sustained_buy  = tbr_5m > 0.60 and tbr_15m > 0.55
            _burst_buy      = tbr_5m > 0.60
            _sustained_sell = tbr_5m < 0.40 and tbr_15m < 0.45
            _burst_sell     = tbr_5m < 0.40

            if candidate_direction == "LONG":
                if _sustained_buy:
                    _tf_pts = 8.0
                elif _burst_buy:
                    _tf_pts = 4.0
                elif _sustained_sell:
                    _tf_pts = -8.0
                elif _burst_sell:
                    _tf_pts = -4.0
            else:  # SHORT
                if _sustained_sell:
                    _tf_pts = 8.0
                elif _burst_sell:
                    _tf_pts = 4.0
                elif _sustained_buy:
                    _tf_pts = -8.0
                elif _burst_buy:
                    _tf_pts = -4.0

            if _tf_pts != 0.0:
                score += _tf_pts
                _breakdown.append(f"aggflow={tbr_5m:.2f}/{tbr_15m:.2f}({_tf_pts:+.0f})")
            else:
                _breakdown.append(f"aggflow={tbr_5m:.2f}/neutral(+0)")

        # ── Live liquidation cascade confirmation ─────────────────────────────
        if self.liq_stream_monitor is not None:
            _liq_state = self.liq_stream_monitor.get_live_state()
            signal.live_liq_state = _liq_state
            if _liq_state["cascade_live"]:
                _casc_live_dir = _liq_state["cascade_direction"]
                if _casc_live_dir and _casc_live_dir == candidate_direction:
                    _live_pts = 15.0 + (5.0 if _liq_state["accelerating"] else 0.0)
                    score += _live_pts
                    _breakdown.append(f"live_cascade={_casc_live_dir}({_live_pts:+.0f})")
                elif _casc_live_dir:
                    score -= 15.0
                    _breakdown.append(f"live_cascade={_casc_live_dir}/opp(-15)")

        # ── Market regime: Efficiency Ratio + volatility ──────────────────────
        # HA_TREND and WICK_FADE have inverse relationships with regime:
        #   TRENDING  + HA_TREND  = aligned  → reinforces; price moving efficiently means
        #                                       momentum continues (what HA_TREND needs)
        #   TRENDING  + WICK_FADE = opposed  → fading a trend that's proven to persist is
        #                                       high-risk; trend can easily overrun the fade
        #   RANGING   + HA_TREND  = opposed  → trend signals whipsaw in choppy conditions;
        #                                       HA flips before follow-through materializes
        #   RANGING   + WICK_FADE = aligned  → range extremes are exactly where wick fades
        #                                       work; price has no momentum to break away
        #   HIGH_VOL  + either    = caution  → expanded volatility is dangerous for both;
        #                                       stops get hit by noise, slippage is high;
        #                                       wick fades are worse (can't fade a spike)
        #   NEUTRAL               = no edge  → market character unclear; no adjustment
        if _regime == "TRENDING":
            if not _counter_trend:
                score += 10.0
                _breakdown.append("regime=TRENDING/trend(+10)")
            else:
                score -= 8.0
                _breakdown.append("regime=TRENDING/counter(-8)")
        elif _regime == "RANGING":
            if not _counter_trend:
                score -= 12.0
                _breakdown.append("regime=RANGING/trend(-12)")
            else:
                score += 10.0
                _breakdown.append("regime=RANGING/counter(+10)")
        elif _regime == "HIGH_VOL":
            if not _counter_trend:
                score -= 5.0
                _regime_modifier = 0.75
                _breakdown.append("regime=HIGH_VOL/trend(-5/0.75x)")
            else:
                score -= 10.0
                _regime_modifier = 0.5
                _breakdown.append("regime=HIGH_VOL/counter(-10/0.5x)")
        else:
            _breakdown.append(f"regime={_regime}(+0)")

        signal.regime           = _regime
        signal.regime_er        = _regime_data["er"]
        signal.regime_vol_ratio = _regime_data["vol_ratio"]

        score = min(100.0, max(0.0, score))
        signal.confidence_score = score
        logger.info(f"Score breakdown [{candidate_direction}]: {' | '.join(_breakdown)} → raw={score:.1f}%")

        # ─── Step 10: Determine strength ─────────────────────────────────
        _fire_threshold   = 80.0
        _strong_threshold = 90.0
        if score >= _strong_threshold:
            signal.strength = "STRONG"
        else:
            signal.strength = "WEAK"

        # ─── Step 10.5: Minimum confidence gate ──────────────────────────
        if score < _fire_threshold:
            _mode_label = "counter-trend" if _counter_trend else "trend-following"
            signal.block_reasons.append(
                f"Confidence {score:.0f}% below {_fire_threshold:.0f}% threshold ({_mode_label}) — no trade"
            )
            logger.info(
                f"[{candidate_direction}] BLOCKED — confidence {score:.0f}% < {_fire_threshold:.0f}% "
                f"({_mode_label}) | MII={mii:+.2f} | breakdown: {' '.join(_breakdown[-4:])}"
            )
            signal.direction = candidate_direction
            signal.strength = "BLOCKED"
            signal.block_stage = "CONFIDENCE"
            signal.score_breakdown = list(_breakdown)
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

        combined_modifier = (
            funding_modifier
            * macro_context.get("position_size_modifier", 1.0)
            * _regime_modifier
        )
        signal.position_size_modifier = max(0.25, combined_modifier)

        # ─── Final: Build entry reason ────────────────────────────────────
        signal.direction = candidate_direction
        signal.should_trade = True
        _liq_target_str = (
            f"${signal.liq_target_price:,.0f}" if signal.liq_target_price else "none"
        )
        _long_pct_str  = f"{signal.liq_level_long_pct:.1f}"  if signal.liq_level_long_pct  is not None else "N/A"
        _short_pct_str = f"{signal.liq_level_short_pct:.1f}" if signal.liq_level_short_pct is not None else "N/A"
        _casc_str = (
            f"LONG@{_long_pct_str}%/{signal.liq_level_long_size:.0f}BTC "
            f"SHORT@{_short_pct_str}%/{signal.liq_level_short_size:.0f}BTC"
        )
        _mode_str = (
            f"WICK_FADE(24h_high=${_24h_high:,.0f} dist={_dist_from_24h_high_pct:.2f}%)"
            if _wick_fade_mode and candidate_direction == "SHORT"
            else f"WICK_FADE(24h_low=${_24h_low:,.0f} dist={_dist_from_24h_low_pct:.2f}%)"
            if _wick_fade_mode
            else "HA_TREND"
        )
        signal.entry_reason = (
            f"{candidate_direction} | Mode={_mode_str} | Regime={_regime}(ER={_regime_data['er']:.2f}) | "
            f"Cascade={_casc_str} | MII={mii:+.2f} | "
            f"VR={signal.volume_ratio:+.2f} | BSR={signal.buy_sell_count_ratio:+.2f} | "
            f"HA: 3m={signal.ha_3m_color} 1h={signal.ha_1h_color} 6h={signal.ha_6h_color} | "
            f"LiqTarget={_liq_target_str} | Zone={zone_key} ({zone_position}) | "
            f"Confidence={score:.0f}% | Funding={funding_analysis.get('average_rate', 0)*100:.3f}% | "
            f"Size={signal.position_size_modifier:.2f}x"
        )

        signal.block_stage = "NONE"
        signal.score_breakdown = list(_breakdown)
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
    ) -> Tuple[bool, str]:
        """
        Exit model: liq cluster TP (checked every 1s in bot_engine) + trailing stop.
        No HA-based exits — liq cluster scalping holds through candle noise.
        """
        return self.check_trailing_stop(
            position_side, entry_price, current_price, peak_profit_pct
        )

