"""
Backtesting Engine
Simulates the full Vroom Capital strategy over historical data tick-by-tick.
Uses 1H candles as the simulation clock, with 6H candles for trend.
"""
import asyncio
from collections import deque
from datetime import datetime, timezone
from typing import List, Dict, Optional, Callable, Tuple, Deque
from dataclasses import dataclass, field
from loguru import logger

from backend.strategy.heikin_ashi import compute_heikin_ashi, get_trend, count_consecutive_opposite, detect_reversal
from backend.strategy.zones import ZoneTracker, get_zone_key, get_zone_position, is_whole_number_break
from backend.strategy.time_filter import check_time_filter, get_time_context
from backend.strategy.velocity import check_velocity_filter, compute_velocity
from backend.strategy.macro_calendar import MacroCalendar, FOMC_DATES
from backend.backtest.data_loader import (
    download_klines, download_funding_rates,
    build_6h_from_1h, get_funding_at_time, clear_cache
)
from datetime import date


# ─────────────────────────────────────────────────────────────────
# Data classes
# ─────────────────────────────────────────────────────────────────

@dataclass
class BacktestConfig:
    start_year: int = 2020
    end_year: int = 2025
    initial_capital: float = 1000.0
    leverage: int = 75
    position_size_pct: float = 0.30
    liquidation_buffer_usd: float = 4500.0
    tp1_pct: float = 0.20
    tp2_pct: float = 0.30
    velocity_threshold_pct: float = 1.5
    velocity_window_hours: int = 2
    zone_cooldown_minutes: int = 120
    emergency_candles: int = 4
    fomc_caution_days: int = 7
    use_time_filter: bool = True
    use_velocity_filter: bool = True
    use_funding_filter: bool = True
    use_macro_filter: bool = True
    use_zone_system: bool = True
    use_second_break_rule: bool = True
    # Funding extremes (block trade)
    funding_extreme_positive: float = 0.001   # 0.1%
    funding_extreme_negative: float = -0.0005  # -0.05%
    # Trailing stop — must match live config defaults
    trailing_peak_threshold_pct: float = 25.0   # above this peak, use wide trail
    trailing_after_tp1_peak_low_pct: float = 1.0  # trail % when peak < 25%
    trailing_after_tp1_peak_high_pct: float = 5.0  # trail % when peak >= 25%


@dataclass
class SimTrade:
    trade_id: int
    direction: str              # LONG | SHORT
    entry_price: float
    entry_time: int             # timestamp ms
    margin_usd: float
    position_size_usd: float
    leverage: int
    liquidation_price: float
    zone: str
    entry_reason: str

    # Exit fields (filled when closed)
    exit_price: float = 0.0
    exit_time: int = 0
    exit_reason: str = ""
    realized_pnl_pct: float = 0.0
    realized_pnl_usd: float = 0.0
    peak_profit_pct: float = 0.0
    status: str = "OPEN"        # OPEN | CLOSED | LIQUIDATED

    # Signal metadata
    ha_6h_color: str = ""
    ha_1h_color: str = ""
    funding_rate: float = 0.0
    signal_score: float = 0.0
    time_label: str = ""


@dataclass
class BacktestResult:
    config: BacktestConfig
    trades: List[SimTrade] = field(default_factory=list)
    equity_curve: List[Dict] = field(default_factory=list)  # {time, equity, drawdown}
    daily_pnl: List[Dict] = field(default_factory=list)
    monthly_pnl: List[Dict] = field(default_factory=list)
    block_stats: Dict[str, int] = field(default_factory=dict)

    # Summary stats (computed after run)
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    liquidations: int = 0
    win_rate: float = 0.0
    total_pnl_usd: float = 0.0
    total_return_pct: float = 0.0
    avg_win_pct: float = 0.0
    avg_loss_pct: float = 0.0
    avg_win_usd: float = 0.0
    avg_loss_usd: float = 0.0
    max_drawdown_pct: float = 0.0
    max_drawdown_usd: float = 0.0
    profit_factor: float = 0.0
    sharpe_ratio: float = 0.0
    best_trade_pct: float = 0.0
    worst_trade_pct: float = 0.0
    avg_holding_hours: float = 0.0
    long_trades: int = 0
    short_trades: int = 0
    long_win_rate: float = 0.0
    short_win_rate: float = 0.0
    final_capital: float = 0.0
    peak_capital: float = 0.0
    start_date: str = ""
    end_date: str = ""
    data_points: int = 0


# ─────────────────────────────────────────────────────────────────
# Main engine
# ─────────────────────────────────────────────────────────────────

class BacktestEngine:

    # Rolling window sizes
    HA_1H_WINDOW = 60       # Candles to keep for 1H HA
    HA_6H_WINDOW = 30       # Candles to keep for 6H HA
    VELOCITY_WINDOW = 10    # Extra candles for velocity calc

    def __init__(self, config: BacktestConfig):
        self.config = config
        self._progress: float = 0.0
        self._progress_msg: str = "Idle"
        self._running: bool = False
        self._cancelled: bool = False

    def get_progress(self) -> Tuple[float, str]:
        return self._progress, self._progress_msg

    def cancel(self):
        self._cancelled = True

    async def run(self, progress_cb: Optional[Callable[[float, str], None]] = None) -> BacktestResult:
        self._running = True
        self._cancelled = False
        result = BacktestResult(config=self.config)

        def _progress(pct: float, msg: str):
            self._progress = pct
            self._progress_msg = msg
            if progress_cb:
                progress_cb(pct, msg)

        try:
            # ─── Step 1: Download data ──────────────────────────────
            _progress(0.0, "Downloading historical data...")
            from datetime import datetime, timezone

            start_dt = datetime(self.config.start_year, 1, 1, tzinfo=timezone.utc)
            end_dt = datetime(self.config.end_year + 1, 1, 1, tzinfo=timezone.utc)

            def kline_progress(pct, msg):
                _progress(pct * 0.30, msg)

            candles_1h = await download_klines("1h", start_dt, end_dt, kline_progress)
            _progress(0.30, "Building 6H candles...")
            candles_6h = build_6h_from_1h(candles_1h)

            _progress(0.32, "Downloading funding rates...")
            funding_rates = await download_funding_rates(start_dt, end_dt,
                lambda pct, msg: _progress(0.32 + pct * 0.05, msg))

            _progress(0.37, f"Loaded {len(candles_1h)} 1H + {len(candles_6h)} 6H candles. Running simulation...")
            result.data_points = len(candles_1h)
            result.start_date = datetime.fromtimestamp(candles_1h[0]["open_time"] / 1000).strftime("%Y-%m-%d")
            result.end_date = datetime.fromtimestamp(candles_1h[-1]["open_time"] / 1000).strftime("%Y-%m-%d")

            # ─── Step 2: Run simulation ─────────────────────────────
            await self._simulate(candles_1h, candles_6h, funding_rates, result, _progress)

        except Exception as e:
            logger.exception(f"Backtest error: {e}")
            _progress(1.0, f"Error: {e}")
            raise
        finally:
            self._running = False

        return result

    async def _simulate(
        self,
        candles_1h: List[Dict],
        candles_6h: List[Dict],
        funding_rates: Dict[int, float],
        result: BacktestResult,
        progress_cb: Callable,
    ):
        cfg = self.config
        capital = cfg.initial_capital
        peak_capital = capital
        trade_counter = 0
        current_trade: Optional[SimTrade] = None
        zone_tracker = ZoneTracker(zone_size=1000.0, cooldown_minutes=cfg.zone_cooldown_minutes)
        macro = MacroCalendar()
        block_stats = {}

        # Rolling candle buffers
        buf_1h: Deque[Dict] = deque(maxlen=self.HA_1H_WINDOW + self.VELOCITY_WINDOW)
        buf_6h: Deque[Dict] = deque(maxlen=self.HA_6H_WINDOW)

        # Index 6H candles by time for fast lookup
        idx_6h = 0
        n_6h = len(candles_6h)
        n_1h = len(candles_1h)

        prev_price: Optional[float] = None
        first_breaks: Dict[str, Dict] = {}

        equity_curve = []
        daily_equity: Dict[str, float] = {}
        monthly_equity: Dict[str, float] = {}

        for i, candle_1h in enumerate(candles_1h):
            if self._cancelled:
                break

            ts_ms = candle_1h["open_time"]
            close_price = candle_1h["close"]
            dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
            day_key = dt.strftime("%Y-%m-%d")
            month_key = dt.strftime("%Y-%m")

            # Progress update every 500 candles
            if i % 500 == 0:
                pct = 0.37 + (i / n_1h) * 0.60
                progress_cb(pct, f"Simulating {dt.strftime('%Y-%m')} | Capital: ${capital:.0f} | Trades: {trade_counter}")
                await asyncio.sleep(0)  # Yield control so progress can be read

            # Fill 6H buffer — add all 6H candles that have closed before this 1H candle
            while idx_6h < n_6h and candles_6h[idx_6h]["open_time"] <= ts_ms:
                buf_6h.append(candles_6h[idx_6h])
                idx_6h += 1

            # Fill 1H buffer
            buf_1h.append(candle_1h)

            # Need enough history
            if len(buf_1h) < 10 or len(buf_6h) < 4:
                equity_curve.append({"time": ts_ms, "equity": capital, "drawdown": 0.0})
                prev_price = close_price
                continue

            buf_list_1h = list(buf_1h)
            buf_list_6h = list(buf_6h)

            # ─── Manage open position ──────────────────────────────
            if current_trade is not None:
                current_trade, capital, closed = self._update_position(
                    current_trade, candle_1h, buf_list_1h, buf_list_6h, capital
                )
                if closed:
                    result.trades.append(current_trade)
                    trade_counter += 1
                    current_trade = None

            # ─── Try to open new position ──────────────────────────
            if current_trade is None:
                signal = self._generate_signal(
                    buf_list_1h, buf_list_6h, close_price, dt, ts_ms,
                    funding_rates, zone_tracker, first_breaks, prev_price,
                    macro, cfg, block_stats
                )
                if signal and capital >= 10:
                    # ── Position sizing for exact $4,500 liquidation buffer ──
                    # Cross-margin math: liq_price = entry ± buffer (exact)
                    # qty = capital / (buffer + entry × maint_rate)
                    MAINT_RATE = 0.005
                    MAX_NOTIONAL = 5_000_000.0   # $5M cap — Bitunix liquidity ceiling
                    target_buf = cfg.liquidation_buffer_usd  # $4,500

                    # Apply size modifier (FOMC/funding) — slightly adjusts buffer
                    size_mod = signal["size_mod"]
                    qty = capital * size_mod / (target_buf + close_price * MAINT_RATE)
                    pos_size = qty * close_price

                    # Cap at exchange liquidity limit
                    if pos_size > MAX_NOTIONAL:
                        pos_size = MAX_NOTIONAL
                        qty = pos_size / close_price

                    margin = pos_size / cfg.leverage

                    # Liquidation prices are exactly ± buffer from entry
                    if signal["direction"] == "LONG":
                        liq_price = close_price - target_buf * size_mod
                    else:
                        liq_price = close_price + target_buf * size_mod

                    liq_buffer = abs(close_price - liq_price)

                    if margin < 5.0:  # sanity: need at least $5 margin
                        _block(block_stats, "margin_too_small")
                    else:
                        trade_counter += 1
                        current_trade = SimTrade(
                            trade_id=trade_counter,
                            direction=signal["direction"],
                            entry_price=close_price,
                            entry_time=ts_ms,
                            margin_usd=margin,
                            position_size_usd=pos_size,
                            leverage=cfg.leverage,
                            liquidation_price=liq_price,
                            zone=signal["zone"],
                            entry_reason=signal["reason"],
                            ha_6h_color=signal["ha_6h_color"],
                            ha_1h_color=signal["ha_1h_color"],
                            funding_rate=signal.get("funding_rate", 0.0),
                            signal_score=signal.get("score", 0.0),
                            time_label=signal.get("time_label", ""),
                            status="OPEN",
                        )
                        zone_tracker.record_signal(signal["zone"], signal["direction"])

            prev_price = close_price

            # ─── Track equity ──────────────────────────────────────
            unrealized = 0.0
            if current_trade:
                qty_open = current_trade.position_size_usd / current_trade.entry_price
                if current_trade.direction == "LONG":
                    unrealized = qty_open * (close_price - current_trade.entry_price)
                else:
                    unrealized = qty_open * (current_trade.entry_price - close_price)

            equity_at_tick = capital + unrealized
            drawdown = max(0, (peak_capital - equity_at_tick) / peak_capital * 100) if peak_capital > 0 else 0
            if equity_at_tick > peak_capital:
                peak_capital = equity_at_tick

            equity_curve.append({"time": ts_ms, "equity": round(equity_at_tick, 2), "drawdown": round(drawdown, 2)})
            daily_equity[day_key] = equity_at_tick
            monthly_equity[month_key] = equity_at_tick

        # Close any open trade at end
        if current_trade is not None:
            close_price = candles_1h[-1]["close"]
            self._close_trade(current_trade, close_price, candles_1h[-1]["open_time"], "End of backtest", capital)
            result.trades.append(current_trade)
            capital += current_trade.realized_pnl_usd

        result.equity_curve = self._downsample_equity(equity_curve, max_points=500)
        result.block_stats = block_stats

        # Build daily P&L
        daily_pnl_map: Dict[str, float] = {}
        for trade in result.trades:
            if trade.exit_time:
                d = datetime.fromtimestamp(trade.exit_time / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                daily_pnl_map[d] = daily_pnl_map.get(d, 0.0) + trade.realized_pnl_usd

        result.daily_pnl = [
            {"date": k, "pnl": round(v, 2)}
            for k, v in sorted(daily_pnl_map.items())
        ]

        # Monthly P&L
        monthly_pnl_map: Dict[str, float] = {}
        for trade in result.trades:
            if trade.exit_time:
                m = datetime.fromtimestamp(trade.exit_time / 1000, tz=timezone.utc).strftime("%Y-%m")
                monthly_pnl_map[m] = monthly_pnl_map.get(m, 0.0) + trade.realized_pnl_usd

        result.monthly_pnl = [
            {"month": k, "pnl": round(v, 2)}
            for k, v in sorted(monthly_pnl_map.items())
        ]

        # ─── Compute summary stats ─────────────────────────────────
        self._compute_stats(result, cfg.initial_capital, peak_capital, capital)
        progress_cb(1.0, f"Done! {len(result.trades)} trades, {result.win_rate:.1f}% win rate")

    def _generate_signal(
        self,
        candles_1h: List[Dict],
        candles_6h: List[Dict],
        price: float,
        dt: datetime,
        ts_ms: int,
        funding_rates: Dict,
        zone_tracker: ZoneTracker,
        first_breaks: Dict,
        prev_price: Optional[float],
        macro: MacroCalendar,
        cfg: BacktestConfig,
        block_stats: Dict,
    ) -> Optional[Dict]:

        # ─── HA Analysis ──────────────────────────────────────────
        ha_1h = compute_heikin_ashi(candles_1h[-50:])
        ha_6h = compute_heikin_ashi(candles_6h[-20:])

        if not ha_1h or not ha_6h:
            return None

        ha_1h_color = ha_1h[-1]["color"]
        ha_6h_color = ha_6h[-1]["color"]
        ha_6h_trend = get_trend(ha_6h, lookback=3)

        # Both HA must agree AND 6H trend must be confirmed by multiple candles
        # (mirrors live signal_engine: bullish_trend/bearish_trend requirement)
        if ha_1h_color == "GREEN" and ha_6h_color == "GREEN" and ha_6h_trend in ("BULLISH", "STRONG_BULLISH"):
            direction = "LONG"
        elif ha_1h_color == "RED" and ha_6h_color == "RED" and ha_6h_trend in ("BEARISH", "STRONG_BEARISH"):
            direction = "SHORT"
        else:
            _block(block_stats, "ha_conflict")
            return None

        zone_key = get_zone_key(price)
        zone_pos = get_zone_position(price)

        # ─── Zone cooldown ─────────────────────────────────────────
        if cfg.use_zone_system and zone_tracker.is_in_cooldown(zone_key, direction):
            _block(block_stats, "zone_cooldown")
            return None

        # ─── Second break rule ─────────────────────────────────────
        if cfg.use_second_break_rule and prev_price is not None:
            break_dir = is_whole_number_break(price, prev_price)
            if break_dir:
                fb_key = f"{zone_key}_{break_dir}"
                if fb_key not in first_breaks:
                    first_breaks[fb_key] = ts_ms
                    _block(block_stats, "first_break")
                    return None
                else:
                    # First break was more than 60 minutes ago — reset
                    elapsed_ms = ts_ms - first_breaks[fb_key]
                    if elapsed_ms > 60 * 60 * 1000:
                        first_breaks[fb_key] = ts_ms
                        _block(block_stats, "first_break")
                        return None
                    # Second break — proceed!

        # ─── Time filter ──────────────────────────────────────────
        if cfg.use_time_filter:
            import pytz
            est = pytz.timezone("America/New_York")
            hour_est = dt.astimezone(est).hour
            allowed, _ = check_time_filter(direction, hour_est=hour_est)
            if not allowed:
                _block(block_stats, f"time_filter_{direction.lower()}")
                return None

        # ─── Velocity filter ──────────────────────────────────────
        if cfg.use_velocity_filter:
            vel_ok, _, vel_data = check_velocity_filter(
                candles_1h, direction,
                threshold_pct=cfg.velocity_threshold_pct,
                window_hours=cfg.velocity_window_hours,
            )
            if not vel_ok:
                _block(block_stats, f"velocity_{direction.lower()}")
                return None

        # ─── Funding rate check ────────────────────────────────────
        funding_rate = get_funding_at_time(funding_rates, ts_ms)
        size_mod = 1.0

        if cfg.use_funding_filter:
            if direction == "LONG" and funding_rate > cfg.funding_extreme_positive:
                _block(block_stats, "funding_extreme_long")
                return None
            if direction == "SHORT" and funding_rate < cfg.funding_extreme_negative:
                _block(block_stats, "funding_extreme_short")
                return None
            if funding_rate > 0.0005:
                size_mod *= 0.75
            elif funding_rate < -0.0002:
                size_mod *= 0.75

        # ─── Macro calendar ───────────────────────────────────────
        if cfg.use_macro_filter:
            fomc_days_away = _days_to_fomc(dt.date())
            if fomc_days_away is not None:
                if fomc_days_away == 0:
                    est = __import__("pytz").timezone("America/New_York")
                    hour_est = dt.astimezone(est).hour
                    if 12 <= hour_est < 16:
                        _block(block_stats, "fomc_window")
                        return None
                    size_mod *= 0.50
                elif fomc_days_away <= 1:
                    size_mod *= 0.50
                elif fomc_days_away <= cfg.fomc_caution_days:
                    size_mod *= 0.75

        # ─── Confidence score ──────────────────────────────────────
        score = 50.0
        trend = get_trend(ha_6h, lookback=3)
        if trend in ("STRONG_BULLISH", "STRONG_BEARISH"):
            score += 20.0
        elif trend in ("BULLISH", "BEARISH"):
            score += 10.0

        if (direction == "SHORT" and zone_pos == "TOP") or (direction == "LONG" and zone_pos == "BOTTOM"):
            score += 10.0

        time_ctx = {}
        import pytz
        est = pytz.timezone("America/New_York")
        hour_est = dt.astimezone(est).hour
        time_label = _get_time_label(hour_est)

        return {
            "direction": direction,
            "zone": zone_key,
            "zone_pos": zone_pos,
            "ha_6h_color": ha_6h_color,
            "ha_1h_color": ha_1h_color,
            "funding_rate": funding_rate,
            "size_mod": min(size_mod, 1.0),
            "score": score,
            "time_label": time_label,
            "reason": f"{direction} | Zone {zone_key} {zone_pos} | HA 6h={ha_6h_color} 1h={ha_1h_color} | Score={score:.0f}",
        }

    def _update_position(
        self,
        trade: SimTrade,
        candle: Dict,
        buf_1h: List[Dict],
        buf_6h: List[Dict],
        capital: float,
    ) -> Tuple[SimTrade, float, bool]:
        """
        Update an open position. Returns (trade, capital, was_closed).
        Exit logic mirrors the live signal_engine.get_exit_signal() exactly:
        - P&L is leverage-adjusted %: pnl_pct = (price_delta / entry) * leverage * 100
        - TP1 at 20% P&L — then trail at -1% from peak (or -5% if peak >= 25%)
        - Emergency close: 4 consecutive opposing 1H HA candles
        - 6H HA reversal
        """
        cfg = self.config
        high = candle["high"]
        low = candle["low"]
        close = candle["close"]
        ts_ms = candle["open_time"]
        entry = trade.entry_price
        lev = cfg.leverage

        # ─── 1. Liquidation check (intra-candle high/low) ─────────
        if trade.direction == "LONG" and low <= trade.liquidation_price:
            self._close_trade(trade, trade.liquidation_price, ts_ms, "Liquidated")
            trade.status = "LIQUIDATED"
            capital += trade.realized_pnl_usd
            return trade, capital, True

        if trade.direction == "SHORT" and high >= trade.liquidation_price:
            self._close_trade(trade, trade.liquidation_price, ts_ms, "Liquidated")
            trade.status = "LIQUIDATED"
            capital += trade.realized_pnl_usd
            return trade, capital, True

        # ─── 2. Leverage-adjusted P&L % ───────────────────────────
        if trade.direction == "LONG":
            current_pnl_pct = (close - entry) / entry * 100.0 * lev
            intra_best_pnl  = (high  - entry) / entry * 100.0 * lev
        else:
            current_pnl_pct = (entry - close) / entry * 100.0 * lev
            intra_best_pnl  = (entry - low)   / entry * 100.0 * lev

        # Update peak (using intra-candle best price, same as live system)
        if intra_best_pnl > trade.peak_profit_pct:
            trade.peak_profit_pct = intra_best_pnl

        peak_pnl = trade.peak_profit_pct

        # ─── 3. TP1 trailing stop (matches live get_exit_signal) ──
        tp1_pct = cfg.tp1_pct * 100  # 20.0

        if peak_pnl >= tp1_pct:
            trail_pct = (cfg.trailing_after_tp1_peak_high_pct
                         if peak_pnl >= cfg.trailing_peak_threshold_pct
                         else cfg.trailing_after_tp1_peak_low_pct)

            drawdown = peak_pnl - current_pnl_pct
            if drawdown >= trail_pct:
                # Convert trail stop P&L back to a price for realistic fill
                trail_stop_pnl = peak_pnl - trail_pct
                if trade.direction == "LONG":
                    trail_price = entry * (1 + trail_stop_pnl / (100.0 * lev))
                    exit_px = max(trail_price, low)
                else:
                    trail_price = entry * (1 - trail_stop_pnl / (100.0 * lev))
                    exit_px = min(trail_price, high)
                self._close_trade(trade, exit_px, ts_ms,
                    f"Trailing stop: peak={peak_pnl:.1f}% current={current_pnl_pct:.1f}% "
                    f"drawdown={drawdown:.1f}% > trail={trail_pct}%")
                capital += trade.realized_pnl_usd
                return trade, capital, True

        elif current_pnl_pct >= (tp1_pct - 1.0):
            # Near TP1 — matches live get_exit_signal() TP1 protection branch
            if peak_pnl >= tp1_pct and (peak_pnl - current_pnl_pct) >= 1.0:
                self._close_trade(trade, close, ts_ms,
                    f"TP1 protection: reached {peak_pnl:.1f}%, now at {current_pnl_pct:.1f}%")
                capital += trade.realized_pnl_usd
                return trade, capital, True

        # ─── 4. 4-candle emergency close ──────────────────────────
        ha_1h = compute_heikin_ashi(buf_1h[-20:])
        consecutive = count_consecutive_opposite(ha_1h, trade.direction)
        if consecutive >= cfg.emergency_candles:
            self._close_trade(trade, close, ts_ms,
                f"Emergency: {consecutive} consecutive opposing 1H candles")
            capital += trade.realized_pnl_usd
            return trade, capital, True

        # ─── 5. 6H reversal check ─────────────────────────────────
        ha_6h = compute_heikin_ashi(buf_6h[-10:])
        if detect_reversal(ha_6h, trade.direction):
            self._close_trade(trade, close, ts_ms, "6H HA reversal detected")
            capital += trade.realized_pnl_usd
            return trade, capital, True

        return trade, capital, False

    def _close_trade(self, trade: SimTrade, exit_price: float, exit_time: int, reason: str, capital: float = None):
        """Finalize a trade. P&L = qty × price_delta."""
        qty = trade.position_size_usd / trade.entry_price
        if trade.direction == "LONG":
            pnl_usd = qty * (exit_price - trade.entry_price)
        else:
            pnl_usd = qty * (trade.entry_price - exit_price)

        pnl_pct = (pnl_usd / trade.margin_usd * 100) if trade.margin_usd > 0 else 0.0

        trade.exit_price = exit_price
        trade.exit_time = exit_time
        trade.exit_reason = reason
        trade.realized_pnl_pct = round(pnl_pct, 2)
        trade.realized_pnl_usd = round(pnl_usd, 2)
        trade.status = "CLOSED" if "Liquidat" not in reason else "LIQUIDATED"

    def _compute_stats(self, result: BacktestResult, initial_capital: float, peak_capital: float, final_capital: float):
        """Compute summary statistics from trades and equity curve."""
        trades = [t for t in result.trades if t.exit_time > 0]
        if not trades:
            result.final_capital = initial_capital
            return

        wins = [t for t in trades if t.realized_pnl_usd > 0]
        losses = [t for t in trades if t.realized_pnl_usd <= 0 and t.status != "LIQUIDATED"]
        liqs = [t for t in trades if t.status == "LIQUIDATED"]

        total_pnl = sum(t.realized_pnl_usd for t in trades)

        result.total_trades = len(trades)
        result.winning_trades = len(wins)
        result.losing_trades = len(losses)
        result.liquidations = len(liqs)
        result.win_rate = round(len(wins) / len(trades) * 100, 1) if trades else 0
        result.total_pnl_usd = round(total_pnl, 2)
        result.total_return_pct = round(total_pnl / initial_capital * 100, 1)
        result.avg_win_pct = round(sum(t.realized_pnl_pct for t in wins) / len(wins), 2) if wins else 0
        result.avg_loss_pct = round(sum(t.realized_pnl_pct for t in losses + liqs) / len(losses + liqs), 2) if (losses + liqs) else 0
        result.avg_win_usd = round(sum(t.realized_pnl_usd for t in wins) / len(wins), 2) if wins else 0
        result.avg_loss_usd = round(sum(t.realized_pnl_usd for t in losses + liqs) / len(losses + liqs), 2) if (losses + liqs) else 0
        result.profit_factor = round(abs(result.avg_win_usd / result.avg_loss_usd), 2) if result.avg_loss_usd != 0 else 0
        result.best_trade_pct = round(max((t.realized_pnl_pct for t in trades), default=0), 2)
        result.worst_trade_pct = round(min((t.realized_pnl_pct for t in trades), default=0), 2)
        result.final_capital = round(final_capital, 2)
        result.peak_capital = round(peak_capital, 2)

        # Max drawdown from equity curve
        if result.equity_curve:
            max_dd = max((e["drawdown"] for e in result.equity_curve), default=0)
            result.max_drawdown_pct = round(max_dd, 2)
            result.max_drawdown_usd = round((max_dd / 100) * peak_capital, 2)

        # Long vs short breakdown
        longs = [t for t in trades if t.direction == "LONG"]
        shorts = [t for t in trades if t.direction == "SHORT"]
        long_wins = [t for t in longs if t.realized_pnl_usd > 0]
        short_wins = [t for t in shorts if t.realized_pnl_usd > 0]
        result.long_trades = len(longs)
        result.short_trades = len(shorts)
        result.long_win_rate = round(len(long_wins) / len(longs) * 100, 1) if longs else 0
        result.short_win_rate = round(len(short_wins) / len(shorts) * 100, 1) if shorts else 0

        # Avg holding time
        holding_hours = []
        for t in trades:
            if t.exit_time and t.entry_time:
                hrs = (t.exit_time - t.entry_time) / 1000 / 3600
                holding_hours.append(hrs)
        result.avg_holding_hours = round(sum(holding_hours) / len(holding_hours), 1) if holding_hours else 0

        # Sharpe (simplified: avg daily PnL / std)
        import statistics
        daily_returns = [d["pnl"] / initial_capital for d in result.daily_pnl if d["pnl"] != 0]
        if len(daily_returns) > 2:
            avg_r = statistics.mean(daily_returns)
            std_r = statistics.stdev(daily_returns)
            result.sharpe_ratio = round((avg_r / std_r) * (252 ** 0.5), 2) if std_r > 0 else 0

    def _downsample_equity(self, equity_curve: List[Dict], max_points: int = 500) -> List[Dict]:
        """Reduce equity curve to max_points for frontend performance."""
        n = len(equity_curve)
        if n <= max_points:
            return equity_curve
        step = n // max_points
        return equity_curve[::step]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _block(stats: Dict, reason: str):
    stats[reason] = stats.get(reason, 0) + 1


def _days_to_fomc(today: date) -> Optional[int]:
    future = [d for d in FOMC_DATES if d >= today]
    if not future:
        return None
    return (future[0] - today).days


def _get_time_label(hour_est: int) -> str:
    if hour_est in {0, 1}:
        return "WITCHING_HOUR"
    if hour_est in {9, 10, 11, 12, 13}:
        return "US_SESSION"
    if hour_est in {6, 7}:
        return "EU_OPEN"
    return "OTHER"
