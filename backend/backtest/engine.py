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
    download_klines, download_funding_rates, download_3m_klines,
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
    # Optional custom date range — overrides start_year/end_year if set
    start_date: object = None  # datetime with tzinfo
    end_date: object = None    # datetime with tzinfo
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
    # Trailing stop: 1% trail after TP1, widens to 5% after TP2
    trailing_peak_threshold_pct: float = 25.0   # unused by simulation — kept for API compat
    trailing_after_tp1_peak_low_pct: float = 1.0   # trail % before TP2
    trailing_after_tp1_peak_high_pct: float = 5.0  # trail % after TP2
    # Realistic execution costs
    taker_fee_pct: float = 0.0005   # 0.05% per side (Bitunix taker, confirmed from live trade)
    slippage_pct: float = 0.0001    # 0.01% per side (conservative estimate)
    charge_funding: bool = True     # deduct funding payments every 8h during hold


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
    max_adverse_pct: float = 0.0  # worst intra-trade leveraged PnL (negative = against us)
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
            # ─── Step 1: Download 1H data ───────────────────────────
            _progress(0.0, "Downloading historical data...")
            from datetime import datetime, timezone

            start_dt = self.config.start_date or datetime(self.config.start_year, 1, 1, tzinfo=timezone.utc)
            end_dt   = self.config.end_date   or datetime(self.config.end_year + 1, 1, 1, tzinfo=timezone.utc)

            candles_1h = await download_klines("1h", start_dt, end_dt,
                lambda pct, msg: _progress(pct * 0.22, msg))
            _progress(0.22, "Building 6H candles...")
            candles_6h = build_6h_from_1h(candles_1h)

            _progress(0.24, "Downloading funding rates...")
            funding_rates = await download_funding_rates(start_dt, end_dt,
                lambda pct, msg: _progress(0.24 + pct * 0.04, msg))

            # ─── Step 2: Download 3M data (Binance futures) ─────────
            _progress(0.28, "Downloading 3M candles (Binance futures)...")
            candles_3m = await download_3m_klines(start_dt, end_dt,
                lambda pct, msg: _progress(0.28 + pct * 0.09, msg))

            _progress(0.37, f"Loaded {len(candles_1h)} 1H + {len(candles_6h)} 6H + {len(candles_3m)} 3M candles. Running simulation...")
            result.data_points = len(candles_1h)
            result.start_date = datetime.fromtimestamp(candles_1h[0]["open_time"] / 1000).strftime("%Y-%m-%d")
            result.end_date = datetime.fromtimestamp(candles_1h[-1]["open_time"] / 1000).strftime("%Y-%m-%d")

            # ─── Step 3: Run simulation ─────────────────────────────
            await self._simulate(candles_1h, candles_6h, candles_3m, funding_rates, result, _progress)

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
        candles_3m: List[Dict],
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
        # Index 3M candles by their 1H open_time bucket for fast lookup
        idx_3m_by_hour: Dict[int, List[Dict]] = {}
        for c in candles_3m:
            hour_key = (c["open_time"] // 3_600_000) * 3_600_000
            if hour_key not in idx_3m_by_hour:
                idx_3m_by_hour[hour_key] = []
            idx_3m_by_hour[hour_key].append(c)

        FUNDING_INTERVAL_MS = 8 * 3_600_000
        last_funding_period: Optional[int] = None

        idx_6h = 0
        n_6h = len(candles_6h)
        n_1h = len(candles_1h)

        MAINT_RATE = 0.005
        MAX_NOTIONAL = 20_000_000.0

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

            if i % 500 == 0:
                pct = 0.37 + (i / n_1h) * 0.60
                progress_cb(pct, f"Simulating {dt.strftime('%Y-%m')} | Capital: ${capital:.0f} | Trades: {len(result.trades)}")
                await asyncio.sleep(0)

            # Fill 6H buffer — only add a 6H candle once its LAST 1H has closed.
            # A 6H candle at bucket B closes when the 1H at B+5h closes (i.e. B+5h <= ts_ms).
            # build_6h_from_1h pre-builds complete candles, so we just gate on close time
            # to prevent lookahead (the 6H candle contains all 6 1H data including future ones).
            while idx_6h < n_6h and candles_6h[idx_6h]["open_time"] + 5 * 3_600_000 <= ts_ms:
                buf_6h.append(candles_6h[idx_6h])
                idx_6h += 1

            buf_1h.append(candle_1h)

            if len(buf_1h) < 10 or len(buf_6h) < 4:
                equity_curve.append({"time": ts_ms, "equity": capital, "drawdown": 0.0})
                prev_price = close_price
                continue

            buf_list_1h = list(buf_1h)
            buf_list_6h = list(buf_6h)

            # ─── 3M inner loop — price tracking only ───────────────
            # Entry/exit decisions use 1H close. 3M OHLC is used here
            # for accurate liquidation, peak/MAE, and trailing stop.
            for c3m in idx_3m_by_hour.get(ts_ms, []):
                if current_trade is None:
                    continue

                # ── Funding charge every 8h UTC boundary ──────────────
                if cfg.charge_funding:
                    this_period = c3m["open_time"] // FUNDING_INTERVAL_MS
                    if last_funding_period is None:
                        last_funding_period = this_period
                    elif this_period > last_funding_period:
                        fr = get_funding_at_time(funding_rates, c3m["open_time"])
                        notional = current_trade.position_size_usd
                        cost = notional * fr if current_trade.direction == "LONG" else notional * (-fr)
                        capital -= cost * (this_period - last_funding_period)
                        last_funding_period = this_period

                # ── Update peak and MAE using 3M high/low ─────────────
                entry = current_trade.entry_price
                lev = cfg.leverage
                if current_trade.direction == "LONG":
                    intra_best  = (c3m["high"] - entry) / entry * 100.0 * lev
                    intra_worst = (c3m["low"]  - entry) / entry * 100.0 * lev
                    cur_pnl_pct = (c3m["close"] - entry) / entry * 100.0 * lev
                else:
                    intra_best  = (entry - c3m["low"])  / entry * 100.0 * lev
                    intra_worst = (entry - c3m["high"]) / entry * 100.0 * lev
                    cur_pnl_pct = (entry - c3m["close"]) / entry * 100.0 * lev

                if intra_best > current_trade.peak_profit_pct:
                    current_trade.peak_profit_pct = intra_best
                if intra_worst < current_trade.max_adverse_pct:
                    current_trade.max_adverse_pct = intra_worst

                # ── Liquidation check on 3M OHLC ──────────────────────
                liq_hit = False
                if current_trade.direction == "LONG" and c3m["low"] <= current_trade.liquidation_price:
                    self._close_trade(current_trade, current_trade.liquidation_price, c3m["open_time"], "Liquidated")
                    current_trade.status = "LIQUIDATED"
                    capital += current_trade.realized_pnl_usd
                    result.trades.append(current_trade)
                    current_trade = None
                    last_funding_period = None
                    liq_hit = True
                elif current_trade.direction == "SHORT" and c3m["high"] >= current_trade.liquidation_price:
                    self._close_trade(current_trade, current_trade.liquidation_price, c3m["open_time"], "Liquidated")
                    current_trade.status = "LIQUIDATED"
                    capital += current_trade.realized_pnl_usd
                    result.trades.append(current_trade)
                    current_trade = None
                    last_funding_period = None
                    liq_hit = True

                if liq_hit:
                    continue

                peak_pnl = current_trade.peak_profit_pct
                tp1_pct  = cfg.tp1_pct * 100   # 20.0
                tp2_pct  = cfg.tp2_pct * 100   # 30.0

                # ── Trailing stop at 3M resolution ────────────────────
                # TP1 (20%) hit → 1% trail; TP2 (30%) hit → 5% trail
                if peak_pnl >= tp1_pct:
                    trail_pct = (cfg.trailing_after_tp1_peak_high_pct if peak_pnl >= tp2_pct
                                 else cfg.trailing_after_tp1_peak_low_pct)
                    drawdown_from_peak = peak_pnl - cur_pnl_pct
                    if drawdown_from_peak >= trail_pct:
                        trail_stop_pnl = peak_pnl - trail_pct
                        if current_trade.direction == "LONG":
                            trail_px = entry * (1 + trail_stop_pnl / (100.0 * lev))
                            exit_px = max(trail_px, c3m["low"])
                        else:
                            trail_px = entry * (1 - trail_stop_pnl / (100.0 * lev))
                            exit_px = min(trail_px, c3m["high"])
                        self._close_trade(current_trade, exit_px, c3m["open_time"],
                            f"Trailing stop (3M): peak={peak_pnl:.1f}% drawdown={drawdown_from_peak:.1f}%")
                        capital += current_trade.realized_pnl_usd
                        result.trades.append(current_trade)
                        current_trade = None
                        last_funding_period = None
                        continue

            # ─── 1H-level exits (emergency candles, 6H reversal) ───
            if current_trade is not None:
                current_trade, capital, closed = self._update_position(
                    current_trade, candle_1h, buf_list_1h, buf_list_6h, capital
                )
                if closed:
                    result.trades.append(current_trade)
                    current_trade = None
                    last_funding_period = None

            # ─── 1H entry at candle close ──────────────────────────
            if current_trade is None:
                signal = self._generate_signal(
                    buf_list_1h, buf_list_6h, close_price, dt, ts_ms,
                    funding_rates, zone_tracker, first_breaks, prev_price,
                    macro, cfg, block_stats
                )
                if signal and capital >= 10:
                    size_mod = signal["size_mod"]
                    # Apply entry slippage adversely
                    if signal["direction"] == "LONG":
                        entry_px = close_price * (1 + cfg.slippage_pct)
                    else:
                        entry_px = close_price * (1 - cfg.slippage_pct)
                    # 30% of capital as margin; notional = margin × leverage
                    margin = capital * size_mod * cfg.position_size_pct
                    pos_size = min(margin * cfg.leverage, MAX_NOTIONAL)
                    margin = pos_size / cfg.leverage  # recalculate if notional was capped
                    qty = pos_size / entry_px
                    # Cross-margin liq: liq_dist = entry × (account / notional - MAINT_RATE)
                    # = entry × (1/(size_mod × position_size_pct × leverage) - MAINT_RATE)
                    liq_dist = entry_px * max(
                        1.0 / (size_mod * cfg.position_size_pct * cfg.leverage) - MAINT_RATE, 0.001
                    )
                    liq_px = (entry_px - liq_dist if signal["direction"] == "LONG"
                              else entry_px + liq_dist)
                    if margin >= 5.0:
                        trade_counter += 1
                        current_trade = SimTrade(
                            trade_id=trade_counter,
                            direction=signal["direction"],
                            entry_price=entry_px,
                            entry_time=ts_ms,
                            margin_usd=margin,
                            position_size_usd=pos_size,
                            leverage=cfg.leverage,
                            liquidation_price=liq_px,
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
                        last_funding_period = None

            prev_price = close_price

            # ─── Track equity ───────────────────────────────────────
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

        # 1H GREEN + 6H GREEN → LONG; 1H RED + 6H RED → SHORT (no trend confirmation required)
        if ha_1h_color == "GREEN" and ha_6h_color == "GREEN":
            direction = "LONG"
        elif ha_1h_color == "RED" and ha_6h_color == "RED":
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
        1H-only exit checks. Liquidation, peak tracking, trailing stop, and
        3M HA exits are handled at 3M resolution in the inner loop.
        This handles only signals that are inherently 1H/6H based:
        - Emergency close: 4 consecutive opposing 1H HA candles
        - 6H HA reversal
        """
        cfg = self.config
        close = candle["close"]
        ts_ms = candle["open_time"]

        # ─── Emergency candles ─────────────────────────────────────
        ha_1h = compute_heikin_ashi(buf_1h[-20:])
        consecutive = count_consecutive_opposite(ha_1h, trade.direction)
        if consecutive >= cfg.emergency_candles:
            self._close_trade(trade, close, ts_ms,
                f"Emergency: {consecutive} consecutive opposing 1H candles")
            capital += trade.realized_pnl_usd
            return trade, capital, True

        # ─── 6H reversal ──────────────────────────────────────────
        ha_6h = compute_heikin_ashi(buf_6h[-10:])
        if detect_reversal(ha_6h, trade.direction):
            self._close_trade(trade, close, ts_ms, "6H HA reversal detected")
            capital += trade.realized_pnl_usd
            return trade, capital, True

        return trade, capital, False

    def _close_trade(self, trade: SimTrade, exit_price: float, exit_time: int, reason: str, capital: float = None):
        """Finalize a trade. Applies exit slippage and deducts round-trip fees."""
        cfg = self.config
        is_liq = "Liquidat" in reason

        # Apply exit slippage adversely (not on liquidations — price is fixed)
        if not is_liq:
            if trade.direction == "LONG":
                exit_price = exit_price * (1 - cfg.slippage_pct)
            else:
                exit_price = exit_price * (1 + cfg.slippage_pct)

        qty = trade.position_size_usd / trade.entry_price
        if trade.direction == "LONG":
            pnl_usd = qty * (exit_price - trade.entry_price)
        else:
            pnl_usd = qty * (trade.entry_price - exit_price)

        # Deduct round-trip taker fees (entry notional + exit notional)
        exit_notional = qty * exit_price
        pnl_usd -= (trade.position_size_usd + exit_notional) * cfg.taker_fee_pct

        pnl_pct = (pnl_usd / trade.margin_usd * 100) if trade.margin_usd > 0 else 0.0

        trade.exit_price = exit_price
        trade.exit_time = exit_time
        trade.exit_reason = reason
        trade.realized_pnl_pct = round(pnl_pct, 2)
        trade.realized_pnl_usd = round(pnl_usd, 2)
        trade.status = "CLOSED" if not is_liq else "LIQUIDATED"

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
