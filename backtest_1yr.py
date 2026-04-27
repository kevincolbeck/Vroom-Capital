"""
Backtest — Jan 1 2026 → Apr 27 2026
Mirrors the current live strategy exactly:
  - 1H GREEN + 6H GREEN (no trend confirmation required)
  - Direct entry at 1H candle close (sniper timing, no 3M primed entry)
  - 30% of capital as margin, notional = margin × 75x leverage
  - Liq price via standard cross-margin formula (~0.83% from entry at 75x)
  - Time filter, velocity, funding, macro, zone cooldown (2nd signal), second-break rule
  - TP1 at 20%, TP2 at 30%, trailing stop (1% trail after TP1, 5% after TP2)
  - 4 emergency candles exit, 6H reversal exit (no 3M exit)
  - Fees: 0.05% per side | Slippage: 0.01% | Funding every 8h
  - 6H candle lookahead bias eliminated (only closed 6H candles used for signals)

Run: python backtest_1yr.py
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import types
mock_settings = types.SimpleNamespace(
    leverage=75,
    position_size_pct=0.30,
    liquidation_buffer_usd=3250.0,
    tp1_pct=0.20,
    tp2_pct=0.30,
    velocity_threshold_pct=1.5,
    velocity_window_hours=2,
    zone_size_usd=1000.0,
    zone_cooldown_minutes=120,
    emergency_candles=4,
    fomc_caution_days=7,
)
import backend.config as _cfg_mod
_cfg_mod.settings = mock_settings

from backend.backtest.engine import BacktestEngine, BacktestConfig
from datetime import datetime, timezone


async def main():
    config = BacktestConfig(
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2026, 4, 27, tzinfo=timezone.utc),
        initial_capital=1_000.0,
        leverage=75,
        position_size_pct=0.30,
        tp1_pct=0.20,
        tp2_pct=0.30,
        velocity_threshold_pct=1.5,
        velocity_window_hours=2,
        zone_cooldown_minutes=120,
        emergency_candles=4,
        fomc_caution_days=7,
        use_time_filter=True,
        use_velocity_filter=True,
        use_funding_filter=True,
        use_macro_filter=True,
        use_zone_system=True,
        use_second_break_rule=True,
        trailing_peak_threshold_pct=25.0,
        trailing_after_tp1_peak_low_pct=1.0,
        trailing_after_tp1_peak_high_pct=5.0,
        taker_fee_pct=0.0005,
        slippage_pct=0.0001,
        charge_funding=True,
    )

    print("\n" + "=" * 72)
    print("  VROOM CAPITAL — BACKTEST  |  Jan 1 2026 → Apr 27 2026  |  $1k start  |  75x")
    print("  Strategy: 1H HA + 6H HA (direct entry)  |  30% margin  |  w/ fees+slippage")
    print("=" * 72)

    engine = BacktestEngine(config)
    shown = set()

    def on_progress(pct, msg):
        bucket = int(pct * 20)
        if bucket not in shown:
            shown.add(bucket)
            print(f"  [{pct*100:4.0f}%] {msg}", flush=True)

    r = await engine.run(progress_cb=on_progress)

    print("\n" + "=" * 72)
    print(f"  Period:          {r.start_date}  →  {r.end_date}")
    print(f"  Final Capital:   ${r.final_capital:>14,.2f}")
    print(f"  Total Return:    {r.total_return_pct:>+.1f}%")
    print(f"  Total Trades:    {r.total_trades}")
    print(f"  Win Rate:        {r.win_rate:.1f}%  ({r.winning_trades}W / {r.losing_trades}L)")
    print(f"  Liquidations:    {r.liquidations}")
    print(f"  Max Drawdown:    {r.max_drawdown_pct:.1f}%  (${r.max_drawdown_usd:,.0f})")
    print(f"  Sharpe Ratio:    {r.sharpe_ratio:.2f}")
    print(f"  Profit Factor:   {r.profit_factor:.2f}")
    print(f"  Avg Win:         ${r.avg_win_usd:,.2f}")
    print(f"  Avg Loss:        ${r.avg_loss_usd:,.2f}")
    print(f"  Long trades:     {r.long_trades}  (win rate: {r.long_win_rate:.1f}%)")
    print(f"  Short trades:    {r.short_trades}  (win rate: {r.short_win_rate:.1f}%)")
    print(f"  Avg hold:        {r.avg_holding_hours:.1f}h")
    print("=" * 72)

    if r.monthly_pnl:
        print("\n  Monthly P&L:")
        for m in r.monthly_pnl:
            bar = "█" * int(abs(m["pnl"]) / max(abs(x["pnl"]) for x in r.monthly_pnl) * 20) if r.monthly_pnl else ""
            sign = "+" if m["pnl"] >= 0 else ""
            print(f"    {m['month']}  {sign}${m['pnl']:>9,.0f}  {bar}")

    if r.block_stats:
        print("\n  Signal blocks:")
        for reason, count in sorted(r.block_stats.items(), key=lambda x: -x[1]):
            print(f"    {reason:<30} {count:>6}")

    # Trade detail
    closed = [t for t in r.trades if t.status != "OPEN"]
    winners = [t for t in closed if t.realized_pnl_usd > 0]
    losers  = [t for t in closed if t.realized_pnl_usd <= 0]

    if r.trades:
        print("\n  Individual trades:")
        print(f"  {'#':<4} {'Dir':<6} {'Entry':<12} {'Exit':<12} {'PnL%':<10} {'PnL$':<10} {'Peak%':<10} {'MAE%':<10} {'Exit reason'}")
        print(f"  {'-'*100}")
        for t in closed:
            entry_dt = datetime.fromtimestamp(t.entry_time/1000, tz=timezone.utc).strftime("%m/%d %H:%M")
            pnl_sign = "+" if t.realized_pnl_usd >= 0 else ""
            print(f"  {t.trade_id:<4} {t.direction:<6} {t.entry_price:<12,.0f} {t.exit_price:<12,.0f} "
                  f"{pnl_sign}{t.realized_pnl_pct:>+.1f}%     {pnl_sign}${t.realized_pnl_usd:>7,.2f}   "
                  f"{t.peak_profit_pct:>+.1f}%     {t.max_adverse_pct:>+.1f}%     {t.exit_reason[:40]}")

    if winners or losers:
        stop_thresholds = [-25, -50, -75, -100, -150, -200, -300]
        print("\n  MAE analysis — winners that would be stopped at each level:")
        print(f"  {'Stop (lev%)':<14} {'Winners stopped':<18} {'False stop rate'}")
        print(f"  {'-'*50}")
        for thresh in stop_thresholds:
            knocked = [t for t in winners if t.max_adverse_pct <= thresh]
            pct = len(knocked) / len(winners) * 100 if winners else 0
            print(f"  {thresh:>+4}% lev        {len(knocked):>4} / {len(winners):<6}       {pct:>5.1f}%")

        print(f"\n  Loser MAE distribution:")
        for thresh in stop_thresholds:
            hit = [t for t in losers if t.max_adverse_pct <= thresh]
            pct = len(hit) / len(losers) * 100 if losers else 0
            print(f"  {thresh:>+4}% lev        {len(hit):>4} / {len(losers):<6} losers    {pct:>5.1f}% would have been cut")

        if winners:
            print(f"\n  Avg MAE — winners: {sum(t.max_adverse_pct for t in winners)/len(winners):.1f}%", end="")
        if losers:
            print(f"  |  losers: {sum(t.max_adverse_pct for t in losers)/len(losers):.1f}%")
        print()


if __name__ == "__main__":
    asyncio.run(main())
