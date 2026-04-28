"""
1-Year Backtest — April 2024 → April 2025
Mirrors the current live strategy as closely as possible:
  - 1H GREEN + 6H GREEN + 6H trend confirmed (2/3 same color)
  - Time filter, velocity, funding, macro, zone cooldown, second-break rule
  - $3,250 liquidation buffer, 75x leverage
  - TP1 at 20%, TP2 at 30%, trailing stop
  - 4 emergency candles exit, 6H reversal exit

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


async def main():
    config = BacktestConfig(
        start_year=2020,
        end_year=2024,   # engine uses start_year → end_year+1 Jan 1
        initial_capital=10_000.0,
        leverage=75,
        position_size_pct=0.30,
        liquidation_buffer_usd=3250.0,
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
        # Trailing stop thresholds
        trailing_peak_threshold_pct=25.0,
        trailing_after_tp1_peak_low_pct=1.0,
        trailing_after_tp1_peak_high_pct=5.0,
    )

    print("\n" + "=" * 72)
    print("  VROOM CAPITAL — 5-YEAR BACKTEST  |  2020-2024  |  $10k start  |  75x")
    print("  Strategy: 1H HA + 6H HA + 6H trend confirmed  |  $3,250 liq buffer")
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
            color = "" if m["pnl"] >= 0 else ""
            print(f"    {m['month']}  {sign}${m['pnl']:>9,.0f}  {bar}")

    if r.block_stats:
        print("\n  Signal blocks:")
        for reason, count in sorted(r.block_stats.items(), key=lambda x: -x[1]):
            print(f"    {reason:<30} {count:>6}")

    print()


if __name__ == "__main__":
    asyncio.run(main())
