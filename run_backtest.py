"""
Standalone backtest runner — no FastAPI needed.
Run: python run_backtest.py
"""
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

# Force UTF-8 output on Windows to avoid cp1252 encoding errors
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# Patch settings so we don't need pydantic_settings installed
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
from datetime import datetime


def fmt(n, prefix="$", decimals=2):
    if n is None:
        return "—"
    return f"{prefix}{n:,.{decimals}f}" if prefix == "$" else f"{n:.{decimals}f}{prefix}"


async def main():
    print("\n" + "=" * 60)
    print("  VROOM CAPITAL — 5-YEAR BACKTEST (2020–2024)")
    print("  Strategy: 75x cross | $4,500 liq buffer")
    print("=" * 60 + "\n")

    config = BacktestConfig(
        start_year=2020,
        end_year=2024,
        initial_capital=10000.0,
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
    )

    engine = BacktestEngine(config)

    last_msg = [""]
    def on_progress(pct, msg):
        if msg != last_msg[0]:
            print(f"  [{pct*100:5.1f}%] {msg}")
            last_msg[0] = msg

    result = await engine.run(progress_cb=on_progress)

    r = result
    print("\n" + "=" * 60)
    print("  RESULTS")
    print("=" * 60)
    print(f"\n  Period:           {r.start_date} to {r.end_date}")
    print(f"  Data points:      {r.data_points:,} hourly candles\n")

    print(f"  ── Capital ──────────────────────────────────")
    print(f"  Starting:         ${config.initial_capital:,.2f}")
    print(f"  Final:            ${r.final_capital:,.2f}")
    print(f"  Peak:             ${r.peak_capital:,.2f}")
    print(f"  Total Return:     {r.total_return_pct:+.1f}%")
    print(f"  Total P&L:        ${r.total_pnl_usd:+,.2f}\n")

    print(f"  ── Trades ───────────────────────────────────")
    print(f"  Total trades:     {r.total_trades}")
    print(f"  Winners:          {r.winning_trades}  ({r.win_rate:.1f}% win rate)")
    print(f"  Losers:           {r.losing_trades}")
    print(f"  LIQUIDATIONS:     {r.liquidations}  ({'HIGH' if r.liquidations > 5 else 'LOW'})")
    print(f"  Long trades:      {r.long_trades}  ({r.long_win_rate:.1f}% win rate)")
    print(f"  Short trades:     {r.short_trades}  ({r.short_win_rate:.1f}% win rate)\n")

    print(f"  ── Per-Trade Stats ──────────────────────────")
    print(f"  Avg win:          +{r.avg_win_pct:.1f}% (${r.avg_win_usd:.2f})")
    print(f"  Avg loss:         {r.avg_loss_pct:.1f}% (${r.avg_loss_usd:.2f})")
    print(f"  Best trade:       +{r.best_trade_pct:.1f}%")
    print(f"  Worst trade:      {r.worst_trade_pct:.1f}%")
    print(f"  Avg hold time:    {r.avg_holding_hours:.1f} hours\n")

    print(f"  ── Risk Metrics ─────────────────────────────")
    print(f"  Profit factor:    {r.profit_factor:.2f}x")
    print(f"  Sharpe ratio:     {r.sharpe_ratio:.2f}")
    print(f"  Max drawdown:     -{r.max_drawdown_pct:.1f}% (${r.max_drawdown_usd:.0f})\n")

    # Signal block breakdown
    if result.block_stats:
        print(f"  ── Signal Filters (blocked signals) ─────────")
        total_blocked = sum(result.block_stats.values())
        for k, v in sorted(result.block_stats.items(), key=lambda x: -x[1])[:10]:
            print(f"  {k:<35} {v:>6,}  ({v/total_blocked*100:.1f}%)")
        print(f"  {'TOTAL BLOCKED':<35} {total_blocked:>6,}")
        print(f"  {'TRADES TAKEN':<35} {r.total_trades:>6,}\n")

    # Monthly breakdown
    print(f"  ── Monthly P&L ──────────────────────────────")
    for m in result.monthly_pnl:
        bar_len = int(abs(m['pnl']) / max(abs(x['pnl']) for x in result.monthly_pnl) * 20)
        bar = ('█' * bar_len) if m['pnl'] >= 0 else ('░' * bar_len)
        sign = '+' if m['pnl'] >= 0 else '-'
        print(f"  {m['month']}  {sign}${abs(m['pnl']):>8.2f}  {bar}")

    # Show worst liquidations if any
    liqs = [t for t in result.trades if t.status == 'LIQUIDATED']
    if liqs:
        print(f"\n  ── Liquidation Details ──────────────────────")
        for t in liqs:
            dt = datetime.fromtimestamp(t.entry_time / 1000)
            print(f"  {dt.strftime('%Y-%m-%d %H:%M')}  {t.direction:<5}  entry=${t.entry_price:,.0f}  liq=${t.liquidation_price:,.0f}  loss=${abs(t.realized_pnl_usd):.2f}")

    # --- Losing trade breakdown by exit reason ---
    losers = [t for t in result.trades if t.realized_pnl_usd < 0 and t.status != 'LIQUIDATED']
    if losers:
        from collections import Counter
        # Bucket exit reasons
        def bucket(reason):
            r = reason.lower()
            if 'emergency' in r:  return 'Emergency (4 consec HA candles)'
            if 'reversal' in r:   return '6H HA reversal'
            if 'trailing' in r:   return 'Trailing stop (post-TP1)'
            if 'tp1 protection' in r: return 'TP1 protection pullback'
            if 'end of backtest' in r: return 'End of backtest (open)'
            return reason[:50]
        counts = Counter(bucket(t.exit_reason) for t in losers)
        print(f"\n  ── Losing Trade Exit Reasons ({len(losers)} trades) ──────")
        for reason, cnt in counts.most_common():
            pct = cnt / len(losers) * 100
            avg_loss = sum(t.realized_pnl_usd for t in losers if bucket(t.exit_reason) == reason) / cnt
            print(f"  {reason:<40} {cnt:>4} trades  avg ${avg_loss:+.0f}")

        # Show avg BTC price move on losers vs winners
        winners = [t for t in result.trades if t.realized_pnl_usd > 0]
        avg_win_move = sum(abs(t.exit_price - t.entry_price) for t in winners) / len(winners) if winners else 0
        avg_loss_move = sum(abs(t.exit_price - t.entry_price) for t in losers) / len(losers) if losers else 0
        print(f"\n  Avg BTC move on winners: ${avg_win_move:,.0f}")
        print(f"  Avg BTC move on losers:  ${avg_loss_move:,.0f}")

        # Direction breakdown on losers
        long_losers  = [t for t in losers if t.direction == 'LONG']
        short_losers = [t for t in losers if t.direction == 'SHORT']
        print(f"\n  Long  losers: {len(long_losers)}  |  Short losers: {len(short_losers)}")

        # Show the 10 biggest losers
        worst = sorted(losers, key=lambda t: t.realized_pnl_usd)[:10]
        print(f"\n  ── 10 Biggest Losses ────────────────────────")
        for t in worst:
            dt = datetime.fromtimestamp(t.entry_time / 1000)
            hold_h = (t.exit_time - t.entry_time) / 1000 / 3600
            print(f"  {dt.strftime('%Y-%m-%d %H:%M')}  {t.direction:<5}  "
                  f"entry=${t.entry_price:>7,.0f}  exit=${t.exit_price:>7,.0f}  "
                  f"move=${t.exit_price-t.entry_price:>+7,.0f}  "
                  f"hold={hold_h:.1f}h  pnl=${t.realized_pnl_usd:+,.0f}  [{t.exit_reason[:35]}]")

    print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    asyncio.run(main())
