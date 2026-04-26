"""
Backtest API Routes
"""
import asyncio
from typing import Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel
from loguru import logger

from backend.api.routes import verify_token
from backend.backtest.engine import BacktestEngine, BacktestConfig, BacktestResult
from backend.backtest.data_loader import clear_cache

router = APIRouter()

# ─── In-memory run state ───────────────────────────────────────────────────────
_current_run: Optional[BacktestEngine] = None
_last_result: Optional[Dict] = None
_progress: float = 0.0
_progress_msg: str = "Idle"
_is_running: bool = False


class BacktestRequest(BaseModel):
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


def _result_to_dict(result: BacktestResult) -> Dict:
    """Serialize a BacktestResult to a JSON-safe dict."""
    trades_data = []
    for t in result.trades:
        trades_data.append({
            "id": t.trade_id,
            "direction": t.direction,
            "entry_price": t.entry_price,
            "exit_price": t.exit_price,
            "entry_time": datetime.fromtimestamp(t.entry_time / 1000).isoformat(),
            "exit_time": datetime.fromtimestamp(t.exit_time / 1000).isoformat() if t.exit_time else None,
            "margin_usd": round(t.margin_usd, 2),
            "realized_pnl_pct": t.realized_pnl_pct,
            "realized_pnl_usd": t.realized_pnl_usd,
            "peak_profit_pct": round(t.peak_profit_pct, 2),
            "zone": t.zone,
            "exit_reason": t.exit_reason,
            "status": t.status,
            "ha_6h_color": t.ha_6h_color,
            "ha_1h_color": t.ha_1h_color,
            "funding_rate": round(t.funding_rate * 100, 4),
            "signal_score": t.signal_score,
            "time_label": t.time_label,
            "holding_hours": round((t.exit_time - t.entry_time) / 1000 / 3600, 1) if t.exit_time else None,
        })

    return {
        "summary": {
            "start_date": result.start_date,
            "end_date": result.end_date,
            "data_points": result.data_points,
            "initial_capital": result.config.initial_capital,
            "final_capital": result.final_capital,
            "peak_capital": result.peak_capital,
            "total_return_pct": result.total_return_pct,
            "total_pnl_usd": result.total_pnl_usd,
            "total_trades": result.total_trades,
            "winning_trades": result.winning_trades,
            "losing_trades": result.losing_trades,
            "liquidations": result.liquidations,
            "win_rate": result.win_rate,
            "avg_win_pct": result.avg_win_pct,
            "avg_loss_pct": result.avg_loss_pct,
            "avg_win_usd": result.avg_win_usd,
            "avg_loss_usd": result.avg_loss_usd,
            "profit_factor": result.profit_factor,
            "max_drawdown_pct": result.max_drawdown_pct,
            "max_drawdown_usd": result.max_drawdown_usd,
            "sharpe_ratio": result.sharpe_ratio,
            "best_trade_pct": result.best_trade_pct,
            "worst_trade_pct": result.worst_trade_pct,
            "avg_holding_hours": result.avg_holding_hours,
            "long_trades": result.long_trades,
            "short_trades": result.short_trades,
            "long_win_rate": result.long_win_rate,
            "short_win_rate": result.short_win_rate,
        },
        "config": {
            "start_year": result.config.start_year,
            "end_year": result.config.end_year,
            "leverage": result.config.leverage,
            "position_size_pct": result.config.position_size_pct,
            "liquidation_buffer_usd": result.config.liquidation_buffer_usd,
            "tp1_pct": result.config.tp1_pct,
            "tp2_pct": result.config.tp2_pct,
            "use_time_filter": result.config.use_time_filter,
            "use_velocity_filter": result.config.use_velocity_filter,
            "use_funding_filter": result.config.use_funding_filter,
            "use_macro_filter": result.config.use_macro_filter,
            "use_second_break_rule": result.config.use_second_break_rule,
        },
        "equity_curve": result.equity_curve,
        "daily_pnl": result.daily_pnl,
        "monthly_pnl": result.monthly_pnl,
        "block_stats": result.block_stats,
        "trades": trades_data,
    }


async def _run_backtest(engine: BacktestEngine, config: BacktestConfig):
    global _last_result, _is_running, _progress, _progress_msg

    def on_progress(pct: float, msg: str):
        global _progress, _progress_msg
        _progress = pct
        _progress_msg = msg

    try:
        result = await engine.run(progress_cb=on_progress)
        _last_result = _result_to_dict(result)
    except Exception as e:
        logger.exception(f"Backtest failed: {e}")
        _progress_msg = f"Failed: {e}"
    finally:
        _is_running = False
        _progress = 1.0


@router.post("/backtest/run")
async def start_backtest(
    req: BacktestRequest,
    background_tasks: BackgroundTasks,
    user: str = Depends(verify_token),
):
    global _current_run, _is_running, _progress, _progress_msg, _last_result

    if _is_running:
        raise HTTPException(400, "A backtest is already running. Wait for it to finish or cancel it.")

    if req.start_year < 2019 or req.end_year > 2025 or req.start_year >= req.end_year:
        raise HTTPException(400, "Invalid date range. Use start_year 2019-2024, end_year > start_year, max 2025.")

    config = BacktestConfig(
        start_year=req.start_year,
        end_year=req.end_year,
        initial_capital=req.initial_capital,
        leverage=req.leverage,
        position_size_pct=req.position_size_pct,
        liquidation_buffer_usd=req.liquidation_buffer_usd,
        tp1_pct=req.tp1_pct,
        tp2_pct=req.tp2_pct,
        velocity_threshold_pct=req.velocity_threshold_pct,
        velocity_window_hours=req.velocity_window_hours,
        zone_cooldown_minutes=req.zone_cooldown_minutes,
        emergency_candles=req.emergency_candles,
        fomc_caution_days=req.fomc_caution_days,
        use_time_filter=req.use_time_filter,
        use_velocity_filter=req.use_velocity_filter,
        use_funding_filter=req.use_funding_filter,
        use_macro_filter=req.use_macro_filter,
        use_zone_system=req.use_zone_system,
        use_second_break_rule=req.use_second_break_rule,
    )

    _current_run = BacktestEngine(config)
    _is_running = True
    _progress = 0.0
    _progress_msg = "Starting..."
    _last_result = None

    # Run in background
    background_tasks.add_task(_run_backtest, _current_run, config)

    return {"message": "Backtest started", "status": "running"}


@router.get("/backtest/status")
async def get_backtest_status(user: str = Depends(verify_token)):
    return {
        "running": _is_running,
        "progress": round(_progress * 100, 1),
        "message": _progress_msg,
        "has_results": _last_result is not None,
    }


@router.get("/backtest/results")
async def get_backtest_results(user: str = Depends(verify_token)):
    if _last_result is None:
        raise HTTPException(404, "No backtest results available. Run a backtest first.")
    return _last_result


@router.post("/backtest/cancel")
async def cancel_backtest(user: str = Depends(verify_token)):
    global _is_running
    if _current_run:
        _current_run.cancel()
    _is_running = False
    return {"message": "Backtest cancelled"}


@router.delete("/backtest/cache")
async def clear_backtest_cache(user: str = Depends(verify_token)):
    clear_cache()
    return {"message": "Cache cleared — next run will re-download data"}
