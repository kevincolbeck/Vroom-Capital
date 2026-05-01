"""
FastAPI Routes — All REST API endpoints for the admin dashboard.
"""
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, desc, func, update
from pydantic import BaseModel
import json
import asyncio
import jwt
from loguru import logger

from backend.database import (
    get_db, Position, CopyTrader, CopyPosition, BotLog, BotState, BotConfig,
    PositionStatus, BotStatus, ZoneMemory
)
from backend.bot_engine import get_bot_engine
from backend.exchange.bitunix import get_bitunix_client
from backend.strategy.signal_engine import SignalEngine
from backend.strategy.time_filter import get_time_context
from backend.strategy.macro_calendar import MacroCalendar
from backend.strategy.funding_rate import FundingRateMonitor
from backend.strategy.order_flow import SpotOrderFlowMonitor
from backend.strategy.hyblock import HyblockMonitor
from backend.config import settings

router = APIRouter()
security = HTTPBearer()

# ─── WebSocket connection manager ────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data: dict):
        disconnected = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                disconnected.append(ws)
        for ws in disconnected:
            self.disconnect(ws)


ws_manager = ConnectionManager()

# Shared monitor instances — retain order book cache across requests
_order_flow_monitor = SpotOrderFlowMonitor()
_hyblock_monitor = HyblockMonitor()


# ─── Auth ──────────────────────────────────────────────────────────────────

def create_token(user: str = "admin") -> str:
    payload = {
        "sub": user,
        "exp": datetime.utcnow() + timedelta(days=7),
    }
    return jwt.encode(payload, settings.secret_key, algorithm="HS256")


def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    try:
        payload = jwt.decode(credentials.credentials, settings.secret_key, algorithms=["HS256"])
        return payload["sub"]
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


# ─── Auth Routes ──────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    password: str

@router.post("/auth/login")
async def login(req: LoginRequest):
    if req.password != settings.admin_password:
        raise HTTPException(status_code=401, detail="Invalid password")
    token = create_token()
    return {"token": token, "type": "bearer"}


# ─── Bot Control Routes ──────────────────────────────────────────────────────

@router.get("/bot/status")
async def get_bot_status(
    db: AsyncSession = Depends(get_db),
    user: str = Depends(verify_token)
):
    engine = get_bot_engine()
    engine_status = engine.get_status()

    result = await db.execute(select(BotState).where(BotState.id == 1))
    state = result.scalar_one_or_none()

    # Get account balance
    try:
        client = get_bitunix_client()
        balance_data = await client.get_account_balance()
        ticker = await client.get_ticker()
    except Exception:
        balance_data = {"balance": 0, "available": 0, "unrealized_pnl": 0}
        ticker = {"price": 0, "funding_rate": 0}

    # Get open positions count
    pos_result = await db.execute(
        select(func.count(Position.id)).where(Position.status == PositionStatus.OPEN)
    )
    open_count = pos_result.scalar()

    return {
        "bot": {
            "status": state.status if state else "STOPPED",
            "running": engine_status["running"],
            "paused": engine_status["paused"],
            "manual_override": engine_status["manual_override"],
            "uptime_start": state.uptime_start.isoformat() if state and state.uptime_start else None,
            "error_message": state.error_message if state else None,
            "total_trades": state.total_trades if state else 0,
            "winning_trades": state.winning_trades if state else 0,
            "total_pnl_usd": state.total_pnl_usd if state else 0.0,
            "win_rate": round((state.winning_trades / state.total_trades * 100) if state and state.total_trades > 0 else 0, 1),
        },
        "market": {
            "btc_price": ticker.get("price", 0),
            "funding_rate": ticker.get("funding_rate", 0),
            "mark_price": ticker.get("mark_price", 0),
        },
        "account": balance_data,
        "open_positions": open_count,
        "last_signal": engine_status.get("last_signal"),
        "copy_trading_enabled": settings.copy_trading_enabled,
    }


@router.post("/bot/start")
async def start_bot(user: str = Depends(verify_token)):
    engine = get_bot_engine()
    if engine.is_running:
        if engine.is_paused:
            await engine.resume()
            return {"message": "Bot resumed", "status": "RUNNING"}
        return {"message": "Bot is already running", "status": "RUNNING"}
    await engine.start()
    return {"message": "Bot started", "status": "RUNNING"}


@router.post("/bot/stop")
async def stop_bot(user: str = Depends(verify_token)):
    engine = get_bot_engine()
    await engine.stop()
    return {"message": "Bot stopped", "status": "STOPPED"}


@router.post("/bot/pause")
async def pause_bot(user: str = Depends(verify_token)):
    engine = get_bot_engine()
    await engine.pause()
    return {"message": "Bot paused", "status": "PAUSED"}


@router.post("/bot/resume")
async def resume_bot(user: str = Depends(verify_token)):
    engine = get_bot_engine()
    await engine.resume()
    return {"message": "Bot resumed", "status": "RUNNING"}


class EmergencyCloseRequest(BaseModel):
    reason: str = "Emergency stop by admin"

@router.post("/bot/emergency-close")
async def emergency_close(
    req: EmergencyCloseRequest,
    user: str = Depends(verify_token)
):
    engine = get_bot_engine()
    await engine.emergency_close_all(req.reason)
    return {"message": "Emergency close executed", "reason": req.reason}


class ForceTradeRequest(BaseModel):
    direction: str  # LONG or SHORT
    reason: str = "Manual override"

@router.post("/bot/force-trade")
async def force_trade(
    req: ForceTradeRequest,
    user: str = Depends(verify_token)
):
    if req.direction not in ("LONG", "SHORT"):
        raise HTTPException(400, "Direction must be LONG or SHORT")
    engine = get_bot_engine()
    success = await engine.force_open(req.direction, req.reason)
    return {"success": success, "direction": req.direction}


# ─── Signal / Analysis Routes ────────────────────────────────────────────────

@router.get("/signal/current")
async def get_current_signal(user: str = Depends(verify_token)):
    engine = get_bot_engine()
    status = engine.get_status()
    return {"signal": status.get("last_signal"), "timestamp": datetime.utcnow().isoformat()}


@router.get("/signal/analysis")
async def get_full_analysis(user: str = Depends(verify_token)):
    """Run a fresh signal analysis and return all components."""
    try:
        client = get_bitunix_client()
        ticker = await client.get_ticker()
        candles_1h = await client.get_klines("1h", limit=100)
        candles_6h = await client.get_klines("6h", limit=50)

        engine = SignalEngine()
        signal = await engine.generate_signal(candles_1h, candles_6h, ticker["price"])

        # Add additional context
        time_ctx = get_time_context()
        macro = MacroCalendar().get_macro_context()
        funding_monitor = FundingRateMonitor()
        funding_rates = await funding_monitor.fetch_all()
        funding_analysis = funding_monitor.analyze_funding(funding_rates)

        return {
            "signal": signal.to_dict(),
            "time_context": time_ctx,
            "macro_context": macro,
            "funding_analysis": funding_analysis,
            "btc_price": ticker["price"],
        }
    except Exception as e:
        raise HTTPException(500, str(e))


# ─── Position Routes ─────────────────────────────────────────────────────────

@router.get("/positions")
async def get_positions(
    status: Optional[str] = Query(None),
    limit: int = Query(50, le=500),
    offset: int = Query(0),
    db: AsyncSession = Depends(get_db),
    user: str = Depends(verify_token),
):
    query = select(Position).order_by(desc(Position.opened_at)).limit(limit).offset(offset)
    if status:
        query = query.where(Position.status == status)
    result = await db.execute(query)
    positions = result.scalars().all()

    # Fetch live price once so open-position P&L is always fresh
    live_price: Optional[float] = None
    try:
        client = get_bitunix_client()
        ticker = await client.get_ticker()
        live_price = ticker.get("price")
    except Exception:
        pass

    data = []
    for p in positions:
        # For open positions recompute P&L from live price; fall back to DB value
        if p.status == PositionStatus.OPEN and live_price:
            lev = p.leverage or settings.leverage
            if p.side == "LONG":
                pnl_pct = (live_price - p.entry_price) / p.entry_price * lev * 100
            else:
                pnl_pct = (p.entry_price - live_price) / p.entry_price * lev * 100
            pnl_pct = round(pnl_pct, 2)
            current_price = live_price
        else:
            pnl_pct = p.unrealized_pnl_pct
            current_price = p.current_price

        pnl_usd = round((p.margin_used_usd or 0) * pnl_pct / 100, 2)

        # Use actual fees from exchange reconciliation when available;
        # fall back to estimated 0.12% round-trip of notional
        if p.fees_usd is not None:
            fees_usd = round(p.fees_usd, 4)
        else:
            fees_usd = round((p.position_size_usd or 0) * 0.0012, 4)
        if p.status == PositionStatus.OPEN:
            net_pnl_usd = round(pnl_usd - fees_usd, 4)
        else:
            net_pnl_usd = round((p.realized_pnl_usd or 0) - fees_usd, 4)

        data.append({
            "id": p.id,
            "side": p.side,
            "status": p.status,
            "entry_price": p.entry_price,
            "current_price": current_price,
            "exit_price": p.exit_price,
            "position_size_usd": p.position_size_usd,
            "margin_used_usd": p.margin_used_usd,
            "leverage": p.leverage,
            "liquidation_price": p.liquidation_price,
            "unrealized_pnl_pct": pnl_pct,
            "unrealized_pnl_usd": pnl_usd,
            "realized_pnl_pct": p.realized_pnl_pct,
            "realized_pnl_usd": p.realized_pnl_usd,
            "fees_usd": fees_usd,
            "net_pnl_usd": net_pnl_usd,
            "peak_profit_pct": p.peak_profit_pct,
            "zone": p.zone,
            "signal_strength": p.signal_strength,
            "entry_reason": p.entry_reason,
            "exit_reason": p.exit_reason,
            "ha_6h_color": p.ha_6h_color,
            "ha_1h_color": p.ha_1h_color,
            "funding_rate_at_entry": p.funding_rate_at_entry,
            "opened_at": p.opened_at.isoformat(),
            "closed_at": p.closed_at.isoformat() if p.closed_at else None,
        })

    return {"positions": data, "total": len(data)}


@router.post("/positions/{position_id}/close")
async def close_position(
    position_id: int,
    db: AsyncSession = Depends(get_db),
    user: str = Depends(verify_token),
):
    result = await db.execute(select(Position).where(Position.id == position_id))
    position = result.scalar_one_or_none()
    if not position:
        raise HTTPException(404, "Position not found")
    if position.status != PositionStatus.OPEN:
        raise HTTPException(400, "Position is not open")

    client = get_bitunix_client()
    from backend.trading.position_manager import PositionManager
    from backend.copy_trading.manager import CopyTradingManager

    pos_manager = PositionManager(client, db)
    copy_manager = CopyTradingManager(db)

    try:
        ticker = await client.get_ticker()
        current_price = ticker["price"]
    except Exception:
        current_price = position.entry_price

    await pos_manager.close_position(position, current_price, "Manual close by admin")
    await copy_manager.close_copy_positions(position, "Manual close by admin")

    return {"message": f"Position #{position_id} closed", "exit_price": current_price}


# ─── Copy Trading Routes ─────────────────────────────────────────────────────

class AddTraderRequest(BaseModel):
    nickname: str
    api_key: str
    api_secret: str
    position_size_pct: Optional[float] = None
    leverage_override: Optional[int] = None
    max_position_usd: Optional[float] = None
    copy_longs: bool = True
    copy_shorts: bool = True
    notes: Optional[str] = None

@router.post("/copy-traders")
async def add_copy_trader(
    req: AddTraderRequest,
    db: AsyncSession = Depends(get_db),
    user: str = Depends(verify_token),
):
    trader = CopyTrader(
        nickname=req.nickname,
        api_key=req.api_key,
        api_secret=req.api_secret,
        position_size_override_pct=req.position_size_pct,
        leverage_override=req.leverage_override,
        max_position_usd=req.max_position_usd,
        copy_longs=req.copy_longs,
        copy_shorts=req.copy_shorts,
        notes=req.notes,
        is_active=True,
    )
    db.add(trader)
    await db.commit()
    await db.refresh(trader)
    return {"message": "Trader added", "id": trader.id}


@router.get("/copy-traders")
async def get_copy_traders(
    db: AsyncSession = Depends(get_db),
    user: str = Depends(verify_token),
):
    result = await db.execute(select(CopyTrader).order_by(desc(CopyTrader.joined_at)))
    traders = result.scalars().all()

    data = []
    for t in traders:
        data.append({
            "id": t.id,
            "nickname": t.nickname,
            "is_active": t.is_active,
            "position_size_pct": t.position_size_override_pct,
            "leverage_override": t.leverage_override,
            "max_position_usd": t.max_position_usd,
            "copy_longs": t.copy_longs,
            "copy_shorts": t.copy_shorts,
            "total_pnl_usd": t.total_pnl_usd,
            "total_trades": t.total_trades,
            "win_trades": t.win_trades,
            "win_rate": round(t.win_trades / t.total_trades * 100 if t.total_trades > 0 else 0, 1),
            "notes": t.notes,
            "joined_at": t.joined_at.isoformat(),
            "last_active": t.last_active.isoformat() if t.last_active else None,
        })
    return {"traders": data}


class UpdateTraderRequest(BaseModel):
    nickname: Optional[str] = None
    is_active: Optional[bool] = None
    position_size_pct: Optional[float] = None
    leverage_override: Optional[int] = None
    max_position_usd: Optional[float] = None
    copy_longs: Optional[bool] = None
    copy_shorts: Optional[bool] = None
    notes: Optional[str] = None

@router.patch("/copy-traders/{trader_id}")
async def update_copy_trader(
    trader_id: int,
    req: UpdateTraderRequest,
    db: AsyncSession = Depends(get_db),
    user: str = Depends(verify_token),
):
    result = await db.execute(select(CopyTrader).where(CopyTrader.id == trader_id))
    trader = result.scalar_one_or_none()
    if not trader:
        raise HTTPException(404, "Trader not found")

    if req.nickname is not None:
        trader.nickname = req.nickname
    if req.is_active is not None:
        trader.is_active = req.is_active
    if req.position_size_pct is not None:
        trader.position_size_override_pct = req.position_size_pct
    if req.leverage_override is not None:
        trader.leverage_override = req.leverage_override
    if req.max_position_usd is not None:
        trader.max_position_usd = req.max_position_usd
    if req.copy_longs is not None:
        trader.copy_longs = req.copy_longs
    if req.copy_shorts is not None:
        trader.copy_shorts = req.copy_shorts
    if req.notes is not None:
        trader.notes = req.notes

    await db.commit()
    return {"message": "Trader updated"}


@router.delete("/copy-traders/{trader_id}")
async def delete_copy_trader(
    trader_id: int,
    db: AsyncSession = Depends(get_db),
    user: str = Depends(verify_token),
):
    result = await db.execute(select(CopyTrader).where(CopyTrader.id == trader_id))
    trader = result.scalar_one_or_none()
    if not trader:
        raise HTTPException(404, "Trader not found")
    await db.delete(trader)
    await db.commit()
    return {"message": "Trader deleted"}


@router.get("/copy-positions")
async def get_copy_positions(
    trader_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    user: str = Depends(verify_token),
):
    query = select(CopyPosition).order_by(desc(CopyPosition.opened_at)).limit(100)
    if trader_id:
        query = query.where(CopyPosition.trader_id == trader_id)
    result = await db.execute(query)
    positions = result.scalars().all()

    data = []
    for p in positions:
        data.append({
            "id": p.id,
            "master_position_id": p.master_position_id,
            "trader_id": p.trader_id,
            "status": p.status,
            "entry_price": p.entry_price,
            "exit_price": p.exit_price,
            "position_size_usd": p.position_size_usd,
            "realized_pnl_usd": p.realized_pnl_usd,
            "error_message": p.error_message,
            "opened_at": p.opened_at.isoformat() if p.opened_at else None,
            "closed_at": p.closed_at.isoformat() if p.closed_at else None,
        })
    return {"copy_positions": data}


# ─── Settings Routes ─────────────────────────────────────────────────────────

@router.get("/settings")
async def get_settings(user: str = Depends(verify_token)):
    return {
        "leverage": settings.leverage,
        "position_size_pct": settings.position_size_pct,
        "liquidation_buffer_usd": settings.liquidation_buffer_usd,
        "tp1_pct": settings.tp1_pct,
        "tp2_pct": settings.tp2_pct,
        "velocity_threshold_pct": settings.velocity_threshold_pct,
        "velocity_window_hours": settings.velocity_window_hours,
        "zone_size_usd": settings.zone_size_usd,
        "zone_cooldown_minutes": settings.zone_cooldown_minutes,
        "emergency_candles": settings.emergency_candles,
        "fomc_caution_days": settings.fomc_caution_days,
        "max_concurrent_positions": settings.max_concurrent_positions,
        "copy_trading_enabled": settings.copy_trading_enabled,
        "has_api_key": bool(settings.bitunix_api_key),
    }


class UpdateSettingsRequest(BaseModel):
    leverage: Optional[int] = None
    position_size_pct: Optional[float] = None
    liquidation_buffer_usd: Optional[float] = None
    tp1_pct: Optional[float] = None
    tp2_pct: Optional[float] = None
    velocity_threshold_pct: Optional[float] = None
    velocity_window_hours: Optional[int] = None
    zone_cooldown_minutes: Optional[int] = None
    max_concurrent_positions: Optional[int] = None
    copy_trading_enabled: Optional[bool] = None
    bitunix_api_key: Optional[str] = None
    bitunix_api_secret: Optional[str] = None
    admin_password: Optional[str] = None

@router.patch("/settings")
async def update_settings(
    req: UpdateSettingsRequest,
    db: AsyncSession = Depends(get_db),
    user: str = Depends(verify_token),
):
    updated = []
    data = req.model_dump(exclude_none=True)
    for key, value in data.items():
        if hasattr(settings, key):
            object.__setattr__(settings, key, value)
            updated.append(key)

            # Persist to env file
            await _save_config(db, key, str(value))

    return {"message": f"Updated: {', '.join(updated)}", "updated": updated}


async def _save_config(db: AsyncSession, key: str, value: str):
    result = await db.execute(select(BotConfig).where(BotConfig.key == key))
    existing = result.scalar_one_or_none()
    if existing:
        existing.value = value
        existing.updated_at = datetime.utcnow()
    else:
        db.add(BotConfig(key=key, value=value))
    await db.commit()


# ─── Analytics Routes ────────────────────────────────────────────────────────

@router.get("/analytics/summary")
async def get_analytics_summary(
    days: int = Query(30, le=365),
    db: AsyncSession = Depends(get_db),
    user: str = Depends(verify_token),
):
    since = datetime.utcnow() - timedelta(days=days)
    result = await db.execute(
        select(Position).where(
            Position.status != PositionStatus.OPEN,
            Position.opened_at >= since,
        )
    )
    positions = result.scalars().all()

    if not positions:
        return {"message": "No trades in this period", "data": {}}

    total_trades = len(positions)
    wins = [p for p in positions if (p.realized_pnl_pct or 0) > 0]
    losses = [p for p in positions if (p.realized_pnl_pct or 0) <= 0]
    liquidations = [p for p in positions if p.status == PositionStatus.LIQUIDATED]

    total_pnl = sum(p.realized_pnl_usd or 0 for p in positions)
    avg_win = sum(p.realized_pnl_pct or 0 for p in wins) / len(wins) if wins else 0
    avg_loss = sum(p.realized_pnl_pct or 0 for p in losses) / len(losses) if losses else 0
    best_trade = max(positions, key=lambda p: p.realized_pnl_pct or -999)
    worst_trade = min(positions, key=lambda p: p.realized_pnl_pct or 999)

    # Daily P&L
    daily_pnl: Dict[str, float] = {}
    for p in positions:
        day = p.closed_at.strftime("%Y-%m-%d") if p.closed_at else p.opened_at.strftime("%Y-%m-%d")
        daily_pnl[day] = daily_pnl.get(day, 0) + (p.realized_pnl_usd or 0)

    # Signal distribution
    longs = [p for p in positions if p.side == "LONG"]
    shorts = [p for p in positions if p.side == "SHORT"]

    return {
        "period_days": days,
        "total_trades": total_trades,
        "winning_trades": len(wins),
        "losing_trades": len(losses),
        "liquidations": len(liquidations),
        "win_rate": round(len(wins) / total_trades * 100, 1),
        "total_pnl_usd": round(total_pnl, 2),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "profit_factor": round(abs(avg_win / avg_loss) if avg_loss != 0 else 0, 2),
        "best_trade_pct": round(best_trade.realized_pnl_pct or 0, 2),
        "worst_trade_pct": round(worst_trade.realized_pnl_pct or 0, 2),
        "long_trades": len(longs),
        "short_trades": len(shorts),
        "daily_pnl": [{"date": k, "pnl": round(v, 2)} for k, v in sorted(daily_pnl.items())],
    }


# ─── Logs Routes ────────────────────────────────────────────────────────────

@router.get("/logs")
async def get_logs(
    level: Optional[str] = None,
    category: Optional[str] = None,
    limit: int = Query(100, le=500),
    offset: int = Query(0),
    db: AsyncSession = Depends(get_db),
    user: str = Depends(verify_token),
):
    query = select(BotLog).order_by(desc(BotLog.created_at)).limit(limit).offset(offset)
    if level:
        query = query.where(BotLog.level == level.upper())
    if category:
        query = query.where(BotLog.category == category.upper())

    result = await db.execute(query)
    logs = result.scalars().all()

    return {
        "logs": [
            {
                "id": l.id,
                "level": l.level,
                "category": l.category,
                "message": l.message,
                "details": l.details,
                "created_at": l.created_at.isoformat(),
            }
            for l in logs
        ]
    }


@router.delete("/logs")
async def clear_logs(
    db: AsyncSession = Depends(get_db),
    user: str = Depends(verify_token),
):
    await db.execute(BotLog.__table__.delete())
    await db.commit()
    return {"message": "Logs cleared"}


# ─── Market Data Routes ──────────────────────────────────────────────────────

@router.get("/market/ticker")
async def get_ticker(user: str = Depends(verify_token)):
    client = get_bitunix_client()
    ticker = await client.get_ticker()
    return ticker


@router.get("/market/context")
async def get_market_context(user: str = Depends(verify_token)):
    time_ctx = get_time_context()
    macro = MacroCalendar().get_macro_context()
    funding_monitor = FundingRateMonitor()

    # Fetch ticker for current price (needed for order book pressure window)
    try:
        client = get_bitunix_client()
        ticker = await client.get_ticker()
        current_price = ticker.get("price", 0.0)
    except Exception:
        current_price = 0.0

    funding_rates, spot_flow = await asyncio.gather(
        funding_monitor.fetch_all(),
        _order_flow_monitor.fetch_all(current_price) if current_price else asyncio.sleep(0),
        return_exceptions=True,
    )

    if isinstance(funding_rates, Exception):
        funding_rates = {}
    funding = funding_monitor.analyze_funding(funding_rates)

    spot_flow_data = spot_flow if isinstance(spot_flow, dict) else {"available": False}

    return {
        "time": time_ctx,
        "macro": macro,
        "funding": funding,
        "spot_flow": spot_flow_data,
    }


# ─── Hyblock Data ────────────────────────────────────────────────────────────

@router.get("/hyblock/data")
async def get_hyblock_data(user: str = Depends(verify_token)):
    """Return latest Hyblock Capital signal data (cached 60s)."""
    try:
        client = get_bitunix_client()
        ticker = await client.get_ticker()
        current_price = ticker.get("price", 0.0)
    except Exception:
        current_price = 0.0

    data = await _hyblock_monitor.fetch_all(current_price)
    return data


# ─── WebSocket ───────────────────────────────────────────────────────────────

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            engine = get_bot_engine()
            client = get_bitunix_client()

            try:
                ticker = await client.get_ticker()
            except Exception:
                ticker = {}

            data = {
                "type": "update",
                "timestamp": datetime.utcnow().isoformat(),
                "bot_status": engine.get_status(),
                "market": ticker,
            }
            await websocket.send_json(data)
            await asyncio.sleep(5)  # Push updates every 5s
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)
