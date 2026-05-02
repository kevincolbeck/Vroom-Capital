from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import String, Float, Boolean, DateTime, Integer, Text, ForeignKey, Enum as SAEnum
from datetime import datetime
from typing import Optional, List
import enum
from backend.config import settings


engine = create_async_engine(settings.database_url, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class PositionSide(str, enum.Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class PositionStatus(str, enum.Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    LIQUIDATED = "LIQUIDATED"
    EMERGENCY_CLOSED = "EMERGENCY_CLOSED"


class BotStatus(str, enum.Enum):
    RUNNING = "RUNNING"
    STOPPED = "STOPPED"
    PAUSED = "PAUSED"
    ERROR = "ERROR"


class SignalStrength(str, enum.Enum):
    STRONG = "STRONG"
    WEAK = "WEAK"
    BLOCKED = "BLOCKED"


class Position(Base):
    __tablename__ = "positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    exchange_order_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    side: Mapped[PositionSide] = mapped_column(SAEnum(PositionSide))
    status: Mapped[PositionStatus] = mapped_column(SAEnum(PositionStatus), default=PositionStatus.OPEN)
    entry_price: Mapped[float] = mapped_column(Float)
    current_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    exit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    position_size_usd: Mapped[float] = mapped_column(Float)
    margin_used_usd: Mapped[float] = mapped_column(Float)
    leverage: Mapped[int] = mapped_column(Integer, default=86)
    liquidation_price: Mapped[float] = mapped_column(Float)
    unrealized_pnl_pct: Mapped[float] = mapped_column(Float, default=0.0)
    realized_pnl_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    realized_pnl_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fees_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    peak_profit_pct: Mapped[float] = mapped_column(Float, default=0.0)
    zone: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    signal_strength: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    entry_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    exit_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ha_6h_color: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)
    ha_1h_color: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)
    funding_rate_at_entry: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_target_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    mii_at_entry: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    confidence_score_at_entry: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    opened_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    is_copy_trade: Mapped[bool] = mapped_column(Boolean, default=False)

    copy_positions: Mapped[List["CopyPosition"]] = relationship("CopyPosition", back_populates="master_position")


class CopyTrader(Base):
    __tablename__ = "copy_traders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nickname: Mapped[str] = mapped_column(String(64))
    api_key: Mapped[str] = mapped_column(String(256))
    api_secret: Mapped[str] = mapped_column(String(256))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    position_size_override_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    leverage_override: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    max_position_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    copy_longs: Mapped[bool] = mapped_column(Boolean, default=True)
    copy_shorts: Mapped[bool] = mapped_column(Boolean, default=True)
    total_pnl_usd: Mapped[float] = mapped_column(Float, default=0.0)
    total_trades: Mapped[int] = mapped_column(Integer, default=0)
    win_trades: Mapped[int] = mapped_column(Integer, default=0)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    joined_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_active: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    copy_positions: Mapped[List["CopyPosition"]] = relationship("CopyPosition", back_populates="trader")


class CopyPosition(Base):
    __tablename__ = "copy_positions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    master_position_id: Mapped[int] = mapped_column(ForeignKey("positions.id"))
    trader_id: Mapped[int] = mapped_column(ForeignKey("copy_traders.id"))
    exchange_order_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    status: Mapped[PositionStatus] = mapped_column(SAEnum(PositionStatus), default=PositionStatus.OPEN)
    entry_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    exit_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    position_size_usd: Mapped[float] = mapped_column(Float)
    realized_pnl_usd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    opened_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    master_position: Mapped["Position"] = relationship("Position", back_populates="copy_positions")
    trader: Mapped["CopyTrader"] = relationship("CopyTrader", back_populates="copy_positions")


class BotLog(Base):
    __tablename__ = "bot_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    level: Mapped[str] = mapped_column(String(10))
    category: Mapped[str] = mapped_column(String(32))
    message: Mapped[str] = mapped_column(Text)
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class BotConfig(Base):
    __tablename__ = "bot_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    key: Mapped[str] = mapped_column(String(64), unique=True)
    value: Mapped[str] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class BotState(Base):
    __tablename__ = "bot_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    status: Mapped[BotStatus] = mapped_column(SAEnum(BotStatus), default=BotStatus.STOPPED)
    last_signal: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    last_signal_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    total_trades: Mapped[int] = mapped_column(Integer, default=0)
    winning_trades: Mapped[int] = mapped_column(Integer, default=0)
    total_pnl_usd: Mapped[float] = mapped_column(Float, default=0.0)
    uptime_start: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ZoneMemory(Base):
    __tablename__ = "zone_memory"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    zone_key: Mapped[str] = mapped_column(String(20))
    direction: Mapped[str] = mapped_column(String(5))
    signal_count: Mapped[int] = mapped_column(Integer, default=0)
    last_signal_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    cooldown_until: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class SignalTick(Base):
    """
    Full signal state at every tick where the bot determines a candidate direction.
    Captures both fired signals and blocked ones so we can train/backtest later.
    Only written when direction is not None (neutral ticks are excluded).
    """
    __tablename__ = "signal_ticks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    price: Mapped[float] = mapped_column(Float)

    # Signal outcome
    direction: Mapped[str] = mapped_column(String(5))          # LONG / SHORT
    should_trade: Mapped[bool] = mapped_column(Boolean, default=False)
    fired: Mapped[bool] = mapped_column(Boolean, default=False) # True if position was actually opened
    confidence_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    position_size_modifier: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    block_reasons: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON list

    # HA colors
    ha_6h_color: Mapped[Optional[str]] = mapped_column(String(7), nullable=True)
    ha_1h_color: Mapped[Optional[str]] = mapped_column(String(7), nullable=True)
    ha_3m_color: Mapped[Optional[str]] = mapped_column(String(7), nullable=True)
    ha_6h_trend: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    ha_6h_green_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ha_6h_red_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ha_1h_consecutive: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Hyblock signals
    mii: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    obi_direction: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    whale_sentiment: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    top_trader_sentiment: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    volume_delta_sentiment: Mapped[Optional[str]] = mapped_column(String(15), nullable=True)
    cascade_risk: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)

    # Liquidation clusters
    liq_above_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_below_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_target_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Funding
    funding_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    funding_sentiment: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)

    # Precision scalping signals (15m MII + exact liq levels + order flow ratios)
    mii_15m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_level_nearest_long_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_level_nearest_short_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_level_long_size: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_level_short_size: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    volume_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    buy_sell_count_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    cascade_direction: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    cvd: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    oi_delta_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # Liq levels — exact cascade trigger prices and size/count oscillators
    liq_long_cluster_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_short_cluster_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_levels_size_delta: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_levels_count_delta: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # Heatmap cluster sizes (BTC) to complement the existing pct columns
    liq_above_size: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_below_size: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # MII sustained bars — consecutive bars MII held above threshold (signal quality)
    mii_sustained_bars: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # WarriorAI-aligned HA scoring components
    ha_6h_body_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ha_1h_aligned_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    # Retail/global L/S positioning (contrarian signals)
    true_retail_long_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    global_accounts_long_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # Net long/short positioning delta
    net_ls_delta: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # Cumulative liquidation zone bias
    cumulative_liq_bias: Mapped[Optional[str]] = mapped_column(String(15), nullable=True)
    # Previous day structure (ABOVE_PDH / BETWEEN / BELOW_PDL)
    prev_day_structure: Mapped[Optional[str]] = mapped_column(String(15), nullable=True)
    # Nearest round number zone distance %
    round_number_dist_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # 4H compression flag
    is_compressed: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    # Gap 2 — 3m velocity toward liq target
    velocity_toward_target: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    velocity_pct_3m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    # Gap 3 — 3m HA momentum burst
    ha_3m_aligned_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ha_3m_expanding: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    # Spot/futures divergence
    cvd_spot: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    basis_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Walk-forward data collection — block stage, mode, score breakdown
    block_stage: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    mode: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    score_breakdown: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON array

    # Regime
    regime: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    regime_er: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    regime_vol_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Order book (from ob_state)
    ob_bid_wall_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ob_bid_wall_size_btc: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ob_ask_wall_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ob_ask_wall_size_btc: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ob_book_imbalance: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ob_blocking_wall_size_btc: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Trade flow (from trade_flow_state)
    tf_taker_buy_ratio_5m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tf_taker_buy_ratio_15m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tf_buy_volume_5m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tf_sell_volume_5m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Live liquidation stream (from live_liq_state)
    live_cascade_live: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    live_cascade_direction: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    live_hawkes_intensity: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    live_liq_rate_btc_min: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Funding trajectory
    funding_trajectory: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    funding_slope_per_min: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # 24h range distances
    dist_from_24h_high_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    dist_from_24h_low_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # 6h HA price levels (already on signal)
    ha_6h_high: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ha_6h_low: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ha_prev_6h_color: Mapped[Optional[str]] = mapped_column(String(7), nullable=True)


class SignalOutcome(Base):
    """Price outcomes for signal ticks — filled lazily as time horizons elapse."""
    __tablename__ = "signal_outcomes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    signal_tick_id: Mapped[int] = mapped_column(Integer, ForeignKey("signal_ticks.id"), unique=True, index=True)
    entry_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    direction: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)

    # Price at each horizon
    price_15m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    price_1h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    price_4h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    price_8h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    price_24h: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Return pct (positive = price went up)
    return_15m_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    return_1h_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    return_4h_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    return_8h_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    return_24h_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Was direction correct?
    correct_15m: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    correct_1h: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    correct_4h: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    correct_8h: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    correct_24h: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    # When each horizon was filled
    ts_filled_15m: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    ts_filled_1h: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    ts_filled_4h: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    ts_filled_8h: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    ts_filled_24h: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)


class MarketSnapshot(Base):
    """Full market state snapshot written every 60s for walk-forward backtesting."""
    __tablename__ = "market_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    price: Mapped[float] = mapped_column(Float)

    # HA state
    ha_1h_color: Mapped[Optional[str]] = mapped_column(String(7), nullable=True)
    ha_6h_color: Mapped[Optional[str]] = mapped_column(String(7), nullable=True)
    ha_3m_color: Mapped[Optional[str]] = mapped_column(String(7), nullable=True)
    ha_1h_aligned_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ha_6h_body_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Regime
    regime: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    regime_er: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    regime_vol_ratio: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Trade flow (from TradeFlowMonitor.get_live_state())
    tf_taker_buy_ratio_5m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tf_taker_buy_ratio_15m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tf_buy_volume_5m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tf_sell_volume_5m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tf_total_volume_5m: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tf_connected: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    # Order book (from OrderBookMonitor.get_live_state())
    ob_bid_wall_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ob_bid_wall_size_btc: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ob_bid_wall_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ob_ask_wall_price: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ob_ask_wall_size_btc: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ob_ask_wall_pct: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ob_book_imbalance: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    ob_synced: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    # Liquidation stream (from LiquidationStreamMonitor.get_live_state())
    liq_cascade_live: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    liq_cascade_direction: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    liq_rate_btc_min: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_long_btc_min: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_short_btc_min: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_hawkes_intensity: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    liq_accelerating: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    liq_connected: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    # Funding
    funding_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    funding_sentiment: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    funding_trajectory: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    funding_slope_per_min: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Hyblock key metrics
    mii: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    obi_direction: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    cascade_risk: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    whale_sentiment: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # SQLite migration: add new columns to existing tables if they don't exist
        from sqlalchemy import text
        new_position_cols = [
            ("liq_target_price", "REAL"),
            ("mii_at_entry", "REAL"),
            ("confidence_score_at_entry", "REAL"),
        ]
        for col_name, col_type in new_position_cols:
            try:
                await conn.execute(text(f"ALTER TABLE positions ADD COLUMN {col_name} {col_type}"))
            except Exception:
                pass  # column already exists
        new_signal_tick_cols = [
            ("mii_15m", "REAL"),
            ("liq_level_nearest_long_pct", "REAL"),
            ("liq_level_nearest_short_pct", "REAL"),
            ("liq_level_long_size", "REAL"),
            ("liq_level_short_size", "REAL"),
            ("volume_ratio", "REAL"),
            ("buy_sell_count_ratio", "REAL"),
            ("cascade_direction", "TEXT"),
            ("cvd", "REAL"),
            ("oi_delta_pct", "REAL"),
            ("liq_long_cluster_price", "REAL"),
            ("liq_short_cluster_price", "REAL"),
            ("liq_levels_size_delta", "REAL"),
            ("liq_levels_count_delta", "REAL"),
            ("liq_above_size", "REAL"),
            ("liq_below_size", "REAL"),
            ("mii_sustained_bars", "INTEGER"),
            ("ha_6h_body_pct", "REAL"),
            ("ha_1h_aligned_count", "INTEGER"),
            ("true_retail_long_pct", "REAL"),
            ("global_accounts_long_pct", "REAL"),
            ("net_ls_delta", "REAL"),
            ("cumulative_liq_bias", "TEXT"),
            ("prev_day_structure", "TEXT"),
            ("round_number_dist_pct", "REAL"),
            ("is_compressed", "INTEGER"),
            # Gap 2 — 3m velocity toward liq target
            ("velocity_toward_target", "INTEGER"),
            ("velocity_pct_3m", "REAL"),
            # Gap 3 — 3m HA momentum burst
            ("ha_3m_aligned_count", "INTEGER"),
            ("ha_3m_expanding", "INTEGER"),
            # Spot/futures divergence
            ("cvd_spot", "REAL"),
            ("basis_pct", "REAL"),
            # Walk-forward data collection
            ("block_stage", "TEXT"),
            ("mode", "TEXT"),
            ("score_breakdown", "TEXT"),
            ("regime", "TEXT"),
            ("regime_er", "REAL"),
            ("regime_vol_ratio", "REAL"),
            ("ob_bid_wall_pct", "REAL"),
            ("ob_bid_wall_size_btc", "REAL"),
            ("ob_ask_wall_pct", "REAL"),
            ("ob_ask_wall_size_btc", "REAL"),
            ("ob_book_imbalance", "REAL"),
            ("ob_blocking_wall_size_btc", "REAL"),
            ("tf_taker_buy_ratio_5m", "REAL"),
            ("tf_taker_buy_ratio_15m", "REAL"),
            ("tf_buy_volume_5m", "REAL"),
            ("tf_sell_volume_5m", "REAL"),
            ("live_cascade_live", "INTEGER"),
            ("live_cascade_direction", "TEXT"),
            ("live_hawkes_intensity", "REAL"),
            ("live_liq_rate_btc_min", "REAL"),
            ("funding_trajectory", "TEXT"),
            ("funding_slope_per_min", "REAL"),
            ("dist_from_24h_high_pct", "REAL"),
            ("dist_from_24h_low_pct", "REAL"),
            ("ha_6h_high", "REAL"),
            ("ha_6h_low", "REAL"),
            ("ha_prev_6h_color", "TEXT"),
        ]
        for col_name, col_type in new_signal_tick_cols:
            try:
                await conn.execute(text(f"ALTER TABLE signal_ticks ADD COLUMN {col_name} {col_type}"))
            except Exception:
                pass  # column already exists
    async with AsyncSessionLocal() as session:
        from sqlalchemy import select
        result = await session.execute(select(BotState).where(BotState.id == 1))
        state = result.scalar_one_or_none()
        if not state:
            session.add(BotState(id=1, status=BotStatus.STOPPED))
            await session.commit()
