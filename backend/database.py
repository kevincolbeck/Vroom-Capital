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
    peak_profit_pct: Mapped[float] = mapped_column(Float, default=0.0)
    zone: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    signal_strength: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    entry_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    exit_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ha_6h_color: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)
    ha_1h_color: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)
    funding_rate_at_entry: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
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


async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with AsyncSessionLocal() as session:
        from sqlalchemy import select
        result = await session.execute(select(BotState).where(BotState.id == 1))
        state = result.scalar_one_or_none()
        if not state:
            session.add(BotState(id=1, status=BotStatus.STOPPED))
            await session.commit()
