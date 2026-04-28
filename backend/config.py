from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional
import os


class Settings(BaseSettings):
    # App
    app_name: str = "Vroom Capital"
    debug: bool = False
    secret_key: str = Field(default="change-me-in-production-32-chars-min")
    admin_password: str = Field(default="admin123")

    # Database
    database_url: str = Field(default="sqlite+aiosqlite:///./legion_bot.db")

    # Bitunix API
    bitunix_api_key: str = Field(default="")
    bitunix_api_secret: str = Field(default="")
    bitunix_base_url: str = Field(default="https://fapi.bitunix.com")
    bitunix_ws_url: str = Field(default="wss://fstream.bitunix.com/stream")

    # External funding rate sources
    binance_base_url: str = Field(default="https://fapi.binance.com")
    okx_base_url: str = Field(default="https://www.okx.com")

    # Strategy defaults
    leverage: int = Field(default=75)
    position_size_pct: float = Field(default=0.30)
    liquidation_buffer_usd: float = Field(default=3250.0)
    tp1_pct: float = Field(default=0.20)
    tp2_pct: float = Field(default=0.30)
    velocity_threshold_pct: float = Field(default=1.5)
    velocity_window_hours: int = Field(default=2)
    zone_size_usd: float = Field(default=1000.0)
    zone_cooldown_minutes: int = Field(default=120)
    emergency_candles: int = Field(default=4)
    fomc_caution_days: int = Field(default=7)

    # Bot control
    bot_enabled: bool = Field(default=False)
    max_concurrent_positions: int = Field(default=1)
    copy_trading_enabled: bool = Field(default=True)

    # Trailing stop
    trailing_before_tp1_pct: Optional[float] = Field(default=None)
    trailing_after_tp1_peak_low_pct: float = Field(default=1.0)
    trailing_after_tp1_peak_high_pct: float = Field(default=5.0)
    trailing_peak_threshold_pct: float = Field(default=25.0)

    # Hyblock Capital API
    hyblock_api_key: str = Field(default="")
    hyblock_api_secret: str = Field(default="")
    hyblock_base_url: str = Field(default="https://api.hyblockcapital.com/v2")
    hyblock_confidence_threshold: float = Field(default=60.0)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()
