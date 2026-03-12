from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    CLOB_WS_URL: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    ASSETS: list[str] = ["BTC", "ETH", "SOL", "XRP"]
    HORIZONS: list[str] = ["daily", "4h", "hourly", "15m", "5m"]
    DB_PATH: str = "storage/poly.db"
    TICK_BATCH_SIZE: int = 100
    TICK_FLUSH_INTERVAL: float = 5.0
    LOG_LEVEL: str = "INFO"
    BINANCE_WS_URL: str = "wss://stream.binance.com:9443/stream"
    MARKET_DISCOVERY_INTERVAL: int = 300
    BINANCE_FLUSH_BATCH: int = 200
    BINANCE_FLUSH_INTERVAL: float = 2.0
    BACKTEST_DATA_DIR: str = ""
    BACKTEST_EXPORT_INTERVAL: float = 60.0
    BACKTEST_LOOKBACK_SECONDS: int = 3600

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()
