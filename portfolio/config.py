"""
Portfolio Management Configuration
Central configuration for the portfolio management system
"""

import os
from decimal import Decimal
from typing import Dict, Any, List
from pydantic import BaseSettings, Field


class PortfolioConfig(BaseSettings):
    """Portfolio management configuration"""
    
    # Database settings
    DATABASE_URL: str = Field(
        default="postgresql://user:password@localhost:5432/trading_db",
        env="PORTFOLIO_DATABASE_URL"
    )
    
    # Real-time tracking settings
    REAL_TIME_ENABLED: bool = Field(default=True, env="REAL_TIME_ENABLED")
    REAL_TIME_UPDATE_INTERVAL: int = Field(default=1, env="REAL_TIME_UPDATE_INTERVAL")  # seconds
    POSITION_UPDATE_INTERVAL: int = Field(default=60, env="POSITION_UPDATE_INTERVAL")  # seconds
    PERFORMANCE_UPDATE_INTERVAL: int = Field(default=300, env="PERFORMANCE_UPDATE_INTERVAL")  # seconds
    
    # Market data settings
    MARKET_DATA_SOURCES: List[str] = Field(
        default=["alpaca", "yahoo", "polygon"],
        env="MARKET_DATA_SOURCES"
    )
    MARKET_DATA_REFRESH_INTERVAL: int = Field(default=5, env="MARKET_DATA_REFRESH_INTERVAL")  # seconds
    
    # Risk management settings
    DEFAULT_RISK_FREE_RATE: Decimal = Field(default=Decimal("0.02"), env="DEFAULT_RISK_FREE_RATE")
    VAR_CONFIDENCE_LEVELS: List[float] = Field(default=[0.95, 0.99], env="VAR_CONFIDENCE_LEVELS")
    DEFAULT_LOOKBACK_DAYS: int = Field(default=252, env="DEFAULT_LOOKBACK_DAYS")
    
    # Performance calculation settings
    ANNUALIZATION_FACTOR: int = Field(default=252, env="ANNUALIZATION_FACTOR")  # trading days per year
    MIN_PERIODS_FOR_CALCULATION: int = Field(default=30, env="MIN_PERIODS_FOR_CALCULATION")
    
    # Portfolio settings
    DEFAULT_BASE_CURRENCY: str = Field(default="USD", env="DEFAULT_BASE_CURRENCY")
    DEFAULT_INITIAL_CAPITAL: Decimal = Field(default=Decimal("100000.00"), env="DEFAULT_INITIAL_CAPITAL")
    MAX_POSITIONS_PER_PORTFOLIO: int = Field(default=1000, env="MAX_POSITIONS_PER_PORTFOLIO")
    
    # API settings
    API_HOST: str = Field(default="0.0.0.0", env="API_HOST")
    API_PORT: int = Field(default=8000, env="API_PORT")
    API_RELOAD: bool = Field(default=False, env="API_RELOAD")
    
    # WebSocket settings
    WEBSOCKET_ENABLED: bool = Field(default=True, env="WEBSOCKET_ENABLED")
    WEBSOCKET_PORT: int = Field(default=8001, env="WEBSOCKET_PORT")
    
    # Trading platform integration
    TRADING_PLATFORM_ENABLED: bool = Field(default=True, env="TRADING_PLATFORM_ENABLED")
    SYNC_INTERVAL: int = Field(default=30, env="SYNC_INTERVAL")  # seconds
    AUTO_SYNC_ENABLED: bool = Field(default=True, env="AUTO_SYNC_ENABLED")
    
    # Logging settings
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")
    LOG_FORMAT: str = Field(
        default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        env="LOG_FORMAT"
    )
    
    # Cache settings
    REDIS_URL: str = Field(default="redis://localhost:6379", env="REDIS_URL")
    CACHE_ENABLED: bool = Field(default=False, env="CACHE_ENABLED")
    CACHE_TTL: int = Field(default=300, env="CACHE_TTL")  # seconds
    
    # Email notifications (for alerts)
    EMAIL_ENABLED: bool = Field(default=False, env="EMAIL_ENABLED")
    SMTP_HOST: str = Field(default="", env="SMTP_HOST")
    SMTP_PORT: int = Field(default=587, env="SMTP_PORT")
    SMTP_USERNAME: str = Field(default="", env="SMTP_USERNAME")
    SMTP_PASSWORD: str = Field(default="", env="SMTP_PASSWORD")
    
    # Alert thresholds
    ALERT_THRESHOLDS: Dict[str, Any] = {
        "max_drawdown": 0.10,  # 10%
        "var_breach": 0.05,    # 5%
        "position_concentration": 0.25,  # 25% of portfolio in single position
        "daily_loss_limit": 0.05  # 5% daily loss
    }
    
    # Performance benchmarks
    DEFAULT_BENCHMARKS: List[str] = Field(
        default=["SPY", "QQQ", "IWM"],
        env="DEFAULT_BENCHMARKS"
    )
    
    class Config:
        env_file = ".env"
        case_sensitive = True


class MarketDataConfig(BaseSettings):
    """Market data specific configuration"""
    
    # Alpaca settings
    ALPACA_API_KEY: str = Field(default="", env="ALPACA_API_KEY")
    ALPACA_SECRET_KEY: str = Field(default="", env="ALPACA_SECRET_KEY")
    ALPACA_BASE_URL: str = Field(default="https://paper-api.alpaca.markets", env="ALPACA_BASE_URL")
    
    # Yahoo Finance settings
    YAHOO_FINANCE_ENABLED: bool = Field(default=True, env="YAHOO_FINANCE_ENABLED")
    
    # Polygon settings
    POLYGON_API_KEY: str = Field(default="", env="POLYGON_API_KEY")
    
    # IEX Cloud settings
    IEX_API_KEY: str = Field(default="", env="IEX_API_KEY")
    
    class Config:
        env_file = ".env"
        case_sensitive = True


class DatabaseConfig(BaseSettings):
    """Database specific configuration"""
    
    DB_HOST: str = Field(default="localhost", env="DB_HOST")
    DB_PORT: int = Field(default=5432, env="DB_PORT")
    DB_NAME: str = Field(default="trading_db", env="DB_NAME")
    DB_USER: str = Field(default="user", env="DB_USER")
    DB_PASSWORD: str = Field(default="password", env="DB_PASSWORD")
    
    # Connection pool settings
    DB_POOL_SIZE: int = Field(default=10, env="DB_POOL_SIZE")
    DB_MAX_OVERFLOW: int = Field(default=20, env="DB_MAX_OVERFLOW")
    DB_POOL_TIMEOUT: int = Field(default=30, env="DB_POOL_TIMEOUT")
    
    # TimescaleDB settings
    TIMESCALEDB_ENABLED: bool = Field(default=True, env="TIMESCALEDB_ENABLED")
    RETENTION_PERIOD: str = Field(default="5 years", env="RETENTION_PERIOD")
    
    @property
    def database_url(self) -> str:
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Global configuration instances
portfolio_config = PortfolioConfig()
market_data_config = MarketDataConfig()
database_config = DatabaseConfig()


def get_portfolio_config() -> PortfolioConfig:
    """Get portfolio configuration"""
    return portfolio_config


def get_market_data_config() -> MarketDataConfig:
    """Get market data configuration"""
    return market_data_config


def get_database_config() -> DatabaseConfig:
    """Get database configuration"""
    return database_config


# Configuration validation
def validate_config():
    """Validate configuration settings"""
    errors = []
    
    # Validate database configuration
    if not database_config.DB_HOST:
        errors.append("Database host not configured")
    
    if not database_config.DB_NAME:
        errors.append("Database name not configured")
    
    # Validate market data configuration
    if not market_data_config.ALPACA_API_KEY and "alpaca" in portfolio_config.MARKET_DATA_SOURCES:
        errors.append("Alpaca API key required when Alpaca is enabled")
    
    # Validate thresholds
    for threshold_name, threshold_value in portfolio_config.ALERT_THRESHOLDS.items():
        if not isinstance(threshold_value, (int, float)) or threshold_value < 0:
            errors.append(f"Invalid alert threshold for {threshold_name}: {threshold_value}")
    
    if errors:
        raise ValueError(f"Configuration validation failed: {', '.join(errors)}")
    
    return True


# Environment-specific configurations
def get_development_config() -> PortfolioConfig:
    """Get development environment configuration"""
    config = PortfolioConfig()
    config.API_RELOAD = True
    config.LOG_LEVEL = "DEBUG"
    config.REAL_TIME_ENABLED = False  # Disable in development
    return config


def get_production_config() -> PortfolioConfig:
    """Get production environment configuration"""
    config = PortfolioConfig()
    config.API_RELOAD = False
    config.LOG_LEVEL = "INFO"
    config.REAL_TIME_ENABLED = True
    return config


def get_testing_config() -> PortfolioConfig:
    """Get testing environment configuration"""
    config = PortfolioConfig()
    config.DATABASE_URL = "postgresql://test:test@localhost:5432/test_trading_db"
    config.REAL_TIME_ENABLED = False
    config.LOG_LEVEL = "WARNING"
    return config