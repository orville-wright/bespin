"""
Portfolio Management Models
Core data models for the portfolio management system
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, List, Dict, Any
from enum import Enum
from dataclasses import dataclass, field
from pydantic import BaseModel, Field, validator
import pandas as pd
import numpy as np


class InstrumentType(str, Enum):
    STOCK = "stock"
    OPTION = "option"
    FUTURE = "future"
    CRYPTO = "crypto"
    BOND = "bond"
    FX = "fx"
    ETF = "etf"
    MUTUAL_FUND = "mutual_fund"


class TransactionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"
    SPLIT = "SPLIT"
    INTEREST = "INTEREST"
    FEE = "FEE"


class AttributionType(str, Enum):
    STOCK_SELECTION = "stock_selection"
    SECTOR_ALLOCATION = "sector_allocation"
    CURRENCY = "currency"
    TIMING = "timing"
    ASSET_ALLOCATION = "asset_allocation"


@dataclass
class Instrument:
    """Represents a tradeable instrument"""
    id: Optional[int] = None
    symbol: str = ""
    instrument_type: InstrumentType = InstrumentType.STOCK
    exchange: Optional[str] = None
    sector: Optional[str] = None
    currency: str = "USD"
    multiplier: Decimal = Decimal("1.0")
    tick_size: Decimal = Decimal("0.01")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.updated_at is None:
            self.updated_at = datetime.now(timezone.utc)


@dataclass
class Portfolio:
    """Represents a trading portfolio"""
    id: Optional[int] = None
    name: str = ""
    description: str = ""
    base_currency: str = "USD"
    initial_capital: Decimal = Decimal("0.0")
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.updated_at is None:
            self.updated_at = datetime.now(timezone.utc)


@dataclass
class Position:
    """Represents a position in a portfolio"""
    timestamp: datetime
    portfolio_id: int
    instrument_id: int
    quantity: Decimal
    average_cost: Decimal
    market_value: Optional[Decimal] = None
    unrealized_pnl: Optional[Decimal] = None
    realized_pnl: Decimal = Decimal("0.0")
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.market_value is None:
            self.market_value = self.quantity * self.average_cost
        if self.unrealized_pnl is None:
            self.unrealized_pnl = self.market_value - (self.quantity * self.average_cost)

    @property
    def total_pnl(self) -> Decimal:
        """Total P&L (realized + unrealized)"""
        return self.realized_pnl + (self.unrealized_pnl or Decimal("0.0"))

    @property
    def return_pct(self) -> Decimal:
        """Return percentage"""
        cost_basis = self.quantity * self.average_cost
        if cost_basis == 0:
            return Decimal("0.0")
        return (self.total_pnl / cost_basis) * 100


@dataclass
class Transaction:
    """Represents a portfolio transaction"""
    id: Optional[int] = None
    portfolio_id: int = 0
    instrument_id: int = 0
    transaction_type: TransactionType = TransactionType.BUY
    quantity: Decimal = Decimal("0.0")
    price: Decimal = Decimal("0.0")
    total_amount: Decimal = Decimal("0.0")
    fees: Decimal = Decimal("0.0")
    commission: Decimal = Decimal("0.0")
    execution_time: Optional[datetime] = None
    order_id: Optional[str] = None
    strategy_name: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if self.execution_time is None:
            self.execution_time = datetime.now(timezone.utc)
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)
        if self.total_amount == Decimal("0.0"):
            self.total_amount = (self.quantity * self.price) + self.fees + self.commission


@dataclass
class MarketData:
    """Market data for an instrument"""
    timestamp: datetime
    instrument_id: int
    open_price: Optional[Decimal] = None
    high_price: Optional[Decimal] = None
    low_price: Optional[Decimal] = None
    close_price: Decimal = Decimal("0.0")
    volume: Optional[int] = None
    bid_price: Optional[Decimal] = None
    ask_price: Optional[Decimal] = None
    data_source: Optional[str] = None

    @property
    def mid_price(self) -> Optional[Decimal]:
        """Calculate mid price from bid/ask"""
        if self.bid_price and self.ask_price:
            return (self.bid_price + self.ask_price) / 2
        return self.close_price


@dataclass
class PerformanceMetrics:
    """Portfolio performance metrics"""
    timestamp: datetime
    portfolio_id: int
    total_value: Decimal
    cash_balance: Decimal
    invested_amount: Decimal
    total_return: Decimal
    daily_return: Optional[Decimal] = None
    cumulative_return: Optional[Decimal] = None
    sharpe_ratio: Optional[Decimal] = None
    max_drawdown: Optional[Decimal] = None
    volatility: Optional[Decimal] = None
    beta: Optional[Decimal] = None
    alpha: Optional[Decimal] = None
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)


@dataclass
class RiskMetrics:
    """Risk metrics for portfolio"""
    timestamp: datetime
    portfolio_id: int
    var_95: Optional[Decimal] = None  # Value at Risk 95%
    var_99: Optional[Decimal] = None  # Value at Risk 99%
    cvar_95: Optional[Decimal] = None  # Conditional VaR 95%
    cvar_99: Optional[Decimal] = None  # Conditional VaR 99%
    tracking_error: Optional[Decimal] = None
    information_ratio: Optional[Decimal] = None
    sortino_ratio: Optional[Decimal] = None
    calmar_ratio: Optional[Decimal] = None
    maximum_drawdown: Optional[Decimal] = None
    drawdown_duration: Optional[int] = None  # days
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)


@dataclass
class AttributionAnalysis:
    """Performance attribution analysis"""
    timestamp: datetime
    portfolio_id: int
    instrument_id: Optional[int] = None
    sector: Optional[str] = None
    attribution_type: AttributionType = AttributionType.STOCK_SELECTION
    contribution: Decimal = Decimal("0.0")
    weight: Optional[Decimal] = None
    benchmark_weight: Optional[Decimal] = None
    created_at: Optional[datetime] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc)


# Pydantic models for API requests/responses
class InstrumentCreate(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    instrument_type: InstrumentType
    exchange: Optional[str] = Field(None, max_length=10)
    sector: Optional[str] = Field(None, max_length=50)
    currency: str = Field("USD", max_length=3)
    multiplier: Decimal = Field(Decimal("1.0"), gt=0)
    tick_size: Decimal = Field(Decimal("0.01"), gt=0)


class PortfolioCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    base_currency: str = Field("USD", max_length=3)
    initial_capital: Decimal = Field(..., gt=0)


class TransactionCreate(BaseModel):
    portfolio_id: int
    instrument_id: int
    transaction_type: TransactionType
    quantity: Decimal
    price: Decimal = Field(..., gt=0)
    fees: Decimal = Field(Decimal("0.0"), ge=0)
    commission: Decimal = Field(Decimal("0.0"), ge=0)
    execution_time: Optional[datetime] = None
    order_id: Optional[str] = None
    strategy_name: Optional[str] = None
    notes: Optional[str] = None


class PositionResponse(BaseModel):
    portfolio_id: int
    instrument_id: int
    symbol: str
    quantity: Decimal
    average_cost: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal
    return_pct: Decimal
    timestamp: datetime

    class Config:
        from_attributes = True


class PortfolioSummary(BaseModel):
    portfolio_id: int
    name: str
    base_currency: str
    position_count: int
    total_market_value: Decimal
    total_unrealized_pnl: Decimal
    total_realized_pnl: Decimal
    net_value: Decimal
    total_return_pct: Optional[Decimal] = None

    class Config:
        from_attributes = True


class PerformanceAnalysis(BaseModel):
    """Comprehensive performance analysis"""
    portfolio_id: int
    period: str  # '1D', '1W', '1M', '3M', '6M', '1Y', 'YTD', 'ALL'
    start_date: datetime
    end_date: datetime
    
    # Return metrics
    total_return: Decimal
    annualized_return: Decimal
    daily_returns: List[Decimal]
    cumulative_returns: List[Decimal]
    
    # Risk metrics
    volatility: Decimal
    sharpe_ratio: Decimal
    sortino_ratio: Decimal
    max_drawdown: Decimal
    var_95: Decimal
    cvar_95: Decimal
    
    # Benchmark comparison
    benchmark_return: Optional[Decimal] = None
    alpha: Optional[Decimal] = None
    beta: Optional[Decimal] = None
    tracking_error: Optional[Decimal] = None
    information_ratio: Optional[Decimal] = None
    
    # Attribution analysis
    attribution_breakdown: Dict[str, Decimal] = Field(default_factory=dict)


class RiskAnalysis(BaseModel):
    """Comprehensive risk analysis"""
    portfolio_id: int
    timestamp: datetime
    
    # VaR metrics
    var_95_1d: Decimal
    var_99_1d: Decimal
    cvar_95_1d: Decimal
    cvar_99_1d: Decimal
    
    # Drawdown metrics
    current_drawdown: Decimal
    max_drawdown: Decimal
    max_drawdown_duration: int
    
    # Concentration risk
    top_5_positions_weight: Decimal
    sector_concentrations: Dict[str, Decimal]
    currency_exposures: Dict[str, Decimal]
    
    # Portfolio metrics
    portfolio_beta: Decimal
    tracking_error: Decimal
    active_share: Decimal


# Utility classes for calculations
class PortfolioCalculator:
    """Portfolio calculation utilities"""
    
    @staticmethod
    def calculate_returns(prices: List[Decimal]) -> List[Decimal]:
        """Calculate simple returns from price series"""
        if len(prices) < 2:
            return []
        
        returns = []
        for i in range(1, len(prices)):
            if prices[i-1] != 0:
                ret = (prices[i] - prices[i-1]) / prices[i-1]
                returns.append(ret)
            else:
                returns.append(Decimal("0.0"))
        return returns
    
    @staticmethod
    def calculate_sharpe_ratio(returns: List[Decimal], risk_free_rate: Decimal = Decimal("0.02")) -> Decimal:
        """Calculate Sharpe ratio"""
        if not returns:
            return Decimal("0.0")
        
        returns_array = np.array([float(r) for r in returns])
        excess_returns = returns_array - float(risk_free_rate) / 252  # Daily risk-free rate
        
        if np.std(excess_returns) == 0:
            return Decimal("0.0")
        
        return Decimal(str(np.mean(excess_returns) / np.std(excess_returns) * np.sqrt(252)))
    
    @staticmethod
    def calculate_max_drawdown(cumulative_returns: List[Decimal]) -> Decimal:
        """Calculate maximum drawdown"""
        if not cumulative_returns:
            return Decimal("0.0")
        
        peak = cumulative_returns[0]
        max_dd = Decimal("0.0")
        
        for ret in cumulative_returns:
            if ret > peak:
                peak = ret
            dd = (peak - ret) / peak if peak != 0 else Decimal("0.0")
            if dd > max_dd:
                max_dd = dd
        
        return max_dd
    
    @staticmethod
    def calculate_var(returns: List[Decimal], confidence: float = 0.95) -> Decimal:
        """Calculate Value at Risk"""
        if not returns:
            return Decimal("0.0")
        
        returns_array = np.array([float(r) for r in returns])
        return Decimal(str(np.percentile(returns_array, (1 - confidence) * 100)))


class PerformanceTracker:
    """Track and calculate portfolio performance metrics"""
    
    def __init__(self, portfolio_id: int):
        self.portfolio_id = portfolio_id
        
    def update_performance_metrics(self, positions: List[Position], 
                                 benchmark_data: Optional[List[MarketData]] = None) -> PerformanceMetrics:
        """Update comprehensive performance metrics"""
        
        total_value = sum(pos.market_value or Decimal("0.0") for pos in positions)
        invested_amount = sum(pos.quantity * pos.average_cost for pos in positions)
        total_return = sum(pos.total_pnl for pos in positions)
        
        # Create performance metrics object
        metrics = PerformanceMetrics(
            timestamp=datetime.now(timezone.utc),
            portfolio_id=self.portfolio_id,
            total_value=total_value,
            cash_balance=Decimal("0.0"),  # Will be updated from portfolio cash tracking
            invested_amount=invested_amount,
            total_return=total_return
        )
        
        return metrics