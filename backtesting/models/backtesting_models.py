"""
Backtesting Framework Models
Core data models for the comprehensive backtesting system
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from dataclasses import dataclass, field
from pydantic import BaseModel, Field, validator
import uuid


class BacktestStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BacktestMode(str, Enum):
    FULL = "full"  # Complete historical backtest
    WALK_FORWARD = "walk_forward"  # Walk-forward analysis
    MONTE_CARLO = "monte_carlo"  # Monte Carlo simulation
    STRESS_TEST = "stress_test"  # Stress testing


class RebalanceFrequency(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    CUSTOM = "custom"


class CostModel(str, Enum):
    FIXED = "fixed"  # Fixed cost per trade
    PERCENTAGE = "percentage"  # Percentage of trade value
    TIERED = "tiered"  # Tiered commission structure
    REALISTIC = "realistic"  # Realistic broker fees


@dataclass
class BacktestConfig:
    """Configuration for backtesting parameters"""
    backtest_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: Optional[str] = None
    
    # Time parameters
    start_date: datetime = field(default_factory=lambda: datetime(2020, 1, 1, tzinfo=timezone.utc))
    end_date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Capital and universe
    initial_capital: Decimal = Decimal("100000.0")
    benchmark_symbol: Optional[str] = "SPY"
    universe: List[str] = field(default_factory=list)  # Trading universe
    
    # Execution parameters
    mode: BacktestMode = BacktestMode.FULL
    rebalance_frequency: RebalanceFrequency = RebalanceFrequency.DAILY
    lookback_window: Optional[int] = None  # Days for strategy lookback
    
    # Transaction costs
    cost_model: CostModel = CostModel.REALISTIC
    commission_rate: Decimal = Decimal("0.001")  # 0.1% default
    fixed_commission: Decimal = Decimal("1.0")  # $1 per trade
    bid_ask_spread: Decimal = Decimal("0.0002")  # 0.02% spread
    market_impact: Decimal = Decimal("0.0005")  # 0.05% impact
    
    # Risk management
    max_position_size: Optional[Decimal] = Decimal("0.05")  # 5% max per position
    max_leverage: Decimal = Decimal("1.0")  # No leverage by default
    stop_loss_pct: Optional[Decimal] = None
    take_profit_pct: Optional[Decimal] = None
    
    # Walk-forward specific
    training_window: Optional[int] = 252  # 1 year training window
    testing_window: Optional[int] = 63   # 3 months testing window
    step_size: Optional[int] = 21        # 1 month step
    
    # Monte Carlo specific
    num_simulations: Optional[int] = 1000
    confidence_levels: List[float] = field(default_factory=lambda: [0.95, 0.99])
    
    # Performance settings
    use_real_time_data: bool = False
    enable_slippage: bool = True
    enable_dividends: bool = True
    currency: str = "USD"
    
    def __post_init__(self):
        """Validate configuration after initialization"""
        if self.end_date <= self.start_date:
            raise ValueError("End date must be after start date")
        if self.initial_capital <= 0:
            raise ValueError("Initial capital must be positive")


@dataclass
class BacktestPosition:
    """Represents a position during backtesting"""
    timestamp: datetime
    symbol: str
    quantity: Decimal
    entry_price: Decimal
    current_price: Decimal
    entry_date: datetime
    strategy_name: str
    position_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    
    @property
    def market_value(self) -> Decimal:
        """Current market value of position"""
        return self.quantity * self.current_price
    
    @property
    def unrealized_pnl(self) -> Decimal:
        """Unrealized P&L"""
        return (self.current_price - self.entry_price) * self.quantity
    
    @property
    def unrealized_pnl_pct(self) -> Decimal:
        """Unrealized P&L percentage"""
        if self.entry_price == 0:
            return Decimal("0")
        return (self.current_price - self.entry_price) / self.entry_price * 100
    
    @property
    def days_held(self) -> int:
        """Number of days position has been held"""
        return (self.timestamp - self.entry_date).days


@dataclass
class BacktestTrade:
    """Represents a completed trade in backtesting"""
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str = ""
    strategy_name: str = ""
    
    # Entry details
    entry_date: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    entry_price: Decimal = Decimal("0")
    entry_quantity: Decimal = Decimal("0")
    entry_commission: Decimal = Decimal("0")
    entry_slippage: Decimal = Decimal("0")
    
    # Exit details
    exit_date: Optional[datetime] = None
    exit_price: Optional[Decimal] = None
    exit_quantity: Optional[Decimal] = None
    exit_commission: Decimal = Decimal("0")
    exit_slippage: Decimal = Decimal("0")
    
    # Trade metrics
    gross_pnl: Decimal = Decimal("0")
    net_pnl: Decimal = Decimal("0")
    total_commission: Decimal = Decimal("0")
    total_slippage: Decimal = Decimal("0")
    
    # Trade context
    side: str = "long"  # "long" or "short"
    exit_reason: Optional[str] = None  # "signal", "stop_loss", "take_profit", "end_of_backtest"
    confidence: Optional[Decimal] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def close_trade(self, exit_date: datetime, exit_price: Decimal, 
                   exit_quantity: Decimal, commission: Decimal = Decimal("0"),
                   slippage: Decimal = Decimal("0"), reason: str = "signal"):
        """Close the trade and calculate metrics"""
        self.exit_date = exit_date
        self.exit_price = exit_price
        self.exit_quantity = exit_quantity
        self.exit_commission = commission
        self.exit_slippage = slippage
        self.exit_reason = reason
        
        # Calculate P&L
        if self.side == "long":
            self.gross_pnl = (exit_price - self.entry_price) * min(self.entry_quantity, exit_quantity)
        else:  # short
            self.gross_pnl = (self.entry_price - exit_price) * min(self.entry_quantity, exit_quantity)
        
        self.total_commission = self.entry_commission + self.exit_commission
        self.total_slippage = self.entry_slippage + self.exit_slippage
        self.net_pnl = self.gross_pnl - self.total_commission - self.total_slippage
    
    @property
    def return_pct(self) -> Decimal:
        """Return percentage"""
        entry_value = self.entry_price * self.entry_quantity
        if entry_value == 0:
            return Decimal("0")
        return self.net_pnl / entry_value * 100
    
    @property
    def days_held(self) -> Optional[int]:
        """Number of days trade was held"""
        if self.exit_date:
            return (self.exit_date - self.entry_date).days
        return None
    
    @property
    def is_winner(self) -> bool:
        """Whether trade was profitable"""
        return self.net_pnl > 0


@dataclass
class BacktestSnapshot:
    """Portfolio snapshot at a point in time during backtesting"""
    timestamp: datetime
    total_value: Decimal
    cash: Decimal
    invested_value: Decimal
    positions: List[BacktestPosition]
    daily_return: Decimal = Decimal("0")
    cumulative_return: Decimal = Decimal("0")
    drawdown: Decimal = Decimal("0")
    
    @property
    def position_count(self) -> int:
        """Number of positions held"""
        return len(self.positions)
    
    @property
    def leverage(self) -> Decimal:
        """Current leverage ratio"""
        if self.total_value == 0:
            return Decimal("0")
        return self.invested_value / self.total_value


@dataclass
class BacktestResults:
    """Complete backtesting results"""
    config: BacktestConfig
    status: BacktestStatus = BacktestStatus.PENDING
    
    # Execution metadata
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    execution_duration: Optional[float] = None  # seconds
    error_message: Optional[str] = None
    
    # Portfolio snapshots and trades
    snapshots: List[BacktestSnapshot] = field(default_factory=list)
    trades: List[BacktestTrade] = field(default_factory=list)
    
    # Performance metrics
    total_return: Decimal = Decimal("0")
    annualized_return: Decimal = Decimal("0")
    volatility: Decimal = Decimal("0")
    sharpe_ratio: Decimal = Decimal("0")
    sortino_ratio: Decimal = Decimal("0")
    max_drawdown: Decimal = Decimal("0")
    calmar_ratio: Decimal = Decimal("0")
    
    # Trade statistics
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: Decimal = Decimal("0")
    avg_win: Decimal = Decimal("0")
    avg_loss: Decimal = Decimal("0")
    profit_factor: Decimal = Decimal("0")
    
    # Risk metrics
    var_95: Decimal = Decimal("0")
    cvar_95: Decimal = Decimal("0")
    maximum_leverage: Decimal = Decimal("0")
    avg_leverage: Decimal = Decimal("0")
    
    # Benchmark comparison
    benchmark_return: Optional[Decimal] = None
    alpha: Optional[Decimal] = None
    beta: Optional[Decimal] = None
    tracking_error: Optional[Decimal] = None
    information_ratio: Optional[Decimal] = None
    
    # Walk-forward specific results
    walk_forward_results: Optional[List[Dict[str, Any]]] = None
    
    # Monte Carlo specific results
    monte_carlo_results: Optional[Dict[str, Any]] = None
    
    def calculate_summary_metrics(self):
        """Calculate summary metrics from trades and snapshots"""
        if not self.snapshots:
            return
        
        # Calculate basic metrics
        initial_value = self.config.initial_capital
        final_value = self.snapshots[-1].total_value
        
        self.total_return = (final_value - initial_value) / initial_value
        
        # Calculate volatility and Sharpe from daily returns
        daily_returns = [s.daily_return for s in self.snapshots[1:]]  # Skip first day
        if daily_returns:
            import numpy as np
            returns_array = np.array([float(r) for r in daily_returns])
            self.volatility = Decimal(str(np.std(returns_array) * np.sqrt(252)))
            
            if self.volatility > 0:
                self.sharpe_ratio = self.annualized_return / self.volatility
        
        # Calculate trade statistics
        if self.trades:
            self.total_trades = len(self.trades)
            winning_trades = [t for t in self.trades if t.is_winner]
            losing_trades = [t for t in self.trades if not t.is_winner]
            
            self.winning_trades = len(winning_trades)
            self.losing_trades = len(losing_trades)
            self.win_rate = Decimal(self.winning_trades) / Decimal(self.total_trades) * 100
            
            if winning_trades:
                self.avg_win = sum(t.net_pnl for t in winning_trades) / len(winning_trades)
            if losing_trades:
                self.avg_loss = sum(abs(t.net_pnl) for t in losing_trades) / len(losing_trades)
                if self.avg_loss > 0:
                    self.profit_factor = self.avg_win / self.avg_loss
        
        # Calculate drawdown
        peak_value = initial_value
        max_dd = Decimal("0")
        for snapshot in self.snapshots:
            if snapshot.total_value > peak_value:
                peak_value = snapshot.total_value
            current_dd = (peak_value - snapshot.total_value) / peak_value
            if current_dd > max_dd:
                max_dd = current_dd
            snapshot.drawdown = current_dd
        
        self.max_drawdown = max_dd
        if self.max_drawdown > 0:
            self.calmar_ratio = self.annualized_return / self.max_drawdown


# Pydantic models for API
class BacktestConfigAPI(BaseModel):
    """API model for backtest configuration"""
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    
    start_date: datetime
    end_date: datetime
    initial_capital: Decimal = Field(..., gt=0)
    
    strategy_id: str
    universe: List[str] = Field(..., min_items=1)
    benchmark_symbol: Optional[str] = "SPY"
    
    mode: BacktestMode = BacktestMode.FULL
    rebalance_frequency: RebalanceFrequency = RebalanceFrequency.DAILY
    
    # Risk management
    max_position_size: Optional[Decimal] = Field(None, gt=0, le=1)
    max_leverage: Decimal = Field(Decimal("1.0"), ge=1, le=10)
    
    # Transaction costs
    commission_rate: Decimal = Field(Decimal("0.001"), ge=0, le=0.1)
    enable_slippage: bool = True
    
    @validator('end_date')
    def validate_dates(cls, v, values):
        if 'start_date' in values and v <= values['start_date']:
            raise ValueError('End date must be after start date')
        return v


class BacktestStatus(BaseModel):
    """API model for backtest status"""
    backtest_id: str
    status: BacktestStatus
    progress: float = Field(0, ge=0, le=100)
    message: Optional[str] = None
    start_time: Optional[datetime] = None
    estimated_completion: Optional[datetime] = None


class BacktestSummary(BaseModel):
    """API model for backtest summary results"""
    backtest_id: str
    name: str
    status: BacktestStatus
    
    # Period and setup
    start_date: datetime
    end_date: datetime
    duration_days: int
    initial_capital: Decimal
    final_value: Decimal
    
    # Key metrics
    total_return: Decimal
    annualized_return: Decimal
    volatility: Decimal
    sharpe_ratio: Decimal
    max_drawdown: Decimal
    
    # Trade stats
    total_trades: int
    win_rate: Decimal
    profit_factor: Decimal
    
    # Benchmark comparison
    benchmark_return: Optional[Decimal] = None
    alpha: Optional[Decimal] = None
    beta: Optional[Decimal] = None
    
    created_at: datetime
    completed_at: Optional[datetime] = None