"""
Strategy management models
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import Field, validator
from sqlalchemy import Column, String, Numeric, DateTime, Enum as SQLEnum, Text, Boolean, Integer
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid

from .base import BaseModel, BaseEntity, IDMixin, TimestampModel


class StrategyStatus(str, Enum):
    """Strategy execution status"""
    INACTIVE = "inactive"
    ACTIVE = "active"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


class StrategySignal(str, Enum):
    """Trading signals from strategies"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    CLOSE_LONG = "close_long"
    CLOSE_SHORT = "close_short"


class StrategyEntity(BaseEntity):
    """SQLAlchemy Strategy entity"""
    __tablename__ = "strategies"
    
    strategy_id = Column(String(100), unique=True, index=True, nullable=False)
    name = Column(String(200), nullable=False)
    description = Column(Text, nullable=True)
    status = Column(SQLEnum(StrategyStatus), default=StrategyStatus.INACTIVE)
    
    # Strategy configuration
    strategy_type = Column(String(50), nullable=False)  # e.g., "momentum", "mean_reversion"
    parameters = Column(JSONB, nullable=True)  # Strategy-specific parameters
    symbols = Column(JSONB, nullable=True)  # List of symbols to trade
    
    # Execution settings
    max_position_size = Column(Numeric(20, 8), nullable=True)
    max_daily_trades = Column(Integer, nullable=True)
    risk_limit = Column(Numeric(20, 2), nullable=True)  # Max loss per day
    
    # Performance tracking
    total_trades = Column(Integer, default=0)
    winning_trades = Column(Integer, default=0)
    losing_trades = Column(Integer, default=0)
    total_pnl = Column(Numeric(20, 2), default=0)
    max_drawdown = Column(Numeric(20, 2), default=0)
    
    # Timing
    started_at = Column(DateTime(timezone=True), nullable=True)
    stopped_at = Column(DateTime(timezone=True), nullable=True)
    last_signal_at = Column(DateTime(timezone=True), nullable=True)
    
    # Settings
    is_simulation = Column(Boolean, default=True)
    auto_execute = Column(Boolean, default=False)
    
    # Metadata
    metadata = Column(JSONB, nullable=True)


class Strategy(BaseModel, IDMixin, TimestampModel):
    """
    Trading strategy model
    """
    
    strategy_id: str = Field(..., description="Unique strategy identifier")
    name: str = Field(..., description="Strategy name")
    description: Optional[str] = Field(None, description="Strategy description")
    status: StrategyStatus = Field(StrategyStatus.INACTIVE, description="Current status")
    
    # Strategy configuration
    strategy_type: str = Field(..., description="Strategy type/category")
    parameters: Optional[Dict[str, Any]] = Field(None, description="Strategy parameters")
    symbols: Optional[List[str]] = Field(None, description="Symbols to trade")
    
    # Execution settings
    max_position_size: Optional[Decimal] = Field(None, gt=0, description="Maximum position size")
    max_daily_trades: Optional[int] = Field(None, gt=0, description="Maximum trades per day")
    risk_limit: Optional[Decimal] = Field(None, gt=0, description="Daily risk limit")
    
    # Performance tracking
    total_trades: int = Field(0, ge=0, description="Total number of trades")
    winning_trades: int = Field(0, ge=0, description="Number of winning trades")
    losing_trades: int = Field(0, ge=0, description="Number of losing trades")
    total_pnl: Decimal = Field(Decimal('0'), description="Total P&L")
    max_drawdown: Decimal = Field(Decimal('0'), description="Maximum drawdown")
    
    # Timing
    started_at: Optional[datetime] = Field(None, description="When strategy was started")
    stopped_at: Optional[datetime] = Field(None, description="When strategy was stopped")
    last_signal_at: Optional[datetime] = Field(None, description="Last signal timestamp")
    
    # Settings
    is_simulation: bool = Field(True, description="Whether running in simulation mode")
    auto_execute: bool = Field(False, description="Whether to auto-execute signals")
    
    # Metadata
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator('losing_trades', always=True)
    def validate_trade_counts(cls, v, values):
        """Ensure trade counts are consistent"""
        total = values.get('total_trades', 0)
        winning = values.get('winning_trades', 0)
        
        if winning + v > total:
            raise ValueError("Sum of winning and losing trades cannot exceed total trades")
        return v
    
    @property
    def win_rate(self) -> Optional[Decimal]:
        """Calculate win rate percentage"""
        if self.total_trades > 0:
            return Decimal(self.winning_trades) / Decimal(self.total_trades) * 100
        return None
    
    @property
    def avg_pnl_per_trade(self) -> Optional[Decimal]:
        """Calculate average P&L per trade"""
        if self.total_trades > 0:
            return self.total_pnl / Decimal(self.total_trades)
        return None
    
    @property
    def is_running(self) -> bool:
        """Check if strategy is currently running"""
        return self.status == StrategyStatus.ACTIVE
    
    @property
    def can_trade(self) -> bool:
        """Check if strategy can execute trades"""
        return self.status in [StrategyStatus.ACTIVE] and not self._is_risk_limit_exceeded()
    
    def _is_risk_limit_exceeded(self) -> bool:
        """Check if daily risk limit is exceeded"""
        if self.risk_limit and self.total_pnl < -self.risk_limit:
            return True
        return False
    
    def start(self) -> None:
        """Start the strategy"""
        self.status = StrategyStatus.ACTIVE
        self.started_at = datetime.utcnow()
        self.stopped_at = None
    
    def stop(self) -> None:
        """Stop the strategy"""
        self.status = StrategyStatus.STOPPED
        self.stopped_at = datetime.utcnow()
    
    def pause(self) -> None:
        """Pause the strategy"""
        self.status = StrategyStatus.PAUSED
    
    def resume(self) -> None:
        """Resume the strategy"""
        if self.status == StrategyStatus.PAUSED:
            self.status = StrategyStatus.ACTIVE
    
    def record_trade(self, pnl: Decimal, is_winner: bool) -> None:
        """Record a completed trade"""
        self.total_trades += 1
        self.total_pnl += pnl
        
        if is_winner:
            self.winning_trades += 1
        else:
            self.losing_trades += 1
        
        # Update max drawdown if necessary
        if pnl < 0 and abs(pnl) > self.max_drawdown:
            self.max_drawdown = abs(pnl)
    
    def generate_signal(self, symbol: str, signal: StrategySignal, 
                       confidence: float = 1.0, metadata: Optional[Dict] = None) -> 'StrategySignalEvent':
        """Generate a trading signal"""
        self.last_signal_at = datetime.utcnow()
        
        return StrategySignalEvent(
            strategy_id=self.strategy_id,
            symbol=symbol,
            signal=signal,
            confidence=confidence,
            timestamp=self.last_signal_at,
            metadata=metadata or {}
        )


class StrategySignalEvent(BaseModel):
    """
    Trading signal event generated by a strategy
    """
    
    strategy_id: str = Field(..., description="Strategy that generated the signal")
    symbol: str = Field(..., description="Symbol for the signal")
    signal: StrategySignal = Field(..., description="Type of signal")
    confidence: float = Field(..., ge=0, le=1, description="Signal confidence (0-1)")
    timestamp: datetime = Field(..., description="When signal was generated")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Signal metadata")
    
    # Price context
    current_price: Optional[Decimal] = Field(None, description="Current market price")
    target_price: Optional[Decimal] = Field(None, description="Target price for signal")
    stop_loss: Optional[Decimal] = Field(None, description="Suggested stop loss")
    take_profit: Optional[Decimal] = Field(None, description="Suggested take profit")
    
    # Position sizing
    suggested_quantity: Optional[Decimal] = Field(None, description="Suggested position size")
    risk_amount: Optional[Decimal] = Field(None, description="Amount to risk")
    
    @property
    def is_entry_signal(self) -> bool:
        """Check if this is an entry signal"""
        return self.signal in [StrategySignal.BUY, StrategySignal.SELL]
    
    @property
    def is_exit_signal(self) -> bool:
        """Check if this is an exit signal"""
        return self.signal in [StrategySignal.CLOSE_LONG, StrategySignal.CLOSE_SHORT]
    
    def to_order_params(self) -> Dict[str, Any]:
        """Convert signal to order parameters"""
        if not self.is_entry_signal:
            raise ValueError("Can only convert entry signals to orders")
        
        side = "buy" if self.signal == StrategySignal.BUY else "sell"
        
        params = {
            "strategy_id": self.strategy_id,
            "symbol": self.symbol,
            "side": side,
            "order_type": "market",  # Default to market order
            "quantity": self.suggested_quantity,
            "metadata": {
                "signal_confidence": self.confidence,
                "signal_timestamp": self.timestamp.isoformat(),
                **self.metadata
            }
        }
        
        if self.target_price:
            params["order_type"] = "limit"
            params["price"] = self.target_price
        
        return params


class StrategyCreate(BaseModel):
    """Model for creating new strategies"""
    strategy_id: str
    name: str
    description: Optional[str] = None
    strategy_type: str
    parameters: Optional[Dict[str, Any]] = None
    symbols: Optional[List[str]] = None
    max_position_size: Optional[Decimal] = None
    max_daily_trades: Optional[int] = None
    risk_limit: Optional[Decimal] = None
    is_simulation: bool = True
    auto_execute: bool = False
    metadata: Optional[Dict[str, Any]] = None


class StrategyUpdate(BaseModel):
    """Model for updating strategies"""
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[StrategyStatus] = None
    parameters: Optional[Dict[str, Any]] = None
    symbols: Optional[List[str]] = None
    max_position_size: Optional[Decimal] = None
    max_daily_trades: Optional[int] = None
    risk_limit: Optional[Decimal] = None
    auto_execute: Optional[bool] = None
    metadata: Optional[Dict[str, Any]] = None