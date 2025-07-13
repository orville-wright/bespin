"""
Position management models
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any
from pydantic import Field, validator
from sqlalchemy import Column, String, Numeric, DateTime, Enum as SQLEnum, Text, Boolean
from sqlalchemy.dialects.postgresql import UUID
import uuid

from .base import BaseModel, BaseEntity, IDMixin, TimestampModel


class PositionStatus(str, Enum):
    """Position status"""
    OPEN = "open"
    CLOSED = "closed"
    CLOSING = "closing"  # In process of being closed


class PositionEntity(BaseEntity):
    """SQLAlchemy Position entity"""
    __tablename__ = "positions"
    
    position_id = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, index=True)
    strategy_id = Column(String(100), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    status = Column(SQLEnum(PositionStatus), default=PositionStatus.OPEN)
    
    # Position details
    quantity = Column(Numeric(20, 8), nullable=False)  # Positive for long, negative for short
    avg_entry_price = Column(Numeric(20, 8), nullable=False)
    current_price = Column(Numeric(20, 8), nullable=True)
    market_value = Column(Numeric(20, 2), nullable=True)
    
    # P&L
    unrealized_pnl = Column(Numeric(20, 2), default=0)
    realized_pnl = Column(Numeric(20, 2), default=0)
    total_pnl = Column(Numeric(20, 2), default=0)
    
    # Timing
    opened_at = Column(DateTime(timezone=True), nullable=False)
    closed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Cost basis and fees
    cost_basis = Column(Numeric(20, 2), nullable=False)
    total_fees = Column(Numeric(20, 2), default=0)
    
    # Risk management
    stop_loss = Column(Numeric(20, 8), nullable=True)
    take_profit = Column(Numeric(20, 8), nullable=True)
    
    # Metadata
    metadata = Column(Text, nullable=True)  # JSON string
    is_simulation = Column(Boolean, default=False)


class Position(BaseModel, IDMixin, TimestampModel):
    """
    Position model representing a trading position
    """
    
    position_id: Optional[str] = Field(None, description="Unique position identifier")
    strategy_id: str = Field(..., description="Strategy that opened this position")
    symbol: str = Field(..., description="Trading symbol")
    status: PositionStatus = Field(PositionStatus.OPEN, description="Position status")
    
    # Position details
    quantity: Decimal = Field(..., description="Position quantity (+ long, - short)")
    avg_entry_price: Decimal = Field(..., gt=0, description="Average entry price")
    current_price: Optional[Decimal] = Field(None, gt=0, description="Current market price")
    market_value: Optional[Decimal] = Field(None, description="Current market value")
    
    # P&L
    unrealized_pnl: Decimal = Field(Decimal('0'), description="Unrealized P&L")
    realized_pnl: Decimal = Field(Decimal('0'), description="Realized P&L")
    total_pnl: Decimal = Field(Decimal('0'), description="Total P&L")
    
    # Timing
    opened_at: datetime = Field(..., description="When position was opened")
    closed_at: Optional[datetime] = Field(None, description="When position was closed")
    
    # Cost basis and fees
    cost_basis: Decimal = Field(..., gt=0, description="Total cost basis")
    total_fees: Decimal = Field(Decimal('0'), ge=0, description="Total fees paid")
    
    # Risk management
    stop_loss: Optional[Decimal] = Field(None, gt=0, description="Stop loss price")
    take_profit: Optional[Decimal] = Field(None, gt=0, description="Take profit price")
    
    # Metadata
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional position metadata")
    is_simulation: bool = Field(False, description="Whether this is a simulation position")
    
    @validator('market_value', always=True)
    def calculate_market_value(cls, v, values):
        """Calculate market value if current price is available"""
        if values.get('current_price') and values.get('quantity'):
            return values['current_price'] * abs(values['quantity'])
        return v
    
    @validator('unrealized_pnl', always=True)
    def calculate_unrealized_pnl(cls, v, values):
        """Calculate unrealized P&L"""
        current_price = values.get('current_price')
        quantity = values.get('quantity')
        avg_entry_price = values.get('avg_entry_price')
        
        if current_price and quantity and avg_entry_price:
            return (current_price - avg_entry_price) * quantity
        return v
    
    @validator('total_pnl', always=True)
    def calculate_total_pnl(cls, v, values):
        """Calculate total P&L"""
        unrealized = values.get('unrealized_pnl', Decimal('0'))
        realized = values.get('realized_pnl', Decimal('0'))
        return unrealized + realized
    
    @property
    def is_long(self) -> bool:
        """Check if position is long"""
        return self.quantity > 0
    
    @property
    def is_short(self) -> bool:
        """Check if position is short"""
        return self.quantity < 0
    
    @property
    def position_size(self) -> Decimal:
        """Get absolute position size"""
        return abs(self.quantity)
    
    @property
    def is_open(self) -> bool:
        """Check if position is open"""
        return self.status == PositionStatus.OPEN
    
    @property
    def is_closed(self) -> bool:
        """Check if position is closed"""
        return self.status == PositionStatus.CLOSED
    
    @property
    def pnl_percentage(self) -> Optional[Decimal]:
        """Calculate P&L as percentage of cost basis"""
        if self.cost_basis > 0:
            return (self.total_pnl / self.cost_basis) * 100
        return None
    
    @property
    def holding_period(self) -> Optional[int]:
        """Get holding period in days"""
        if self.closed_at:
            return (self.closed_at - self.opened_at).days
        else:
            return (datetime.utcnow() - self.opened_at).days
    
    def update_current_price(self, price: Decimal) -> None:
        """Update current price and recalculate derived fields"""
        self.current_price = price
        self.market_value = price * abs(self.quantity)
        self.unrealized_pnl = (price - self.avg_entry_price) * self.quantity
        self.total_pnl = self.unrealized_pnl + self.realized_pnl
    
    def close_position(self, close_price: Decimal, fees: Decimal = Decimal('0')) -> None:
        """Close the position and calculate final P&L"""
        self.status = PositionStatus.CLOSED
        self.closed_at = datetime.utcnow()
        self.current_price = close_price
        
        # Calculate final P&L
        pnl = (close_price - self.avg_entry_price) * self.quantity
        self.realized_pnl += pnl
        self.unrealized_pnl = Decimal('0')
        self.total_pnl = self.realized_pnl
        
        # Add closing fees
        self.total_fees += fees


class PositionCreate(BaseModel):
    """Model for creating new positions"""
    strategy_id: str
    symbol: str
    quantity: Decimal
    avg_entry_price: Decimal = Field(..., gt=0)
    cost_basis: Decimal = Field(..., gt=0)
    stop_loss: Optional[Decimal] = Field(None, gt=0)
    take_profit: Optional[Decimal] = Field(None, gt=0)
    metadata: Optional[Dict[str, Any]] = None
    is_simulation: bool = False


class PositionUpdate(BaseModel):
    """Model for updating existing positions"""
    current_price: Optional[Decimal] = Field(None, gt=0)
    stop_loss: Optional[Decimal] = Field(None, gt=0)
    take_profit: Optional[Decimal] = Field(None, gt=0)
    metadata: Optional[Dict[str, Any]] = None