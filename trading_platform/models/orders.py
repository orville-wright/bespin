"""
Order management models
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


class OrderType(str, Enum):
    """Order types supported by the trading platform"""
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"
    TRAILING_STOP = "trailing_stop"


class OrderSide(str, Enum):
    """Order side (buy/sell)"""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    """Order status lifecycle"""
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class OrderEntity(BaseEntity):
    """SQLAlchemy Order entity"""
    __tablename__ = "orders"
    
    order_id = Column(UUID(as_uuid=True), default=uuid.uuid4, unique=True, index=True)
    strategy_id = Column(String(100), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    side = Column(SQLEnum(OrderSide), nullable=False)
    order_type = Column(SQLEnum(OrderType), nullable=False)
    status = Column(SQLEnum(OrderStatus), default=OrderStatus.PENDING)
    
    # Quantities and prices
    quantity = Column(Numeric(20, 8), nullable=False)
    filled_quantity = Column(Numeric(20, 8), default=0)
    price = Column(Numeric(20, 8), nullable=True)  # None for market orders
    stop_price = Column(Numeric(20, 8), nullable=True)  # For stop orders
    avg_fill_price = Column(Numeric(20, 8), nullable=True)
    
    # Timing
    time_in_force = Column(String(10), default="DAY")  # DAY, GTC, IOC, FOK
    submitted_at = Column(DateTime(timezone=True), nullable=True)
    filled_at = Column(DateTime(timezone=True), nullable=True)
    expires_at = Column(DateTime(timezone=True), nullable=True)
    
    # External references
    broker_order_id = Column(String(100), nullable=True, index=True)
    parent_order_id = Column(UUID(as_uuid=True), nullable=True)  # For bracket orders
    
    # Metadata
    metadata = Column(Text, nullable=True)  # JSON string for additional data
    is_simulation = Column(Boolean, default=False)


class Order(BaseModel, IDMixin, TimestampModel):
    """
    Order model for trading operations
    """
    
    order_id: Optional[str] = Field(None, description="Unique order identifier")
    strategy_id: str = Field(..., description="Strategy that generated this order")
    symbol: str = Field(..., description="Trading symbol")
    side: OrderSide = Field(..., description="Buy or sell")
    order_type: OrderType = Field(..., description="Order type")
    status: OrderStatus = Field(OrderStatus.PENDING, description="Current order status")
    
    # Quantities and prices
    quantity: Decimal = Field(..., gt=0, description="Order quantity")
    filled_quantity: Decimal = Field(Decimal('0'), ge=0, description="Filled quantity")
    price: Optional[Decimal] = Field(None, gt=0, description="Limit price (if applicable)")
    stop_price: Optional[Decimal] = Field(None, gt=0, description="Stop price (if applicable)")
    avg_fill_price: Optional[Decimal] = Field(None, gt=0, description="Average fill price")
    
    # Timing
    time_in_force: str = Field("DAY", description="Time in force (DAY, GTC, IOC, FOK)")
    submitted_at: Optional[datetime] = Field(None, description="When order was submitted")
    filled_at: Optional[datetime] = Field(None, description="When order was filled")
    expires_at: Optional[datetime] = Field(None, description="When order expires")
    
    # External references
    broker_order_id: Optional[str] = Field(None, description="Broker's order ID")
    parent_order_id: Optional[str] = Field(None, description="Parent order for bracket orders")
    
    # Metadata
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional order metadata")
    is_simulation: bool = Field(False, description="Whether this is a simulation order")
    
    @validator('filled_quantity')
    def validate_filled_quantity(cls, v, values):
        """Validate filled quantity doesn't exceed order quantity"""
        if 'quantity' in values and v > values['quantity']:
            raise ValueError("Filled quantity cannot exceed order quantity")
        return v
    
    @validator('price', 'stop_price', always=True)
    def validate_prices(cls, v, values, field):
        """Validate price fields based on order type"""
        order_type = values.get('order_type')
        
        if order_type == OrderType.LIMIT and field.name == 'price' and v is None:
            raise ValueError("Limit orders must have a price")
        
        if order_type in [OrderType.STOP, OrderType.STOP_LIMIT, OrderType.TRAILING_STOP]:
            if field.name == 'stop_price' and v is None:
                raise ValueError(f"{order_type} orders must have a stop price")
        
        return v
    
    @property
    def remaining_quantity(self) -> Decimal:
        """Calculate remaining quantity to be filled"""
        return self.quantity - self.filled_quantity
    
    @property
    def is_filled(self) -> bool:
        """Check if order is completely filled"""
        return self.status == OrderStatus.FILLED
    
    @property
    def is_active(self) -> bool:
        """Check if order is still active (can be filled)"""
        return self.status in [OrderStatus.PENDING, OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED]
    
    def to_broker_order(self) -> Dict[str, Any]:
        """Convert to broker-specific order format (Alpaca example)"""
        order_data = {
            "symbol": self.symbol,
            "qty": float(self.quantity),
            "side": self.side.value,
            "type": self.order_type.value,
            "time_in_force": self.time_in_force,
        }
        
        if self.price:
            order_data["limit_price"] = float(self.price)
        
        if self.stop_price:
            order_data["stop_price"] = float(self.stop_price)
        
        return order_data


class OrderCreate(BaseModel):
    """Model for creating new orders"""
    strategy_id: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal = Field(..., gt=0)
    price: Optional[Decimal] = Field(None, gt=0)
    stop_price: Optional[Decimal] = Field(None, gt=0)
    time_in_force: str = "DAY"
    metadata: Optional[Dict[str, Any]] = None
    is_simulation: bool = False


class OrderUpdate(BaseModel):
    """Model for updating existing orders"""
    status: Optional[OrderStatus] = None
    filled_quantity: Optional[Decimal] = Field(None, ge=0)
    avg_fill_price: Optional[Decimal] = Field(None, gt=0)
    broker_order_id: Optional[str] = None
    submitted_at: Optional[datetime] = None
    filled_at: Optional[datetime] = None
    metadata: Optional[Dict[str, Any]] = None