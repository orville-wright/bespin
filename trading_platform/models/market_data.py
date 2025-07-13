"""
Market data models
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any, List
from pydantic import Field, validator
from sqlalchemy import Column, String, Numeric, DateTime, Text, Integer, Index
from sqlalchemy.dialects.postgresql import JSONB

from .base import BaseModel, BaseEntity, TimestampModel


class MarketDataEntity(BaseEntity):
    """SQLAlchemy Market Data entity"""
    __tablename__ = "market_data"
    
    symbol = Column(String(20), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    data_type = Column(String(20), nullable=False)  # 'quote', 'bar', 'trade'
    
    # Price data
    price = Column(Numeric(20, 8), nullable=True)
    bid = Column(Numeric(20, 8), nullable=True)
    ask = Column(Numeric(20, 8), nullable=True)
    bid_size = Column(Numeric(20, 8), nullable=True)
    ask_size = Column(Numeric(20, 8), nullable=True)
    
    # OHLCV data (for bars)
    open_price = Column(Numeric(20, 8), nullable=True)
    high_price = Column(Numeric(20, 8), nullable=True)
    low_price = Column(Numeric(20, 8), nullable=True)
    close_price = Column(Numeric(20, 8), nullable=True)
    volume = Column(Numeric(20, 8), nullable=True)
    
    # Additional data
    metadata = Column(JSONB, nullable=True)
    
    # Create composite index for efficient querying
    __table_args__ = (
        Index('idx_symbol_timestamp', 'symbol', 'timestamp'),
        Index('idx_symbol_type_timestamp', 'symbol', 'data_type', 'timestamp'),
    )


class Quote(BaseModel):
    """Real-time quote data"""
    
    symbol: str = Field(..., description="Trading symbol")
    timestamp: datetime = Field(..., description="Quote timestamp")
    bid: Decimal = Field(..., gt=0, description="Bid price")
    ask: Decimal = Field(..., gt=0, description="Ask price")
    bid_size: Decimal = Field(..., ge=0, description="Bid size")
    ask_size: Decimal = Field(..., ge=0, description="Ask size")
    
    # Optional fields
    last_price: Optional[Decimal] = Field(None, gt=0, description="Last trade price")
    last_size: Optional[Decimal] = Field(None, ge=0, description="Last trade size")
    
    # Metadata
    source: Optional[str] = Field(None, description="Data source")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator('ask')
    def ask_must_be_greater_than_bid(cls, v, values):
        """Validate ask price is greater than bid price"""
        if 'bid' in values and v <= values['bid']:
            raise ValueError("Ask price must be greater than bid price")
        return v
    
    @property
    def spread(self) -> Decimal:
        """Calculate bid-ask spread"""
        return self.ask - self.bid
    
    @property
    def mid_price(self) -> Decimal:
        """Calculate mid price"""
        return (self.bid + self.ask) / 2
    
    @property
    def spread_percentage(self) -> Decimal:
        """Calculate spread as percentage of mid price"""
        return (self.spread / self.mid_price) * 100


class Bar(BaseModel):
    """OHLCV bar data"""
    
    symbol: str = Field(..., description="Trading symbol")
    timestamp: datetime = Field(..., description="Bar timestamp")
    timeframe: str = Field(..., description="Bar timeframe (1m, 5m, 1h, 1d, etc.)")
    
    # OHLCV data
    open: Decimal = Field(..., gt=0, description="Open price")
    high: Decimal = Field(..., gt=0, description="High price")
    low: Decimal = Field(..., gt=0, description="Low price")
    close: Decimal = Field(..., gt=0, description="Close price")
    volume: Decimal = Field(..., ge=0, description="Volume")
    
    # Optional fields
    vwap: Optional[Decimal] = Field(None, gt=0, description="Volume weighted average price")
    trade_count: Optional[int] = Field(None, ge=0, description="Number of trades")
    
    # Metadata
    source: Optional[str] = Field(None, description="Data source")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    @validator('high')
    def high_must_be_highest(cls, v, values):
        """Validate high is the highest price"""
        if 'open' in values and v < values['open']:
            raise ValueError("High must be >= open")
        if 'close' in values and v < values['close']:
            raise ValueError("High must be >= close")
        return v
    
    @validator('low')
    def low_must_be_lowest(cls, v, values):
        """Validate low is the lowest price"""
        if 'open' in values and v > values['open']:
            raise ValueError("Low must be <= open")
        if 'close' in values and v > values['close']:
            raise ValueError("Low must be <= close")
        return v
    
    @property
    def price_change(self) -> Decimal:
        """Calculate price change (close - open)"""
        return self.close - self.open
    
    @property
    def price_change_percentage(self) -> Decimal:
        """Calculate price change percentage"""
        return (self.price_change / self.open) * 100
    
    @property
    def range(self) -> Decimal:
        """Calculate high-low range"""
        return self.high - self.low
    
    @property
    def range_percentage(self) -> Decimal:
        """Calculate range as percentage of open"""
        return (self.range / self.open) * 100
    
    @property
    def is_green(self) -> bool:
        """Check if bar is green (close > open)"""
        return self.close > self.open
    
    @property
    def is_red(self) -> bool:
        """Check if bar is red (close < open)"""
        return self.close < self.open
    
    @property
    def is_doji(self) -> bool:
        """Check if bar is doji (close == open)"""
        return self.close == self.open


class MarketData(BaseModel, TimestampModel):
    """
    Generic market data container
    """
    
    symbol: str = Field(..., description="Trading symbol")
    data_type: str = Field(..., description="Type of market data")
    timestamp: datetime = Field(..., description="Data timestamp")
    
    # Raw data
    data: Dict[str, Any] = Field(..., description="Raw market data")
    
    # Source information
    source: Optional[str] = Field(None, description="Data source")
    latency_ms: Optional[int] = Field(None, ge=0, description="Data latency in milliseconds")
    
    @classmethod
    def from_quote(cls, quote: Quote) -> 'MarketData':
        """Create MarketData from Quote"""
        return cls(
            symbol=quote.symbol,
            data_type="quote",
            timestamp=quote.timestamp,
            data=quote.dict(),
            source=quote.source
        )
    
    @classmethod
    def from_bar(cls, bar: Bar) -> 'MarketData':
        """Create MarketData from Bar"""
        return cls(
            symbol=bar.symbol,
            data_type="bar",
            timestamp=bar.timestamp,
            data=bar.dict(),
            source=bar.source
        )
    
    def to_quote(self) -> Optional[Quote]:
        """Convert to Quote if applicable"""
        if self.data_type == "quote":
            return Quote(**self.data)
        return None
    
    def to_bar(self) -> Optional[Bar]:
        """Convert to Bar if applicable"""
        if self.data_type == "bar":
            return Bar(**self.data)
        return None


class MarketDataSnapshot(BaseModel):
    """
    Market data snapshot for multiple symbols
    """
    
    timestamp: datetime = Field(..., description="Snapshot timestamp")
    quotes: Dict[str, Quote] = Field(default_factory=dict, description="Latest quotes by symbol")
    bars: Dict[str, List[Bar]] = Field(default_factory=dict, description="Recent bars by symbol")
    
    # Metadata
    source: Optional[str] = Field(None, description="Data source")
    symbols_count: int = Field(0, description="Number of symbols in snapshot")
    
    @validator('symbols_count', always=True)
    def calculate_symbols_count(cls, v, values):
        """Calculate number of unique symbols"""
        symbols = set()
        symbols.update(values.get('quotes', {}).keys())
        symbols.update(values.get('bars', {}).keys())
        return len(symbols)
    
    def get_latest_price(self, symbol: str) -> Optional[Decimal]:
        """Get latest price for a symbol"""
        # Try quote first
        if symbol in self.quotes:
            quote = self.quotes[symbol]
            return quote.last_price or quote.mid_price
        
        # Try latest bar
        if symbol in self.bars and self.bars[symbol]:
            latest_bar = max(self.bars[symbol], key=lambda b: b.timestamp)
            return latest_bar.close
        
        return None
    
    def get_symbols(self) -> List[str]:
        """Get all symbols in snapshot"""
        symbols = set()
        symbols.update(self.quotes.keys())
        symbols.update(self.bars.keys())
        return sorted(list(symbols))