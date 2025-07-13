"""
Market Data Handler

Manages real-time market data feeds and distribution.
Integrates with existing data infrastructure (aop.py and data engines).
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
from decimal import Decimal
import uuid

logger = logging.getLogger(__name__)


class MarketDataHandler:
    """
    Handles real-time market data feeds and distribution
    
    Responsibilities:
    - Market data subscription management
    - Data feed integration with existing infrastructure
    - Real-time data processing and distribution
    - Data caching and historical data access
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.is_simulation = self.config.get('simulation_mode', True)
        
        # Subscription management
        self.subscribed_symbols: Set[str] = set()
        self.subscribers: Dict[str, List[callable]] = {}
        
        # Data storage
        self.latest_data: Dict[str, Dict[str, Any]] = {}
        self.historical_data: Dict[str, List[Dict[str, Any]]] = {}
        
        # Data sources (integration points)
        self.data_sources = {
            'alpaca': None,
            'eod': None,
            'simulation': None
        }
        
        # Performance tracking
        self.stats = {
            'subscriptions': 0,
            'data_points_processed': 0,
            'last_update': None,
            'data_latency_ms': 0,
            'errors': 0
        }
        
        logger.info(f"MarketDataHandler initialized (simulation: {self.is_simulation})")
    
    async def initialize(self) -> None:
        """Initialize the market data handler"""
        try:
            # Initialize data sources
            await self._initialize_data_sources()
            
            # Start data processing loop
            if not self.is_simulation:
                asyncio.create_task(self._data_processing_loop())
            
            logger.info("MarketDataHandler initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing MarketDataHandler: {e}")
            raise
    
    async def stop(self) -> None:
        """Stop the market data handler"""
        try:
            # Unsubscribe from all symbols
            for symbol in list(self.subscribed_symbols):
                await self.unsubscribe(symbol)
            
            # Stop data sources
            await self._stop_data_sources()
            
            logger.info("MarketDataHandler stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping MarketDataHandler: {e}")
    
    async def _initialize_data_sources(self) -> None:
        """Initialize data source connections"""
        try:
            if self.is_simulation:
                # Initialize simulation data source
                self.data_sources['simulation'] = SimulationDataSource()
                await self.data_sources['simulation'].initialize()
            else:
                # Initialize real data sources
                await self._initialize_alpaca_data_source()
                await self._initialize_eod_data_source()
                
        except Exception as e:
            logger.error(f"Error initializing data sources: {e}")
            raise
    
    async def _initialize_alpaca_data_source(self) -> None:
        """Initialize Alpaca data source integration"""
        try:
            # In a real implementation, this would integrate with the existing
            # alpaca_md.py module for real-time data feeds
            
            # For now, create a placeholder
            self.data_sources['alpaca'] = AlpacaDataSource(self.config)
            await self.data_sources['alpaca'].initialize()
            
        except Exception as e:
            logger.error(f"Error initializing Alpaca data source: {e}")
            raise
    
    async def _initialize_eod_data_source(self) -> None:
        """Initialize EOD Historical Data source integration"""
        try:
            # Import and initialize EOD integration
            from ..integration.eod_integration import EODIntegration
            
            self.data_sources['eod'] = EODIntegration(self.config)
            await self.data_sources['eod'].initialize()
            
            logger.info("EOD data source initialized successfully")
            
        except Exception as e:
            logger.error(f"Error initializing EOD data source: {e}")
            # Don't raise - EOD is optional, continue without it
    
    async def _stop_data_sources(self) -> None:
        """Stop all data sources"""
        try:
            for source_name, source in self.data_sources.items():
                if source:
                    await source.stop()
                    
        except Exception as e:
            logger.error(f"Error stopping data sources: {e}")
    
    async def _data_processing_loop(self) -> None:
        """Main data processing loop for real-time feeds"""
        logger.info("Starting data processing loop...")
        
        while True:
            try:
                # Process data from all sources
                for source_name, source in self.data_sources.items():
                    if source and hasattr(source, 'get_latest_data'):
                        data_updates = await source.get_latest_data()
                        
                        for symbol, data in data_updates.items():
                            await self._process_data_update(symbol, data)
                
                # Short sleep to prevent busy waiting
                await asyncio.sleep(0.1)
                
            except Exception as e:
                logger.error(f"Error in data processing loop: {e}")
                self.stats['errors'] += 1
                await asyncio.sleep(1)  # Longer sleep on error
    
    async def _process_data_update(self, symbol: str, data: Dict[str, Any]) -> None:
        """Process a data update for a symbol"""
        try:
            # Update latest data
            self.latest_data[symbol] = data
            
            # Store in historical data (limited history)
            if symbol not in self.historical_data:
                self.historical_data[symbol] = []
            
            self.historical_data[symbol].append(data)
            
            # Keep only last 1000 data points
            if len(self.historical_data[symbol]) > 1000:
                self.historical_data[symbol] = self.historical_data[symbol][-1000:]
            
            # Update stats
            self.stats['data_points_processed'] += 1
            self.stats['last_update'] = datetime.utcnow()
            
            # Notify subscribers
            await self._notify_subscribers(symbol, data)
            
        except Exception as e:
            logger.error(f"Error processing data update for {symbol}: {e}")
            self.stats['errors'] += 1
    
    async def _notify_subscribers(self, symbol: str, data: Dict[str, Any]) -> None:
        """Notify subscribers of data updates"""
        try:
            if symbol in self.subscribers:
                for callback in self.subscribers[symbol]:
                    try:
                        await callback(symbol, data)
                    except Exception as e:
                        logger.error(f"Error in subscriber callback: {e}")
                        
        except Exception as e:
            logger.error(f"Error notifying subscribers: {e}")
    
    # Public API methods
    
    async def subscribe(self, symbol: str, callback: callable = None) -> bool:
        """Subscribe to market data for a symbol"""
        try:
            symbol = symbol.upper()
            
            if symbol in self.subscribed_symbols:
                logger.info(f"Already subscribed to {symbol}")
                return True
            
            # Add to subscriptions
            self.subscribed_symbols.add(symbol)
            
            if callback:
                if symbol not in self.subscribers:
                    self.subscribers[symbol] = []
                self.subscribers[symbol].append(callback)
            
            # Subscribe with data sources
            for source in self.data_sources.values():
                if source and hasattr(source, 'subscribe'):
                    await source.subscribe(symbol)
            
            self.stats['subscriptions'] += 1
            logger.info(f"Subscribed to market data: {symbol}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error subscribing to {symbol}: {e}")
            return False
    
    async def unsubscribe(self, symbol: str) -> bool:
        """Unsubscribe from market data for a symbol"""
        try:
            symbol = symbol.upper()
            
            if symbol not in self.subscribed_symbols:
                logger.warning(f"Not subscribed to {symbol}")
                return False
            
            # Remove from subscriptions
            self.subscribed_symbols.discard(symbol)
            
            if symbol in self.subscribers:
                del self.subscribers[symbol]
            
            # Unsubscribe from data sources
            for source in self.data_sources.values():
                if source and hasattr(source, 'unsubscribe'):
                    await source.unsubscribe(symbol)
            
            self.stats['subscriptions'] -= 1
            logger.info(f"Unsubscribed from market data: {symbol}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error unsubscribing from {symbol}: {e}")
            return False
    
    async def get_latest_data(self, symbol: str = None) -> Dict[str, Dict[str, Any]]:
        """Get latest market data"""
        if symbol:
            symbol = symbol.upper()
            
            # If no cached data, try to fetch from EOD
            if symbol not in self.latest_data and 'eod' in self.data_sources and self.data_sources['eod']:
                try:
                    eod_quote = await self.data_sources['eod'].get_quote(symbol)
                    if eod_quote:
                        self.latest_data[symbol] = eod_quote
                        logger.info(f"Fetched fresh quote for {symbol} from EOD")
                except Exception as e:
                    logger.error(f"Error fetching quote from EOD for {symbol}: {e}")
            
            return {symbol: self.latest_data.get(symbol, {})}
        else:
            return self.latest_data.copy()
    
    async def get_latest_updates(self) -> Dict[str, Dict[str, Any]]:
        """Get latest data updates for all subscribed symbols"""
        try:
            if self.is_simulation:
                # Generate simulation data
                return await self._generate_simulation_data()
            else:
                # Return real data
                return self.latest_data.copy()
                
        except Exception as e:
            logger.error(f"Error getting latest updates: {e}")
            return {}
    
    async def _generate_simulation_data(self) -> Dict[str, Dict[str, Any]]:
        """Generate simulated market data"""
        try:
            simulation_data = {}
            
            for symbol in self.subscribed_symbols:
                # Generate mock price data
                base_price = 100.0  # Base price
                
                # Simple price simulation
                import random
                price_change = random.uniform(-2.0, 2.0)
                current_price = base_price + price_change
                
                simulation_data[symbol] = {
                    'symbol': symbol,
                    'price': current_price,
                    'bid': current_price - 0.01,
                    'ask': current_price + 0.01,
                    'volume': random.randint(1000, 10000),
                    'timestamp': datetime.utcnow(),
                    'source': 'simulation'
                }
                
                # Update latest data
                self.latest_data[symbol] = simulation_data[symbol]
            
            return simulation_data
            
        except Exception as e:
            logger.error(f"Error generating simulation data: {e}")
            return {}
    
    async def get_historical_data(self, symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get historical data for a symbol"""
        symbol = symbol.upper()
        
        # Try cached data first
        if symbol in self.historical_data and len(self.historical_data[symbol]) >= limit:
            return self.historical_data[symbol][-limit:]
        
        # If not enough cached data, try EOD
        if 'eod' in self.data_sources and self.data_sources['eod']:
            try:
                eod_data = await self.data_sources['eod'].get_historical_data(symbol, limit=limit)
                if eod_data:
                    # Cache the data
                    self.historical_data[symbol] = eod_data
                    logger.info(f"Fetched historical data for {symbol} from EOD")
                    return eod_data
            except Exception as e:
                logger.error(f"Error fetching historical data from EOD for {symbol}: {e}")
        
        # Return cached data if available, otherwise empty list
        return self.historical_data.get(symbol, [])[-limit:]
    
    def get_subscribed_symbols(self) -> List[str]:
        """Get list of subscribed symbols"""
        return list(self.subscribed_symbols)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get market data handler statistics"""
        return self.stats.copy()


class SimulationDataSource:
    """Simulation data source for testing"""
    
    async def initialize(self):
        pass
    
    async def stop(self):
        pass


class AlpacaDataSource:
    """Alpaca data source integration"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
    
    async def initialize(self):
        # Initialize Alpaca data connection
        pass
    
    async def stop(self):
        pass
    
    async def subscribe(self, symbol: str):
        # Subscribe to Alpaca data feed
        pass
    
    async def unsubscribe(self, symbol: str):
        # Unsubscribe from Alpaca data feed
        pass
    
    async def get_latest_data(self) -> Dict[str, Dict[str, Any]]:
        # Get latest data from Alpaca
        return {}