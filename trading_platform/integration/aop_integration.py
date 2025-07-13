"""
Integration with aop.py orchestrator

Provides bridge between trading platform and existing aop.py data orchestrator.
Allows trading platform to leverage existing data engines and infrastructure.
"""

import asyncio
import logging
import sys
import os
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
import importlib.util

# Add parent directory to path for importing aop
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

logger = logging.getLogger(__name__)


class AOPIntegration:
    """
    Integration bridge with aop.py orchestrator
    
    Provides access to existing data engines and orchestration capabilities
    while maintaining clean separation between trading platform and data infrastructure.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.aop_instance = None
        self.data_engines = {}
        self.is_initialized = False
        
        # Available data engines from aop.py
        self.available_engines = [
            'alpaca_md',
            'alphavantage_md', 
            'polygon_md',
            'finnhub_md',
            'tiingo_md',
            'sec_md',
            'fred_md',
            'eodhistoricaldata_md'
        ]
        
        # News engines
        self.news_engines = [
            'barrons_news',
            'benzinga_news',
            'forbes_news',
            'fxstreet_news',
            'investing_news'
        ]
        
        logger.info("AOPIntegration initialized")
    
    async def initialize(self) -> bool:
        """Initialize connection to aop.py orchestrator"""
        try:
            # Import aop module if available
            aop_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'aop.py')
            
            if os.path.exists(aop_path):
                spec = importlib.util.spec_from_file_location("aop", aop_path)
                aop_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(aop_module)
                
                # Store reference for later use
                self.aop_module = aop_module
                logger.info("Successfully loaded aop.py module")
            else:
                logger.warning("aop.py not found, using mock integration")
                return await self._initialize_mock_integration()
            
            # Initialize available data engines
            await self._initialize_data_engines()
            
            self.is_initialized = True
            logger.info("AOP integration initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing AOP integration: {e}")
            return False
    
    async def _initialize_mock_integration(self) -> bool:
        """Initialize mock integration for testing when aop.py not available"""
        try:
            # Create mock data engines
            for engine in self.available_engines:
                self.data_engines[engine] = MockDataEngine(engine)
            
            for engine in self.news_engines:
                self.data_engines[engine] = MockNewsEngine(engine)
            
            logger.info("Mock AOP integration initialized")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing mock integration: {e}")
            return False
    
    async def _initialize_data_engines(self) -> None:
        """Initialize available data engines"""
        try:
            # This would initialize the actual data engines from aop.py
            # For now, create placeholders
            
            for engine_name in self.available_engines:
                try:
                    # In a real implementation, this would instantiate the actual engine classes
                    self.data_engines[engine_name] = DataEngineWrapper(engine_name, self.config)
                    logger.info(f"Initialized data engine: {engine_name}")
                except Exception as e:
                    logger.warning(f"Failed to initialize {engine_name}: {e}")
            
            for engine_name in self.news_engines:
                try:
                    self.data_engines[engine_name] = NewsEngineWrapper(engine_name, self.config)
                    logger.info(f"Initialized news engine: {engine_name}")
                except Exception as e:
                    logger.warning(f"Failed to initialize {engine_name}: {e}")
                    
        except Exception as e:
            logger.error(f"Error initializing data engines: {e}")
    
    async def get_market_data(self, symbol: str, source: str = "alpaca") -> Optional[Dict[str, Any]]:
        """Get market data from specified source"""
        try:
            if not self.is_initialized:
                await self.initialize()
            
            if source not in self.data_engines:
                logger.error(f"Data source not available: {source}")
                return None
            
            engine = self.data_engines[source]
            return await engine.get_quote(symbol)
            
        except Exception as e:
            logger.error(f"Error getting market data from {source}: {e}")
            return None
    
    async def get_historical_data(self, symbol: str, timeframe: str = "1day", limit: int = 100, source: str = "alpaca") -> List[Dict[str, Any]]:
        """Get historical market data"""
        try:
            if not self.is_initialized:
                await self.initialize()
            
            if source not in self.data_engines:
                logger.error(f"Data source not available: {source}")
                return []
            
            engine = self.data_engines[source]
            return await engine.get_historical_data(symbol, timeframe, limit)
            
        except Exception as e:
            logger.error(f"Error getting historical data from {source}: {e}")
            return []
    
    async def get_news_data(self, symbol: str = None, source: str = "benzinga") -> List[Dict[str, Any]]:
        """Get news data from specified source"""
        try:
            if not self.is_initialized:
                await self.initialize()
            
            if source not in self.data_engines:
                logger.error(f"News source not available: {source}")
                return []
            
            engine = self.data_engines[source]
            return await engine.get_news(symbol)
            
        except Exception as e:
            logger.error(f"Error getting news data from {source}: {e}")
            return []
    
    async def run_full_data_extraction(self) -> Dict[str, Any]:
        """Run full data extraction using aop.py capabilities"""
        try:
            if not self.is_initialized:
                await self.initialize()
            
            # This would trigger the existing aop.py data extraction workflows
            # For demonstration, return mock results
            
            results = {
                'market_data': await self._extract_market_data(),
                'news_data': await self._extract_news_data(),
                'economic_data': await self._extract_economic_data(),
                'extraction_time': datetime.utcnow(),
                'status': 'completed'
            }
            
            return results
            
        except Exception as e:
            logger.error(f"Error running full data extraction: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    async def _extract_market_data(self) -> Dict[str, Any]:
        """Extract market data from all available sources"""
        market_data = {}
        
        for engine_name, engine in self.data_engines.items():
            if engine_name in self.available_engines:
                try:
                    data = await engine.get_bulk_quotes(['AAPL', 'GOOGL', 'TSLA', 'MSFT'])
                    market_data[engine_name] = data
                except Exception as e:
                    logger.error(f"Error extracting from {engine_name}: {e}")
        
        return market_data
    
    async def _extract_news_data(self) -> Dict[str, Any]:
        """Extract news data from all available sources"""
        news_data = {}
        
        for engine_name, engine in self.data_engines.items():
            if engine_name in self.news_engines:
                try:
                    data = await engine.get_latest_news()
                    news_data[engine_name] = data
                except Exception as e:
                    logger.error(f"Error extracting news from {engine_name}: {e}")
        
        return news_data
    
    async def _extract_economic_data(self) -> Dict[str, Any]:
        """Extract economic data (FRED, etc.)"""
        try:
            if 'fred_md' in self.data_engines:
                engine = self.data_engines['fred_md']
                return await engine.get_economic_snapshot()
            else:
                return {'status': 'fred_not_available'}
                
        except Exception as e:
            logger.error(f"Error extracting economic data: {e}")
            return {'error': str(e)}
    
    def get_available_engines(self) -> Dict[str, List[str]]:
        """Get list of available data engines"""
        return {
            'market_data': self.available_engines,
            'news': self.news_engines,
            'status': 'initialized' if self.is_initialized else 'not_initialized'
        }
    
    async def stop(self) -> None:
        """Stop AOP integration and cleanup"""
        try:
            # Cleanup data engines
            for engine in self.data_engines.values():
                if hasattr(engine, 'stop'):
                    await engine.stop()
            
            self.is_initialized = False
            logger.info("AOP integration stopped")
            
        except Exception as e:
            logger.error(f"Error stopping AOP integration: {e}")


class DataEngineWrapper:
    """Wrapper for aop.py data engines"""
    
    def __init__(self, engine_name: str, config: Dict[str, Any]):
        self.engine_name = engine_name
        self.config = config
        self.engine_instance = None
    
    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        """Get quote for symbol"""
        # Mock implementation - would call actual engine
        return {
            'symbol': symbol,
            'price': 100.0,
            'bid': 99.95,
            'ask': 100.05,
            'volume': 1000000,
            'timestamp': datetime.utcnow(),
            'source': self.engine_name
        }
    
    async def get_historical_data(self, symbol: str, timeframe: str, limit: int) -> List[Dict[str, Any]]:
        """Get historical data"""
        # Mock implementation
        return [
            {
                'symbol': symbol,
                'timestamp': datetime.utcnow(),
                'open': 99.0,
                'high': 101.0,
                'low': 98.5,
                'close': 100.0,
                'volume': 500000,
                'source': self.engine_name
            }
        ]
    
    async def get_bulk_quotes(self, symbols: List[str]) -> Dict[str, Any]:
        """Get quotes for multiple symbols"""
        quotes = {}
        for symbol in symbols:
            quotes[symbol] = await self.get_quote(symbol)
        return quotes


class NewsEngineWrapper:
    """Wrapper for aop.py news engines"""
    
    def __init__(self, engine_name: str, config: Dict[str, Any]):
        self.engine_name = engine_name
        self.config = config
    
    async def get_news(self, symbol: str = None) -> List[Dict[str, Any]]:
        """Get news articles"""
        # Mock implementation
        return [
            {
                'title': f'Market Update from {self.engine_name}',
                'summary': 'Sample news article...',
                'url': f'https://{self.engine_name}.com/article',
                'published_at': datetime.utcnow(),
                'symbols': [symbol] if symbol else ['GENERAL'],
                'source': self.engine_name
            }
        ]
    
    async def get_latest_news(self) -> List[Dict[str, Any]]:
        """Get latest news articles"""
        return await self.get_news()


class MockDataEngine:
    """Mock data engine for testing"""
    
    def __init__(self, name: str):
        self.name = name
    
    async def get_quote(self, symbol: str) -> Dict[str, Any]:
        return {
            'symbol': symbol,
            'price': 100.0,
            'source': f'mock_{self.name}'
        }


class MockNewsEngine:
    """Mock news engine for testing"""
    
    def __init__(self, name: str):
        self.name = name
    
    async def get_news(self, symbol: str = None) -> List[Dict[str, Any]]:
        return [{'title': f'Mock news from {self.name}', 'source': f'mock_{self.name}'}]