"""
EOD Historical Data Integration

Integrates eodhistoricaldata_md.py with the trading platform's market data handler.
Provides seamless access to EOD's market data, fundamentals, and technical indicators.
"""

import asyncio
import logging
import sys
import os
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
import importlib.util

# Add parent directory to path for importing eodhistoricaldata_md
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

logger = logging.getLogger(__name__)


class EODIntegration:
    """
    Integration bridge with eodhistoricaldata_md.py
    
    Provides access to EOD Historical Data API through the existing data extractor
    while maintaining compatibility with the trading platform's market data handler.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.eod_instance = None
        self.is_initialized = False
        self.api_calls_today = 0
        self.api_limit = 20  # Free tier limit
        
        # Supported data types
        self.supported_data_types = [
            'eod',           # End of day data
            'realtime',      # Real-time quotes
            'intraday',      # Intraday data
            'fundamentals',  # Company fundamentals
            'dividends',     # Dividend data
            'technical',     # Technical indicators
            'exchanges'      # Exchange information
        ]
        
        logger.info("EOD Historical Data integration initialized")
    
    async def initialize(self) -> bool:
        """Initialize connection to eodhistoricaldata_md"""
        try:
            # Import eodhistoricaldata_md module
            eod_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'eodhistoricaldata_md.py')
            
            if os.path.exists(eod_path):
                spec = importlib.util.spec_from_file_location("eodhistoricaldata_md", eod_path)
                eod_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(eod_module)
                
                # Create EOD instance
                self.eod_instance = eod_module.eodhistoricaldata_md(
                    instance_id="trading_platform_integration",
                    global_args=self.config
                )
                
                logger.info("Successfully loaded eodhistoricaldata_md module")
            else:
                logger.error("eodhistoricaldata_md.py not found")
                return False
            
            self.is_initialized = True
            logger.info("EOD integration initialized successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing EOD integration: {e}")
            return False
    
    async def get_quote(self, symbol: str, exchange: str = 'US') -> Optional[Dict[str, Any]]:
        """Get real-time quote for a symbol"""
        try:
            if not self.is_initialized:
                await self.initialize()
            
            if self.api_calls_today >= self.api_limit:
                logger.warning("EOD API call limit reached for today")
                return None
            
            # Get real-time data
            df = self.eod_instance.get_realtime_data([symbol], exchange)
            self.api_calls_today += 1
            
            if not df.empty:
                data = df.iloc[0].to_dict()
                
                # Standardize the response format
                quote = {
                    'symbol': symbol,
                    'exchange': exchange,
                    'price': float(data.get('close', 0)),
                    'bid': float(data.get('close', 0)) - 0.01,  # EOD doesn't provide bid/ask
                    'ask': float(data.get('close', 0)) + 0.01,
                    'volume': int(data.get('volume', 0)),
                    'timestamp': data.get('timestamp', datetime.utcnow()),
                    'source': 'eod_historical_data',
                    'change': float(data.get('change', 0)),
                    'change_percent': float(data.get('change_p', 0))
                }
                
                return quote
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting quote from EOD for {symbol}: {e}")
            return None
    
    async def get_historical_data(self, symbol: str, timeframe: str = "1day", limit: int = 100, exchange: str = 'US') -> List[Dict[str, Any]]:
        """Get historical market data"""
        try:
            if not self.is_initialized:
                await self.initialize()
            
            if self.api_calls_today >= self.api_limit:
                logger.warning("EOD API call limit reached for today")
                return []
            
            # Calculate date range based on limit
            date_to = datetime.now()
            date_from = date_to - timedelta(days=limit)
            
            # Get EOD data
            df = self.eod_instance.get_eod_data(
                symbol, 
                exchange, 
                date_from.strftime('%Y-%m-%d'),
                date_to.strftime('%Y-%m-%d'),
                period='d'  # Daily data
            )
            self.api_calls_today += 1
            
            if not df.empty:
                historical_data = []
                for _, row in df.iterrows():
                    data_point = {
                        'symbol': symbol,
                        'exchange': exchange,
                        'timestamp': row['date'],
                        'open': float(row.get('open', 0)),
                        'high': float(row.get('high', 0)),
                        'low': float(row.get('low', 0)),
                        'close': float(row.get('close', 0)),
                        'adjusted_close': float(row.get('adjusted_close', row.get('close', 0))),
                        'volume': int(row.get('volume', 0)),
                        'source': 'eod_historical_data'
                    }
                    historical_data.append(data_point)
                
                return historical_data[-limit:]  # Return last 'limit' records
            
            return []
            
        except Exception as e:
            logger.error(f"Error getting historical data from EOD for {symbol}: {e}")
            return []
    
    async def get_intraday_data(self, symbol: str, interval: str = '5m', exchange: str = 'US') -> List[Dict[str, Any]]:
        """Get intraday data for a symbol"""
        try:
            if not self.is_initialized:
                await self.initialize()
            
            if self.api_calls_today >= self.api_limit:
                logger.warning("EOD API call limit reached for today")
                return []
            
            # Get intraday data (consumes 5 API calls)
            df = self.eod_instance.get_intraday_data(symbol, exchange, interval)
            self.api_calls_today += 5  # Intraday endpoint uses 5 calls
            
            if not df.empty:
                intraday_data = []
                for _, row in df.iterrows():
                    data_point = {
                        'symbol': symbol,
                        'exchange': exchange,
                        'timestamp': row['datetime'],
                        'open': float(row.get('open', 0)),
                        'high': float(row.get('high', 0)),
                        'low': float(row.get('low', 0)),
                        'close': float(row.get('close', 0)),
                        'volume': int(row.get('volume', 0)),
                        'source': 'eod_historical_data'
                    }
                    intraday_data.append(data_point)
                
                return intraday_data
            
            return []
            
        except Exception as e:
            logger.error(f"Error getting intraday data from EOD for {symbol}: {e}")
            return []
    
    async def get_fundamentals(self, symbol: str, exchange: str = 'US') -> Optional[Dict[str, Any]]:
        """Get fundamental data for a symbol"""
        try:
            if not self.is_initialized:
                await self.initialize()
            
            if self.api_calls_today >= self.api_limit:
                logger.warning("EOD API call limit reached for today")
                return None
            
            # Get fundamentals (consumes 10 API calls)
            fundamentals = self.eod_instance.get_fundamentals(symbol, exchange)
            self.api_calls_today += 10  # Fundamentals endpoint uses 10 calls
            
            if fundamentals:
                # Add timestamp and source
                fundamentals['timestamp'] = datetime.utcnow()
                fundamentals['source'] = 'eod_historical_data'
                
                return fundamentals
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting fundamentals from EOD for {symbol}: {e}")
            return None
    
    async def get_technical_indicators(self, symbol: str, function: str = 'sma', period: int = 50, exchange: str = 'US') -> List[Dict[str, Any]]:
        """Get technical indicators for a symbol"""
        try:
            if not self.is_initialized:
                await self.initialize()
            
            if self.api_calls_today >= self.api_limit:
                logger.warning("EOD API call limit reached for today")
                return []
            
            # Get technical indicators
            df = self.eod_instance.get_technical_indicators(symbol, exchange, function, period)
            self.api_calls_today += 1
            
            if not df.empty:
                indicators = []
                for _, row in df.iterrows():
                    indicator = {
                        'symbol': symbol,
                        'exchange': exchange,
                        'date': row['date'],
                        'function': function,
                        'period': period,
                        'value': float(row.get(function, 0)),
                        'source': 'eod_historical_data'
                    }
                    indicators.append(indicator)
                
                return indicators
            
            return []
            
        except Exception as e:
            logger.error(f"Error getting technical indicators from EOD for {symbol}: {e}")
            return []
    
    async def get_dividend_data(self, symbol: str, exchange: str = 'US') -> List[Dict[str, Any]]:
        """Get dividend data for a symbol"""
        try:
            if not self.is_initialized:
                await self.initialize()
            
            if self.api_calls_today >= self.api_limit:
                logger.warning("EOD API call limit reached for today")
                return []
            
            # Get dividend data
            df = self.eod_instance.get_dividends(symbol, exchange)
            self.api_calls_today += 1
            
            if not df.empty:
                dividends = []
                for _, row in df.iterrows():
                    dividend = {
                        'symbol': symbol,
                        'exchange': exchange,
                        'date': row['date'],
                        'value': float(row.get('value', 0)),
                        'unadjusted_value': float(row.get('unadjusted_value', row.get('value', 0))),
                        'currency': row.get('currency', 'USD'),
                        'source': 'eod_historical_data'
                    }
                    dividends.append(dividend)
                
                return dividends
            
            return []
            
        except Exception as e:
            logger.error(f"Error getting dividend data from EOD for {symbol}: {e}")
            return []
    
    async def get_supported_exchanges(self) -> List[Dict[str, Any]]:
        """Get list of supported exchanges"""
        try:
            if not self.is_initialized:
                await self.initialize()
            
            if self.api_calls_today >= self.api_limit:
                logger.warning("EOD API call limit reached for today")
                return []
            
            # Get exchanges list
            df = self.eod_instance.get_exchanges()
            self.api_calls_today += 1
            
            if not df.empty:
                exchanges = df.to_dict('records')
                for exchange in exchanges:
                    exchange['source'] = 'eod_historical_data'
                
                return exchanges
            
            return []
            
        except Exception as e:
            logger.error(f"Error getting exchanges from EOD: {e}")
            return []
    
    async def get_bulk_quotes(self, symbols: List[str], exchange: str = 'US') -> Dict[str, Any]:
        """Get quotes for multiple symbols"""
        try:
            quotes = {}
            
            for symbol in symbols:
                if self.api_calls_today >= self.api_limit:
                    logger.warning("EOD API call limit reached, stopping bulk request")
                    break
                
                quote = await self.get_quote(symbol, exchange)
                if quote:
                    quotes[symbol] = quote
                else:
                    quotes[symbol] = {'error': 'No data available'}
                
                # Small delay between requests to avoid rate limiting
                await asyncio.sleep(0.1)
            
            return quotes
            
        except Exception as e:
            logger.error(f"Error getting bulk quotes from EOD: {e}")
            return {}
    
    def get_api_usage(self) -> Dict[str, Any]:
        """Get current API usage statistics"""
        return {
            'calls_today': self.api_calls_today,
            'limit': self.api_limit,
            'remaining': max(0, self.api_limit - self.api_calls_today),
            'reset_time': 'midnight UTC'
        }
    
    def get_supported_data_types(self) -> List[str]:
        """Get list of supported data types"""
        return self.supported_data_types.copy()
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test connection to EOD API"""
        try:
            if not self.is_initialized:
                await self.initialize()
            
            # Test with a demo symbol (AAPL.US works with demo token)
            test_quote = await self.get_quote('AAPL', 'US')
            
            return {
                'status': 'connected' if test_quote else 'failed',
                'test_symbol': 'AAPL.US',
                'api_usage': self.get_api_usage(),
                'supported_types': self.get_supported_data_types(),
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.utcnow()
            }
    
    async def stop(self) -> None:
        """Stop EOD integration and cleanup"""
        try:
            if self.eod_instance and hasattr(self.eod_instance, 'session'):
                self.eod_instance.session.close()
            
            self.is_initialized = False
            logger.info("EOD integration stopped")
            
        except Exception as e:
            logger.error(f"Error stopping EOD integration: {e}")