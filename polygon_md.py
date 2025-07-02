#!/usr/bin/env python3
import requests
import pandas as pd
import logging
import os
import dotenv
from datetime import datetime, timedelta
from rich import print

class polygon_md:
    """
    Polygon.io API data extractor for real-time and historical market data
    Requires Polygon API key (free tier available at https://polygon.io)
    """
    
    def __init__(self, instance_id, global_args=None):
        self.instance_id = instance_id
        self.args = global_args or {}
        self.base_url = "https://api.polygon.io"
        
        # Load environment variables from .env file
        load_status = dotenv.load_dotenv()
        if load_status is False:
            logging.warning('Environment variables not loaded from .env file.')
        
        # Get API key from environment
        self.api_key = os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            logging.warning("POLYGON_API_KEY not found in environment. Some features may not work.")
        
        self.session = requests.Session()
        self.quotes_df = pd.DataFrame()
        self.bars_df = pd.DataFrame()
        
        logging.info(f"Polygon.io data extractor initialized - Instance #{instance_id}")
    
    def get_last_quote(self, symbol):
        """
        Get the most recent quote for a symbol
        """
        if not self.api_key:
            logging.error("Polygon API key required")
            return {}
        
        try:
            url = f"{self.base_url}/v2/last/nbbo/{symbol.upper()}"
            params = {'apikey': self.api_key}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'OK':
                results = data.get('results', {})
                return {
                    'symbol': symbol.upper(),
                    'bid': results.get('bid'),
                    'bid_size': results.get('bidSize'),
                    'ask': results.get('ask'),
                    'ask_size': results.get('askSize'),
                    'timestamp': results.get('timestamp'),
                    'spread': results.get('ask', 0) - results.get('bid', 0) if results.get('ask') and results.get('bid') else None
                }
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching last quote for {symbol}: {e}")
            return {}
    
    def get_aggregates(self, symbol, multiplier=1, timespan='day', from_date=None, to_date=None, limit=120):
        """
        Get aggregate bars for a symbol
        timespan: minute, hour, day, week, month, quarter, year
        """
        if not self.api_key:
            logging.error("Polygon API key required")
            return pd.DataFrame()
        
        try:
            if not from_date:
                from_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            if not to_date:
                to_date = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/v2/aggs/ticker/{symbol.upper()}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
            params = {
                'apikey': self.api_key,
                'adjusted': 'true',
                'sort': 'asc',
                'limit': limit
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'OK' and data.get('results'):
                results = data['results']
                
                df = pd.DataFrame(results)
                df.columns = ['volume', 'vw_avg_price', 'open', 'close', 'high', 'low', 'timestamp', 'num_transactions']
                
                # Convert timestamp to datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df['symbol'] = symbol.upper()
                
                # Reorder columns
                df = df[['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'vw_avg_price', 'num_transactions']]
                
                self.bars_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching aggregates for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_daily_open_close(self, symbol, date=None):
        """
        Get open, high, low, close for a specific date
        """
        if not self.api_key:
            logging.error("Polygon API key required")
            return {}
        
        try:
            if not date:
                date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/v1/open-close/{symbol.upper()}/{date}"
            params = {'apikey': self.api_key, 'adjusted': 'true'}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'OK':
                return {
                    'symbol': data.get('symbol'),
                    'date': data.get('from'),
                    'open': data.get('open'),
                    'high': data.get('high'),
                    'low': data.get('low'),
                    'close': data.get('close'),
                    'volume': data.get('volume'),
                    'pre_market': data.get('preMarket'),
                    'after_hours': data.get('afterHours')
                }
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching daily OHLC for {symbol}: {e}")
            return {}
    
    def get_market_holidays(self, year=None):
        """
        Get market holidays
        """
        if not self.api_key:
            logging.error("Polygon API key required")
            return []
        
        try:
            if not year:
                year = datetime.now().year
                
            url = f"{self.base_url}/v1/marketstatus/upcoming"
            params = {'apikey': self.api_key}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get('results', [])
            
        except Exception as e:
            logging.error(f"Error fetching market holidays: {e}")
            return []
    
    def get_market_status(self):
        """
        Get current market status
        """
        if not self.api_key:
            logging.error("Polygon API key required")
            return {}
        
        try:
            url = f"{self.base_url}/v1/marketstatus/now"
            params = {'apikey': self.api_key}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data
            
        except Exception as e:
            logging.error(f"Error fetching market status: {e}")
            return {}
    
    def get_ticker_details(self, symbol):
        """
        Get detailed information about a ticker
        """
        if not self.api_key:
            logging.error("Polygon API key required")
            return {}
        
        try:
            url = f"{self.base_url}/v3/reference/tickers/{symbol.upper()}"
            params = {'apikey': self.api_key}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'OK':
                results = data.get('results', {})
                return {
                    'ticker': results.get('ticker'),
                    'name': results.get('name'),
                    'market': results.get('market'),
                    'locale': results.get('locale'),
                    'primary_exchange': results.get('primary_exchange'),
                    'type': results.get('type'),
                    'currency_name': results.get('currency_name'),
                    'description': results.get('description'),
                    'market_cap': results.get('market_cap'),
                    'share_class_shares_outstanding': results.get('share_class_shares_outstanding'),
                    'weighted_shares_outstanding': results.get('weighted_shares_outstanding')
                }
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching ticker details for {symbol}: {e}")
            return {}