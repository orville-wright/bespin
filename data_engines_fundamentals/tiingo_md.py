#!/usr/bin/env python3
import requests
import requests.exceptions
import pandas as pd
import logging
import os
import dotenv
from datetime import datetime, timedelta
from rich import print

class tiingo_md:
    """
    Tiingo.com API data extractor for comprehensive financial data
    Provides daily prices, fundamentals, news, and real-time data
    Free tier: 30+ years of stock data, 5 years of fundamentals
    Requires Tiingo API token (free at https://api.tiingo.com/account/api/token)
    """
    
    def __init__(self, instance_id, global_args=None):
        self.instance_id = instance_id
        self.args = global_args or {}
        self.base_url = "https://api.tiingo.com/tiingo"
        
        # Load environment variables from .env file
        load_status = dotenv.load_dotenv()
        if load_status is False:
            logging.warning('Environment variables not loaded from .env file.')
        
        # Get API token from environment
        self.api_token = os.getenv('TIINGO_API_TOKEN')
        if not self.api_token:
            logging.warning("TIINGO_API_TOKEN not found in environment. Some features may not work.")
        
        self.session = requests.Session()
        if self.api_token:
            self.session.headers.update({
                'Content-Type': 'application/json',
                'Authorization': f'Token {self.api_token}'
            })
        
        self.prices_df = pd.DataFrame()
        self.fundamentals_df = pd.DataFrame()
        self.news_df = pd.DataFrame()
        
        logging.info(f"Tiingo financial data extractor initialized - Instance #{instance_id}")
    
    def get_ticker_metadata(self, symbol):
        """
        Get metadata for a ticker symbol including description, exchange, etc.
        """
        if not self.api_token:
            logging.error("Tiingo API token required")
            return {}
        
        try:
            url = f"{self.base_url}/daily/{symbol.upper()}"
            response = self.session.get(url)
            response.raise_for_status()
            
            data = response.json()
            if data:
                return {
                    'ticker': data.get('ticker'),
                    'name': data.get('name'),
                    'description': data.get('description'),
                    'start_date': data.get('startDate'),
                    'end_date': data.get('endDate'),
                    'exchange_code': data.get('exchangeCode')
                }
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching ticker metadata for {symbol}: {e}")
            return {}
    
    def get_daily_prices(self, symbol, start_date=None, end_date=None, frequency='daily'):
        """
        Get historical daily prices for a symbol
        frequency: daily, weekly, monthly, annually
        """
        if not self.api_token:
            logging.error("Tiingo API token required")
            return pd.DataFrame()
        
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/daily/{symbol.upper()}/prices"
            params = {
                'startDate': start_date,
                'endDate': end_date,
                'resampleFreq': frequency,
                'format': 'json'
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = symbol.upper()
                
                # Reorder columns
                column_order = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'adjOpen', 'adjHigh', 'adjLow', 'adjClose', 'adjVolume', 'divCash', 'splitFactor']
                existing_cols = [col for col in column_order if col in df.columns]
                df = df[existing_cols]
                
                self.prices_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching daily prices for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_latest_prices(self, symbols):
        """
        Get latest prices for multiple symbols
        symbols: list of ticker symbols or single symbol
        """
        if not self.api_token:
            logging.error("Tiingo API token required")
            return pd.DataFrame()
        
        try:
            if isinstance(symbols, str):
                symbols = [symbols]
            
            symbols_str = ','.join([s.upper() for s in symbols])
            url = f"{self.base_url}/daily/prices"
            params = {'tickers': symbols_str}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching latest prices: {e}")
            return pd.DataFrame()
    
    def get_fundamentals_definitions(self):
        """
        Get definitions for fundamental data fields
        """
        if not self.api_token:
            logging.error("Tiingo API token required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/fundamentals/definitions"
            response = self.session.get(url)
            response.raise_for_status()
            
            data = response.json()
            if data:
                return pd.DataFrame(data)
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching fundamentals definitions: {e}")
            return pd.DataFrame()
    
    def get_fundamentals_daily(self, symbol, start_date=None, end_date=None):
        """
        Get daily fundamental data for a symbol
        Note: Fundamental data may not be available for all symbols on free tier
        """
        if not self.api_token:
            logging.error("Tiingo API token required")
            return pd.DataFrame()
        
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/fundamentals/{symbol.upper()}/daily"
            params = {
                'startDate': start_date,
                'endDate': end_date,
                'format': 'json'
            }
            
            response = self.session.get(url, params=params)
            
            # Handle specific HTTP error codes
            if response.status_code == 400:
                logging.warning(f"Fundamental data not available for {symbol.upper()} (400 Bad Request) - may not be supported on free tier or for this symbol")
                return pd.DataFrame()
            elif response.status_code == 404:
                logging.warning(f"Fundamental data endpoint not found for {symbol.upper()} (404 Not Found)")
                return pd.DataFrame()
            elif response.status_code == 403:
                logging.warning(f"Fundamental data access denied for {symbol.upper()} (403 Forbidden) - may require premium subscription")
                return pd.DataFrame()
            
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = symbol.upper()
                
                self.fundamentals_df = df
                return df
            
            return pd.DataFrame()
            
        except requests.exceptions.HTTPError as http_err:
            logging.warning(f"HTTP error fetching fundamentals for {symbol.upper()}: {http_err}")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Error fetching fundamentals daily data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_fundamentals_statements(self, symbol, statement_type='income', frequency='quarterly'):
        """
        Get financial statements for a symbol
        statement_type: income, balance, cash
        frequency: quarterly, annual
        Note: Statement data may not be available for all symbols on free tier
        """
        if not self.api_token:
            logging.error("Tiingo API token required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/fundamentals/{symbol.upper()}/statements"
            params = {
                'statementType': statement_type,
                'frequency': frequency,
                'format': 'json'
            }
            
            response = self.session.get(url, params=params)
            
            # Handle specific HTTP error codes
            if response.status_code == 400:
                logging.warning(f"{statement_type.capitalize()} statements not available for {symbol.upper()} (400 Bad Request) - may not be supported on free tier")
                return pd.DataFrame()
            elif response.status_code == 404:
                logging.warning(f"{statement_type.capitalize()} statements endpoint not found for {symbol.upper()} (404 Not Found)")
                return pd.DataFrame()
            elif response.status_code == 403:
                logging.warning(f"{statement_type.capitalize()} statements access denied for {symbol.upper()} (403 Forbidden) - may require premium subscription")
                return pd.DataFrame()
            
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['symbol'] = symbol.upper()
                
                return df
            
            return pd.DataFrame()
            
        except requests.exceptions.HTTPError as http_err:
            logging.warning(f"HTTP error fetching {statement_type} statements for {symbol.upper()}: {http_err}")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Error fetching {statement_type} statements for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_news(self, symbols=None, tags=None, sources=None, start_date=None, end_date=None, limit=100):
        """
        Get curated financial news
        symbols: list of tickers to filter by
        tags: list of tags to filter by  
        sources: list of news sources to filter by
        """
        if not self.api_token:
            logging.error("Tiingo API token required")
            return pd.DataFrame()
        
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/news"
            params = {
                'startDate': start_date,
                'endDate': end_date,
                'limit': limit,
                'format': 'json'
            }
            
            if symbols:
                if isinstance(symbols, str):
                    symbols = [symbols]
                params['tickers'] = ','.join([s.upper() for s in symbols])
            
            if tags:
                if isinstance(tags, str):
                    tags = [tags]
                params['tags'] = ','.join(tags)
            
            if sources:
                if isinstance(sources, str):
                    sources = [sources]
                params['sources'] = ','.join(sources)
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['publishedDate'] = pd.to_datetime(df['publishedDate'])
                
                self.news_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching news: {e}")
            return pd.DataFrame()
    
    def get_crypto_metadata(self, symbol):
        """
        Get metadata for a cryptocurrency symbol
        """
        if not self.api_token:
            logging.error("Tiingo API token required")
            return {}
        
        try:
            url = f"https://api.tiingo.com/tiingo/crypto/{symbol.lower()}"
            response = self.session.get(url)
            response.raise_for_status()
            
            data = response.json()
            if data:
                return {
                    'ticker': data.get('ticker'),
                    'name': data.get('name'),
                    'description': data.get('description'),
                    'start_date': data.get('startDate'),
                    'end_date': data.get('endDate')
                }
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching crypto metadata for {symbol}: {e}")
            return {}
    
    def get_crypto_prices(self, symbol, start_date=None, end_date=None, frequency='1Day'):
        """
        Get historical cryptocurrency prices
        frequency: 1min, 5min, 15min, 30min, 1hour, 4hour, 1Day
        """
        if not self.api_token:
            logging.error("Tiingo API token required")
            return pd.DataFrame()
        
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            url = f"https://api.tiingo.com/tiingo/crypto/prices"
            params = {
                'tickers': symbol.lower(),
                'startDate': start_date,
                'endDate': end_date,
                'resampleFreq': frequency,
                'format': 'json'
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data[0]['priceData'])  # First ticker's data
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = symbol.upper()
                
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching crypto prices for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_market_summary(self, symbols_list):
        """
        Get market summary for multiple symbols with key metrics
        """
        summary_data = []
        
        for symbol in symbols_list:
            try:
                # Get metadata
                metadata = self.get_ticker_metadata(symbol)
                
                # Get latest price
                latest_prices = self.get_latest_prices(symbol)
                
                if not latest_prices.empty:
                    latest = latest_prices.iloc[0]
                    summary_data.append({
                        'symbol': symbol.upper(),
                        'name': metadata.get('name', 'N/A'),
                        'exchange': metadata.get('exchangeCode', 'N/A'),
                        'close': latest.get('close'),
                        'open': latest.get('open'),
                        'high': latest.get('high'),
                        'low': latest.get('low'),
                        'volume': latest.get('volume'),
                        'adj_close': latest.get('adjClose'),
                        'date': latest.get('date')
                    })
                    
            except Exception as e:
                logging.error(f"Error in market summary for {symbol}: {e}")
                continue
        
        return pd.DataFrame(summary_data)