#!/usr/bin/env python3
import requests
import pandas as pd
import logging
import os
import dotenv
from datetime import datetime, timedelta
from rich import print

class marketstack_md:
    """
    Marketstack API data extractor for global stock market data
    Provides real-time quotes, historical data, and intraday data
    Free tier: 100 requests/month (requires API key from https://marketstack.com)
    """
    
    def __init__(self, instance_id, global_args=None):
        self.instance_id = instance_id
        self.args = global_args or {}
        self.base_url = "https://api.marketstack.com/v2"
        
        # Load environment variables from .env file
        load_status = dotenv.load_dotenv()
        if load_status is False:
            logging.warning('Environment variables not loaded from .env file.')
        
        # Get API key from environment
        self.api_key = os.getenv('MARKETSTACK_API_KEY')
        if not self.api_key:
            logging.warning("MARKETSTACK_API_KEY not found in environment. Some features may not work.")
        
        self.session = requests.Session()
        self.eod_df = pd.DataFrame()
        self.intraday_df = pd.DataFrame()
        
        logging.info(f"Marketstack data extractor initialized - Instance #{instance_id}")
    
    def get_eod_latest(self, symbols, limit=100):
        """
        Get latest end-of-day data for symbols
        symbols: string or list of ticker symbols
        """
        if not self.api_key:
            logging.error("Marketstack API key required")
            return pd.DataFrame()
        
        try:
            if isinstance(symbols, str):
                symbols = [symbols]
            
            symbols_str = ','.join([s.upper() for s in symbols])
            
            url = f"{self.base_url}/eod/latest"
            params = {
                'access_key': self.api_key,
                'symbols': symbols_str,
                'limit': limit
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                df['date'] = pd.to_datetime(df['date'])
                
                # Standardize column names
                columns_order = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'exchange']
                existing_cols = [col for col in columns_order if col in df.columns]
                df = df[existing_cols]
                
                self.eod_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching latest EOD data: {e}")
            return pd.DataFrame()
    
    def get_eod_historical(self, symbol, date_from=None, date_to=None, limit=100):
        """
        Get historical end-of-day data for a symbol
        """
        if not self.api_key:
            logging.error("Marketstack API key required")
            return pd.DataFrame()
        
        try:
            if not date_from:
                date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            if not date_to:
                date_to = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/eod"
            params = {
                'access_key': self.api_key,
                'symbols': symbol.upper(),
                'date_from': date_from,
                'date_to': date_to,
                'limit': limit,
                'sort': 'DESC'
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                
                # Standardize column names
                columns_order = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'exchange']
                existing_cols = [col for col in columns_order if col in df.columns]
                df = df[existing_cols]
                
                self.eod_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching historical EOD data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_intraday_latest(self, symbols, interval='1hour', limit=100):
        """
        Get latest intraday data for symbols
        interval: 1min, 5min, 10min, 15min, 30min, 1hour
        Note: Intraday data requires Basic plan or higher
        """
        if not self.api_key:
            logging.error("Marketstack API key required")
            return pd.DataFrame()
        
        try:
            if isinstance(symbols, str):
                symbols = [symbols]
            
            symbols_str = ','.join([s.upper() for s in symbols])
            
            url = f"{self.base_url}/intraday/latest"
            params = {
                'access_key': self.api_key,
                'symbols': symbols_str,
                'interval': interval,
                'limit': limit
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                df['date'] = pd.to_datetime(df['date'])
                
                # Standardize column names
                columns_order = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'exchange']
                existing_cols = [col for col in columns_order if col in df.columns]
                df = df[existing_cols]
                
                self.intraday_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching latest intraday data: {e}")
            return pd.DataFrame()
    
    def get_intraday_historical(self, symbol, date_from=None, date_to=None, interval='1hour', limit=100):
        """
        Get historical intraday data for a symbol
        Note: Intraday data requires Basic plan or higher
        """
        if not self.api_key:
            logging.error("Marketstack API key required")
            return pd.DataFrame()
        
        try:
            if not date_from:
                date_from = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
            if not date_to:
                date_to = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/intraday"
            params = {
                'access_key': self.api_key,
                'symbols': symbol.upper(),
                'date_from': date_from,
                'date_to': date_to,
                'interval': interval,
                'limit': limit,
                'sort': 'DESC'
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                df['date'] = pd.to_datetime(df['date'])
                df = df.sort_values('date')
                
                # Standardize column names
                columns_order = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 'exchange']
                existing_cols = [col for col in columns_order if col in df.columns]
                df = df[existing_cols]
                
                self.intraday_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching historical intraday data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_ticker_info(self, symbol):
        """
        Get ticker information for a symbol
        """
        if not self.api_key:
            logging.error("Marketstack API key required")
            return {}
        
        try:
            url = f"{self.base_url}/tickers/{symbol.upper()}"
            params = {
                'access_key': self.api_key
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                return {
                    'symbol': data.get('symbol'),
                    'name': data.get('name'),
                    'country': data.get('country'),
                    'exchange': data.get('stock_exchange', {}).get('name'),
                    'exchange_mic': data.get('stock_exchange', {}).get('mic'),
                    'currency': data.get('stock_exchange', {}).get('currency'),
                    'timezone': data.get('stock_exchange', {}).get('timezone'),
                    'has_intraday': data.get('has_intraday'),
                    'has_eod': data.get('has_eod')
                }
            
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching ticker info for {symbol}: {e}")
            return {}
    
    def get_exchanges(self, limit=100):
        """
        Get list of supported exchanges
        """
        if not self.api_key:
            logging.error("Marketstack API key required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/exchanges"
            params = {
                'access_key': self.api_key,
                'limit': limit
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching exchanges: {e}")
            return pd.DataFrame()
    
    def get_dividends(self, symbol, date_from=None, date_to=None, limit=100):
        """
        Get dividend data for a symbol
        """
        if not self.api_key:
            logging.error("Marketstack API key required")
            return pd.DataFrame()
        
        try:
            if not date_from:
                date_from = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            if not date_to:
                date_to = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/dividends"
            params = {
                'access_key': self.api_key,
                'symbols': symbol.upper(),
                'date_from': date_from,
                'date_to': date_to,
                'limit': limit
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                df['date'] = pd.to_datetime(df['date'])
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching dividends for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_market_summary(self, symbols_list):
        """
        Get market summary for multiple symbols
        """
        try:
            summary_df = self.get_eod_latest(symbols_list, limit=len(symbols_list))
            
            if not summary_df.empty:
                # Add ticker info for each symbol
                enhanced_data = []
                for _, row in summary_df.iterrows():
                    ticker_info = self.get_ticker_info(row['symbol'])
                    enhanced_row = row.to_dict()
                    enhanced_row.update({
                        'name': ticker_info.get('name', 'N/A'),
                        'country': ticker_info.get('country', 'N/A'),
                        'currency': ticker_info.get('currency', 'N/A')
                    })
                    enhanced_data.append(enhanced_row)
                
                return pd.DataFrame(enhanced_data)
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error getting market summary: {e}")
            return pd.DataFrame()

def main():
    """Example usage of marketstack_md class"""
    try:
        marketstack = marketstack_md(1)
        
        print("========== Marketstack Market Data Test ==========")
        
        test_symbol = "AAPL"
        print(f"Getting latest EOD data for {test_symbol}...")
        eod_data = marketstack.get_eod_latest([test_symbol])
        if not eod_data.empty:
            print(f"EOD data: {eod_data.iloc[0].to_dict()}")
        
        print(f"\nGetting ticker info for {test_symbol}...")
        ticker_info = marketstack.get_ticker_info(test_symbol)
        if ticker_info:
            print(f"Ticker info: {ticker_info}")
        
        print(f"\nGetting historical data for {test_symbol}...")
        historical = marketstack.get_eod_historical(test_symbol, limit=5)
        if not historical.empty:
            print(f"Historical data shape: {historical.shape}")
            print(historical.head())
        
        test_symbols = ["AAPL", "GOOGL", "MSFT"]
        print(f"\nGetting market summary for {test_symbols}...")
        summary = marketstack.get_market_summary(test_symbols)
        if not summary.empty:
            print(summary[['symbol', 'name', 'close', 'volume']])
            
    except Exception as e:
        print(f"Error in main: {e}")
        logging.error(f"Error in marketstack_md main: {e}")

if __name__ == '__main__':
    main()