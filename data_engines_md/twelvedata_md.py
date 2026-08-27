#!/usr/bin/env python3
import requests
import pandas as pd
import logging
import os
import dotenv
from datetime import datetime, timedelta
from rich import print

class twelvedata_md:
    """
    Twelve Data API extractor for stocks, forex, and cryptocurrency data
    Provides real-time quotes, historical time series, and fundamental data
    Free tier: 8 requests/minute, 800 requests/day (requires API key from https://twelvedata.com)
    """
    
    def __init__(self, instance_id, global_args=None):
        self.instance_id = instance_id
        self.args = global_args or {}
        self.base_url = "https://api.twelvedata.com"
        
        # Load environment variables from .env file
        load_status = dotenv.load_dotenv()
        if load_status is False:
            logging.warning('Environment variables not loaded from .env file.')
        
        # Get API key from environment
        self.api_key = os.getenv('TWELVEDATA_API_KEY')
        if not self.api_key:
            logging.warning("TWELVEDATA_API_KEY not found in environment. Some features may not work.")
        
        self.session = requests.Session()
        self.quotes_df = pd.DataFrame()
        self.time_series_df = pd.DataFrame()
        
        logging.info(f"Twelve Data extractor initialized - Instance #{instance_id}")
    
    def get_quote(self, symbol, exchange=None):
        """
        Get real-time quote for a symbol
        """
        if not self.api_key:
            logging.error("Twelve Data API key required")
            return {}
        
        try:
            url = f"{self.base_url}/quote"
            params = {
                'symbol': symbol.upper(),
                'apikey': self.api_key
            }
            
            if exchange:
                params['exchange'] = exchange
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data and 'symbol' in data:
                return {
                    'symbol': data.get('symbol'),
                    'name': data.get('name'),
                    'exchange': data.get('exchange'),
                    'currency': data.get('currency'),
                    'datetime': data.get('datetime'),
                    'timestamp': data.get('timestamp'),
                    'open': float(data.get('open', 0)) if data.get('open') else None,
                    'high': float(data.get('high', 0)) if data.get('high') else None,
                    'low': float(data.get('low', 0)) if data.get('low') else None,
                    'close': float(data.get('close', 0)) if data.get('close') else None,
                    'volume': int(data.get('volume', 0)) if data.get('volume') else None,
                    'previous_close': float(data.get('previous_close', 0)) if data.get('previous_close') else None,
                    'change': float(data.get('change', 0)) if data.get('change') else None,
                    'percent_change': float(data.get('percent_change', 0)) if data.get('percent_change') else None,
                    'average_volume': int(data.get('average_volume', 0)) if data.get('average_volume') else None,
                    'is_market_open': data.get('is_market_open'),
                    'fifty_two_week': data.get('fifty_two_week', {}),
                    'source': 'TwelveData'
                }
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching quote for {symbol}: {e}")
            return {}
    
    def get_time_series(self, symbol, interval='1day', outputsize=30, start_date=None, end_date=None, exchange=None):
        """
        Get historical time series data for a symbol
        interval: 1min, 5min, 15min, 30min, 45min, 1h, 2h, 4h, 1day, 1week, 1month
        """
        if not self.api_key:
            logging.error("Twelve Data API key required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/time_series"
            params = {
                'symbol': symbol.upper(),
                'interval': interval,
                'outputsize': outputsize,
                'apikey': self.api_key
            }
            
            if start_date:
                params['start_date'] = start_date
            if end_date:
                params['end_date'] = end_date
            if exchange:
                params['exchange'] = exchange
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'values' in data and data['values']:
                df = pd.DataFrame(data['values'])
                
                # Convert string columns to appropriate types
                df['datetime'] = pd.to_datetime(df['datetime'])
                numeric_cols = ['open', 'high', 'low', 'close', 'volume']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Add metadata
                meta = data.get('meta', {})
                df['symbol'] = meta.get('symbol', symbol.upper())
                df['exchange'] = meta.get('exchange', '')
                df['currency'] = meta.get('currency', '')
                df['interval'] = meta.get('interval', interval)
                
                # Reorder columns
                columns_order = ['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'exchange', 'currency', 'interval']
                existing_cols = [col for col in columns_order if col in df.columns]
                df = df[existing_cols]
                df = df.sort_values('datetime')
                
                self.time_series_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching time series for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_statistics(self, symbol, exchange=None):
        """
        Get key statistics for a symbol
        Note: This endpoint consumes 100 credits per symbol
        """
        if not self.api_key:
            logging.error("Twelve Data API key required")
            return {}
        
        try:
            url = f"{self.base_url}/statistics"
            params = {
                'symbol': symbol.upper(),
                'apikey': self.api_key
            }
            
            if exchange:
                params['exchange'] = exchange
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data and 'symbol' in data:
                statistics = data.get('statistics', {})
                return {
                    'symbol': data.get('symbol'),
                    'market_cap': statistics.get('market_capitalization'),
                    'pe_ratio': statistics.get('pe_ratio'),
                    'peg_ratio': statistics.get('peg_ratio'),
                    'book_value': statistics.get('book_value'),
                    'dividend_yield': statistics.get('dividend_yield'),
                    'eps': statistics.get('eps'),
                    'beta': statistics.get('beta'),
                    'shares_outstanding': statistics.get('shares_outstanding'),
                    '52_week_high': statistics.get('52_week_high'),
                    '52_week_low': statistics.get('52_week_low'),
                    '50_day_ma': statistics.get('50_day_ma'),
                    '200_day_ma': statistics.get('200_day_ma')
                }
            
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching statistics for {symbol}: {e}")
            return {}
    
    def get_dividends(self, symbol, start_date=None, end_date=None, exchange=None):
        """
        Get dividend history for a symbol
        """
        if not self.api_key:
            logging.error("Twelve Data API key required")
            return pd.DataFrame()
        
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365*2)).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/dividends"
            params = {
                'symbol': symbol.upper(),
                'start_date': start_date,
                'end_date': end_date,
                'apikey': self.api_key
            }
            
            if exchange:
                params['exchange'] = exchange
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'dividends' in data and data['dividends']:
                df = pd.DataFrame(data['dividends'])
                df['ex_date'] = pd.to_datetime(df['ex_date'])
                df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
                df['symbol'] = symbol.upper()
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching dividends for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_income_statement(self, symbol, period='annual', exchange=None):
        """
        Get income statement data for a symbol
        period: annual or quarterly
        Note: This endpoint consumes 100 credits per symbol
        """
        if not self.api_key:
            logging.error("Twelve Data API key required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/income_statement"
            params = {
                'symbol': symbol.upper(),
                'period': period,
                'apikey': self.api_key
            }
            
            if exchange:
                params['exchange'] = exchange
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'income_statement' in data and data['income_statement']:
                df = pd.DataFrame(data['income_statement'])
                df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])
                df['symbol'] = symbol.upper()
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching income statement for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_balance_sheet(self, symbol, period='annual', exchange=None):
        """
        Get balance sheet data for a symbol
        period: annual or quarterly
        Note: This endpoint consumes 100 credits per symbol
        """
        if not self.api_key:
            logging.error("Twelve Data API key required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/balance_sheet"
            params = {
                'symbol': symbol.upper(),
                'period': period,
                'apikey': self.api_key
            }
            
            if exchange:
                params['exchange'] = exchange
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'balance_sheet' in data and data['balance_sheet']:
                df = pd.DataFrame(data['balance_sheet'])
                df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])
                df['symbol'] = symbol.upper()
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching balance sheet for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_multiple_quotes(self, symbols):
        """
        Get quotes for multiple symbols in a single request
        symbols: list of symbols (can include exchange specification)
        """
        if not self.api_key:
            logging.error("Twelve Data API key required")
            return pd.DataFrame()
        
        try:
            if isinstance(symbols, str):
                symbols = [symbols]
            
            symbols_str = ','.join([s.upper() for s in symbols])
            
            url = f"{self.base_url}/quote"
            params = {
                'symbol': symbols_str,
                'apikey': self.api_key
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            quotes_data = []
            
            # Handle both single and multiple symbol responses
            if isinstance(data, dict):
                if 'symbol' in data:
                    # Single symbol response
                    quotes_data.append(data)
                else:
                    # Multiple symbols response
                    for symbol, quote_data in data.items():
                        quote_data['symbol'] = symbol
                        quotes_data.append(quote_data)
            
            if quotes_data:
                df = pd.DataFrame(quotes_data)
                # Convert numeric columns
                numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'previous_close', 'change', 'percent_change']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                self.quotes_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching multiple quotes: {e}")
            return pd.DataFrame()
    
    def get_market_summary(self, symbols_list):
        """
        Get market summary for multiple symbols
        """
        try:
            return self.get_multiple_quotes(symbols_list)
        except Exception as e:
            logging.error(f"Error getting market summary: {e}")
            return pd.DataFrame()

def main():
    """Example usage of twelvedata_md class"""
    try:
        twelvedata = twelvedata_md(1)
        
        print("========== Twelve Data Market Data Test ==========")
        
        test_symbol = "AAPL"
        print(f"Getting quote for {test_symbol}...")
        quote = twelvedata.get_quote(test_symbol)
        if quote:
            print(f"Quote data: {quote}")
        
        print(f"\nGetting time series for {test_symbol}...")
        time_series = twelvedata.get_time_series(test_symbol, interval='1day', outputsize=5)
        if not time_series.empty:
            print(f"Time series shape: {time_series.shape}")
            print(time_series.head())
        
        test_symbols = ["AAPL", "GOOGL", "MSFT"]
        print(f"\nGetting market summary for {test_symbols}...")
        summary = twelvedata.get_market_summary(test_symbols)
        if not summary.empty:
            print(summary[['symbol', 'name', 'close', 'change', 'percent_change']])
            
    except Exception as e:
        print(f"Error in main: {e}")
        logging.error(f"Error in twelvedata_md main: {e}")

if __name__ == '__main__':
    main()