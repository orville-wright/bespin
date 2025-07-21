#!/usr/bin/env python3
import requests
import pandas as pd
import logging
import os
import dotenv
from datetime import datetime, timedelta
from rich import print

class eodhistoricaldata_md:
    """
    EOD Historical Data API extractor for global market data
    Provides end-of-day data, real-time quotes, fundamentals, and technical indicators
    Free tier: 20 API calls/day (requires API key from https://eodhistoricaldata.com)
    """
    
    def __init__(self, instance_id, global_args=None):
        self.instance_id = instance_id
        self.args = global_args or {}
        self.base_url = "https://eodhd.com/api"
        
        # Load environment variables from .env file
        load_status = dotenv.load_dotenv()
        if load_status is False:
            logging.warning('Environment variables not loaded from .env file.')
        
        # Get API token from environment
        self.api_token = os.getenv('EODHISTORICALDATA_API_TOKEN')
        if not self.api_token:
            logging.warning("EODHISTORICALDATA_API_TOKEN not found in environment. Using demo token for limited testing.")
            self.api_token = "demo"  # Demo token for AAPL.US, TSLA.US, VTI.US, AMZN.US, BTC-USD.CC, EURUSD.FOREX
        
        self.session = requests.Session()
        self.eod_df = pd.DataFrame()
        self.realtime_df = pd.DataFrame()
        self.fundamentals_df = pd.DataFrame()
        
        logging.info(f"EOD Historical Data extractor initialized - Instance #{instance_id}")
    
    def get_eod_data(self, symbol, exchange='US', date_from=None, date_to=None, period='d'):
        """
        Get end-of-day historical data
        symbol: ticker symbol
        exchange: exchange code (US, LSE, XETRA, etc.)
        period: d (daily), w (weekly), m (monthly)
        """
        try:
            if not date_from:
                date_from = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            if not date_to:
                date_to = datetime.now().strftime('%Y-%m-%d')
            
            symbol_formatted = f"{symbol.upper()}.{exchange.upper()}"
            
            url = f"{self.base_url}/eod/{symbol_formatted}"
            params = {
                'api_token': self.api_token,
                'fmt': 'json',
                'from': date_from,
                'to': date_to,
                'period': period
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = symbol.upper()
                df['exchange'] = exchange.upper()
                
                # Standardize column names
                columns_order = ['symbol', 'exchange', 'date', 'open', 'high', 'low', 'close', 'adjusted_close', 'volume']
                existing_cols = [col for col in columns_order if col in df.columns]
                df = df[existing_cols]
                df = df.sort_values('date')
                
                self.eod_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching EOD data for {symbol}.{exchange}: {e}")
            return pd.DataFrame()
    
    def get_realtime_data(self, symbols, exchange='US'):
        """
        Get real-time/live data for symbols
        symbols: string or list of ticker symbols
        Note: 15-20 min delay for stocks, 1 min for forex
        """
        try:
            if isinstance(symbols, str):
                symbols = [symbols]
            
            symbols_formatted = [f"{s.upper()}.{exchange.upper()}" for s in symbols]
            symbols_str = ','.join(symbols_formatted)
            
            url = f"{self.base_url}/real-time/{symbols_str}"
            params = {
                'api_token': self.api_token,
                'fmt': 'json'
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            quotes_data = []
            
            # Handle single or multiple symbols
            if isinstance(data, list):
                quotes_data = data
            else:
                quotes_data = [data]
            
            if quotes_data:
                df = pd.DataFrame(quotes_data)
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
                
                # Add separate symbol and exchange columns
                if 'code' in df.columns:
                    df['symbol'] = df['code'].str.split('.').str[0]
                    df['exchange'] = df['code'].str.split('.').str[1]
                
                self.realtime_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching real-time data: {e}")
            return pd.DataFrame()
    
    def get_fundamentals(self, symbol, exchange='US'):
        """
        Get fundamental data for a symbol
        Note: This endpoint consumes 10 API calls per request
        """
        try:
            symbol_formatted = f"{symbol.upper()}.{exchange.upper()}"
            
            url = f"{self.base_url}/fundamentals/{symbol_formatted}"
            params = {
                'api_token': self.api_token,
                'fmt': 'json'
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                # Extract key fundamental metrics
                general = data.get('General', {})
                highlights = data.get('Highlights', {})
                valuation = data.get('Valuation', {})
                
                fundamentals = {
                    'symbol': symbol.upper(),
                    'exchange': exchange.upper(),
                    'name': general.get('Name'),
                    'description': general.get('Description'),
                    'country': general.get('Country'),
                    'sector': general.get('Sector'),
                    'industry': general.get('Industry'),
                    'market_cap': highlights.get('MarketCapitalization'),
                    'pe_ratio': highlights.get('PERatio'),
                    'peg_ratio': highlights.get('PEGRatio'),
                    'dividend_yield': highlights.get('DividendYield'),
                    'eps': highlights.get('EarningsShare'),
                    'beta': highlights.get('Beta'),
                    'book_value': highlights.get('BookValue'),
                    '52_week_high': highlights.get('52WeekHigh'),
                    '52_week_low': highlights.get('52WeekLow'),
                    'trailing_pe': valuation.get('TrailingPE'),
                    'forward_pe': valuation.get('ForwardPE'),
                    'price_to_book': valuation.get('PriceBookMRQ'),
                    'price_to_sales': valuation.get('PriceSalesTTM')
                }
                
                return fundamentals
            
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching fundamentals for {symbol}.{exchange}: {e}")
            return {}
    
    def get_intraday_data(self, symbol, exchange='US', interval='5m', date=None):
        """
        Get intraday data for a symbol
        interval: 1m, 5m, 1h
        Note: This endpoint consumes 5 API calls per request
        """
        try:
            if not date:
                date = datetime.now().strftime('%Y-%m-%d')
            
            symbol_formatted = f"{symbol.upper()}.{exchange.upper()}"
            
            url = f"{self.base_url}/intraday/{symbol_formatted}"
            params = {
                'api_token': self.api_token,
                'fmt': 'json',
                'interval': interval,
                'from': date,
                'to': date
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['datetime'] = pd.to_datetime(df['datetime'])
                df['symbol'] = symbol.upper()
                df['exchange'] = exchange.upper()
                
                # Standardize column names
                columns_order = ['symbol', 'exchange', 'datetime', 'open', 'high', 'low', 'close', 'volume']
                existing_cols = [col for col in columns_order if col in df.columns]
                df = df[existing_cols]
                df = df.sort_values('datetime')
                
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching intraday data for {symbol}.{exchange}: {e}")
            return pd.DataFrame()
    
    def get_exchanges(self):
        """
        Get list of supported exchanges
        """
        try:
            url = f"{self.base_url}/exchanges-list/"
            params = {
                'api_token': self.api_token,
                'fmt': 'json'
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching exchanges list: {e}")
            return pd.DataFrame()
    
    def get_dividends(self, symbol, exchange='US', date_from=None, date_to=None):
        """
        Get dividend data for a symbol
        """
        try:
            if not date_from:
                date_from = (datetime.now() - timedelta(days=365*2)).strftime('%Y-%m-%d')
            if not date_to:
                date_to = datetime.now().strftime('%Y-%m-%d')
            
            symbol_formatted = f"{symbol.upper()}.{exchange.upper()}"
            
            url = f"{self.base_url}/div/{symbol_formatted}"
            params = {
                'api_token': self.api_token,
                'fmt': 'json',
                'from': date_from,
                'to': date_to
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = symbol.upper()
                df['exchange'] = exchange.upper()
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching dividends for {symbol}.{exchange}: {e}")
            return pd.DataFrame()
    
    def get_technical_indicators(self, symbol, exchange='US', function='sma', period=50):
        """
        Get technical indicators for a symbol
        function: sma, ema, rsi, macd, bbands, etc.
        """
        try:
            symbol_formatted = f"{symbol.upper()}.{exchange.upper()}"
            
            url = f"{self.base_url}/technical/{symbol_formatted}"
            params = {
                'api_token': self.api_token,
                'fmt': 'json',
                'function': function,
                'period': period
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = symbol.upper()
                df['exchange'] = exchange.upper()
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching technical indicators for {symbol}.{exchange}: {e}")
            return pd.DataFrame()
    
    def get_market_summary(self, symbols_list, exchange='US'):
        """
        Get market summary for multiple symbols
        """
        try:
            return self.get_realtime_data(symbols_list, exchange)
        except Exception as e:
            logging.error(f"Error getting market summary: {e}")
            return pd.DataFrame()

def main():
    """Example usage of eodhistoricaldata_md class"""
    try:
        eod = eodhistoricaldata_md(1)
        
        print("========== EOD Historical Data API Test ==========")
        
        # Demo symbols that work with demo token
        test_symbol = "AAPL"
        print(f"Getting EOD data for {test_symbol}...")
        eod_data = eod.get_eod_data(test_symbol, 'US', limit=5)
        if not eod_data.empty:
            print(f"EOD data shape: {eod_data.shape}")
            print(eod_data.head())
        
        print(f"\nGetting real-time data for {test_symbol}...")
        realtime = eod.get_realtime_data([test_symbol], 'US')
        if not realtime.empty:
            print(f"Real-time data: {realtime.iloc[0].to_dict()}")
        
        print(f"\nGetting fundamentals for {test_symbol}...")
        fundamentals = eod.get_fundamentals(test_symbol, 'US')
        if fundamentals:
            print(f"Company: {fundamentals.get('name')} ({fundamentals.get('sector')})")
            print(f"Market Cap: {fundamentals.get('market_cap')}")
        
        test_symbols = ["AAPL", "TSLA"]  # Demo-available symbols
        print(f"\nGetting market summary for {test_symbols}...")
        summary = eod.get_market_summary(test_symbols, 'US')
        if not summary.empty:
            print(summary[['code', 'close', 'change_p']])
            
    except Exception as e:
        print(f"Error in main: {e}")
        logging.error(f"Error in eodhistoricaldata_md main: {e}")

if __name__ == '__main__':
    main()