#!/usr/bin/env python3
import requests
import pandas as pd
import logging
import os
import dotenv
from datetime import datetime, timedelta
from rich import print

class stockdata_md:
    """
    StockData.org API data extractor for U.S. stock market data
    Provides current quotes, intraday bars, end-of-day history, and company profiles
    Free tier: 100 requests/day (requires API key from https://stockdata.org)
    """
    
    def __init__(self, instance_id, global_args=None):
        self.instance_id = instance_id
        self.args = global_args or {}
        self.base_url = "https://api.stockdata.org/v1"
        
        # Load environment variables from .env file
        load_status = dotenv.load_dotenv()
        if load_status is False:
            logging.warning('Environment variables not loaded from .env file.')
        
        # Get API token from environment
        self.api_token = os.getenv('STOCKDATA_API_TOKEN')
        if not self.api_token:
            logging.warning("STOCKDATA_API_TOKEN not found in environment. Some features may not work.")
        
        self.session = requests.Session()
        self.quotes_df = pd.DataFrame()
        self.intraday_df = pd.DataFrame()
        self.eod_df = pd.DataFrame()
        
        logging.info(f"StockData.org extractor initialized - Instance #{instance_id}")
    
    def get_quote(self, symbols):
        """
        Get real-time quotes for symbols
        symbols: string or list of ticker symbols (max 3 for free tier)
        """
        if not self.api_token:
            logging.error("StockData API token required")
            return pd.DataFrame()
        
        try:
            if isinstance(symbols, str):
                symbols = [symbols]
            
            # Free tier allows max 3 symbols per request
            symbols_str = ','.join([s.upper() for s in symbols[:3]])
            
            url = f"{self.base_url}/data/quote"
            params = {
                'api_token': self.api_token,
                'symbols': symbols_str
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                
                # Standardize column names to match other extractors
                if not df.empty:
                    df = df.rename(columns={
                        'ticker': 'symbol',
                        'price': 'current_price',
                        'day_change': 'change',
                        'day_open': 'open',
                        'day_high': 'high',
                        'day_low': 'low'
                    })
                    
                    # Add calculated percent change if not present
                    if 'change' in df.columns and 'previous_close_price' in df.columns:
                        df['percent_change'] = (df['change'] / df['previous_close_price']) * 100
                
                self.quotes_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching quotes: {e}")
            return pd.DataFrame()
    
    def get_intraday(self, symbol, date=None):
        """
        Get intraday data for a symbol
        date: YYYY-MM-DD format (default: today)
        Note: Free tier allows 1 symbol per request
        """
        if not self.api_token:
            logging.error("StockData API token required")
            return pd.DataFrame()
        
        try:
            if not date:
                date = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/data/intraday/adjusted"
            params = {
                'api_token': self.api_token,
                'symbols': symbol.upper(),
                'date': date
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                
                if not df.empty:
                    df['datetime'] = pd.to_datetime(df['date_time'])
                    df['symbol'] = symbol.upper()
                    
                    # Standardize column names
                    columns_order = ['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume']
                    existing_cols = [col for col in columns_order if col in df.columns]
                    df = df[existing_cols]
                
                self.intraday_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching intraday data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_eod(self, symbol, date_from=None, date_to=None, limit=100):
        """
        Get end-of-day historical data for a symbol
        """
        if not self.api_token:
            logging.error("StockData API token required")
            return pd.DataFrame()
        
        try:
            if not date_from:
                date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
            if not date_to:
                date_to = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/data/eod"
            params = {
                'api_token': self.api_token,
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
                
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    df['symbol'] = symbol.upper()
                    
                    # Standardize column names
                    columns_order = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
                    existing_cols = [col for col in columns_order if col in df.columns]
                    df = df[existing_cols]
                    df = df.sort_values('date')
                
                self.eod_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching EOD data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_splits(self, symbol, date_from=None, date_to=None):
        """
        Get stock split data for a symbol
        """
        if not self.api_token:
            logging.error("StockData API token required")
            return pd.DataFrame()
        
        try:
            if not date_from:
                date_from = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            if not date_to:
                date_to = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/data/splits"
            params = {
                'api_token': self.api_token,
                'symbols': symbol.upper(),
                'date_from': date_from,
                'date_to': date_to
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    df['symbol'] = symbol.upper()
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching splits for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_dividends(self, symbol, date_from=None, date_to=None):
        """
        Get dividend data for a symbol
        """
        if not self.api_token:
            logging.error("StockData API token required")
            return pd.DataFrame()
        
        try:
            if not date_from:
                date_from = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
            if not date_to:
                date_to = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/data/dividends"
            params = {
                'api_token': self.api_token,
                'symbols': symbol.upper(),
                'date_from': date_from,
                'date_to': date_to
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    df['symbol'] = symbol.upper()
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching dividends for {symbol}: {e}")
            return pd.DataFrame()
    
    def search_entity(self, query, limit=10):
        """
        Search for stock entities by name or symbol
        """
        if not self.api_token:
            logging.error("StockData API token required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/entity/search"
            params = {
                'api_token': self.api_token,
                'search': query,
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
            logging.error(f"Error searching entities for '{query}': {e}")
            return pd.DataFrame()
    
    def get_news(self, symbols=None, limit=2, language='en'):
        """
        Get financial news
        symbols: list of symbols to filter by
        limit: max articles per request (2 for free tier)
        """
        if not self.api_token:
            logging.error("StockData API token required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/news/all"
            params = {
                'api_token': self.api_token,
                'limit': min(limit, 2),  # Free tier limited to 2 articles
                'language': language
            }
            
            if symbols:
                if isinstance(symbols, str):
                    symbols = [symbols]
                params['symbols'] = ','.join([s.upper() for s in symbols])
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'data' in data and data['data']:
                df = pd.DataFrame(data['data'])
                if not df.empty:
                    df['published_at'] = pd.to_datetime(df['published_at'])
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching news: {e}")
            return pd.DataFrame()
    
    def get_market_summary(self, symbols_list):
        """
        Get market summary for multiple symbols
        Note: Free tier limited to 3 symbols per quote request
        """
        summary_data = []
        
        # Process symbols in batches of 3 (free tier limitation)
        for i in range(0, len(symbols_list), 3):
            batch = symbols_list[i:i+3]
            try:
                quotes_df = self.get_quote(batch)
                if not quotes_df.empty:
                    summary_data.extend(quotes_df.to_dict('records'))
            except Exception as e:
                logging.error(f"Error in market summary batch {batch}: {e}")
                continue
        
        return pd.DataFrame(summary_data)

def main():
    """Example usage of stockdata_md class"""
    try:
        stockdata = stockdata_md(1)
        
        print("========== StockData.org Market Data Test ==========")
        
        test_symbol = "AAPL"
        print(f"Getting quote for {test_symbol}...")
        quote = stockdata.get_quote([test_symbol])
        if not quote.empty:
            print(f"Quote data: {quote.iloc[0].to_dict()}")
        
        print(f"\nGetting EOD data for {test_symbol}...")
        eod_data = stockdata.get_eod(test_symbol, limit=5)
        if not eod_data.empty:
            print(f"EOD data shape: {eod_data.shape}")
            print(eod_data.head())
        
        print(f"\nSearching for entities matching 'Apple'...")
        search_results = stockdata.search_entity('Apple', limit=3)
        if not search_results.empty:
            print(search_results[['symbol', 'name']].head())
        
        test_symbols = ["AAPL", "GOOGL", "MSFT"]
        print(f"\nGetting market summary for {test_symbols}...")
        summary = stockdata.get_market_summary(test_symbols)
        if not summary.empty:
            print(summary[['symbol', 'name', 'current_price', 'change']])
            
    except Exception as e:
        print(f"Error in main: {e}")
        logging.error(f"Error in stockdata_md main: {e}")

if __name__ == '__main__':
    main()