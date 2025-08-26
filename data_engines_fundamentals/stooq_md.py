#!/usr/bin/env python3
import requests
import pandas as pd
import logging
import os
from datetime import datetime, timedelta
from rich import print
import time

class stooq_md:
    """
    Stooq.com data extractor for global historical market data
    Provides end-of-day historical data via URL-based CSV downloads
    No API key required - free historical data service
    Coverage: 21,332 global securities, indices, ETFs, currencies, cryptocurrencies
    """
    
    def __init__(self, instance_id, global_args=None):
        self.instance_id = instance_id
        self.args = global_args or {}
        self.base_url = "https://stooq.com/q"
        
        self.session = requests.Session()
        # Set headers to mimic browser behavior
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        self.historical_df = pd.DataFrame()
        self.quotes_df = pd.DataFrame()
        
        logging.info(f"Stooq data extractor initialized - Instance #{instance_id}")
    
    def _format_symbol(self, symbol, market='US'):
        """
        Format symbol according to Stooq naming convention
        US stocks: AAPL.US
        UK stocks: AV.UK  
        Indices: ^DJI, ^UK100
        Crypto: BTC.V
        """
        symbol = symbol.upper()
        
        # Handle indices (already have ^ prefix)
        if symbol.startswith('^'):
            return symbol
        
        # Handle crypto symbols
        if symbol in ['BTC', 'ETH', 'LTC', 'XRP']:
            return f"{symbol}.V"
        
        # Add market suffix for regular stocks
        market_suffix_map = {
            'US': 'US',
            'UK': 'UK', 
            'DE': 'DE',  # Germany
            'JP': 'JP',  # Japan
            'HK': 'HK',  # Hong Kong
            'CA': 'TO'   # Canada (Toronto)
        }
        
        suffix = market_suffix_map.get(market.upper(), 'US')
        return f"{symbol}.{suffix}"
    
    def get_current_quote(self, symbol, market='US', include_headers=True):
        """
        Get current quote data for a symbol
        """
        try:
            formatted_symbol = self._format_symbol(symbol, market)
            
            url = f"{self.base_url}/l/"
            params = {
                's': formatted_symbol,
                'f': 'sd2t2ohlcvn',  # symbol, date, time, open, high, low, close, volume, name
                'e': 'csv'
            }
            
            if include_headers:
                params['h'] = ''
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            # Add delay to be respectful to the service
            time.sleep(0.5)
            
            # Parse CSV response
            if response.text.strip():
                lines = response.text.strip().split('\n')
                
                if include_headers and len(lines) >= 2:
                    headers = lines[0].split(',')
                    data_line = lines[1].split(',')
                elif not include_headers and len(lines) >= 1:
                    headers = ['Symbol', 'Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Name']
                    data_line = lines[0].split(',')
                else:
                    return pd.DataFrame()
                
                # Create DataFrame
                data_dict = {headers[i]: [data_line[i]] for i in range(min(len(headers), len(data_line)))}
                df = pd.DataFrame(data_dict)
                
                # Clean and convert data types
                numeric_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Add timestamp
                if 'Date' in df.columns and 'Time' in df.columns:
                    df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'], errors='coerce')
                elif 'Date' in df.columns:
                    df['DateTime'] = pd.to_datetime(df['Date'], errors='coerce')
                
                self.quotes_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching current quote for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_historical_data(self, symbol, market='US', interval='d', days_back=365):
        """
        Get historical data for a symbol
        interval: d (daily), w (weekly), m (monthly)
        """
        try:
            formatted_symbol = self._format_symbol(symbol, market)
            
            url = f"{self.base_url}/d/l/"
            params = {
                's': formatted_symbol,
                'i': interval
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            # Add delay to be respectful to the service
            time.sleep(0.5)
            
            # Parse CSV response
            if response.text.strip():
                # Read CSV data
                from io import StringIO
                df = pd.read_csv(StringIO(response.text))
                
                if not df.empty:
                    # Standardize column names
                    df.columns = df.columns.str.strip()
                    column_mapping = {
                        'Date': 'date',
                        'Open': 'open', 
                        'High': 'high',
                        'Low': 'low',
                        'Close': 'close',
                        'Volume': 'volume'
                    }
                    df = df.rename(columns=column_mapping)
                    
                    # Convert date column
                    if 'date' in df.columns:
                        # Handle different date formats from Stooq
                        if df['date'].dtype == 'int64':
                            # Convert YYYYMMDD integer format to datetime
                            df['date'] = pd.to_datetime(df['date'].astype(str), format='%Y%m%d', errors='coerce')
                        else:
                            df['date'] = pd.to_datetime(df['date'], errors='coerce')
                    
                    # Convert numeric columns
                    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
                    for col in numeric_cols:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Add symbol and market info
                    df['symbol'] = symbol.upper()
                    df['market'] = market.upper()
                    df['source'] = 'Stooq'
                    
                    # Filter by date range if specified
                    if days_back and 'date' in df.columns:
                        cutoff_date = datetime.now() - timedelta(days=days_back)
                        df = df[df['date'] >= cutoff_date]
                    
                    # Sort by date
                    if 'date' in df.columns:
                        df = df.sort_values('date')
                    
                    # Reorder columns
                    columns_order = ['symbol', 'market', 'date', 'open', 'high', 'low', 'close', 'volume', 'source']
                    existing_cols = [col for col in columns_order if col in df.columns]
                    df = df[existing_cols]
                    
                    self.historical_df = df
                    return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching historical data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_index_data(self, index_symbol, interval='d', days_back=365):
        """
        Get historical data for market indices
        Common indices: ^DJI, ^GSPC, ^IXIC, ^UK100, ^N225
        """
        try:
            # Ensure index symbol has ^ prefix
            if not index_symbol.startswith('^'):
                index_symbol = f"^{index_symbol}"
            
            return self.get_historical_data(index_symbol, market='', interval=interval, days_back=days_back)
            
        except Exception as e:
            logging.error(f"Error fetching index data for {index_symbol}: {e}")
            return pd.DataFrame()
    
    def get_crypto_data(self, crypto_symbol, interval='d', days_back=365):
        """
        Get historical data for cryptocurrencies
        Common cryptos: BTC, ETH, LTC, XRP
        """
        try:
            return self.get_historical_data(crypto_symbol, market='V', interval=interval, days_back=days_back)
            
        except Exception as e:
            logging.error(f"Error fetching crypto data for {crypto_symbol}: {e}")
            return pd.DataFrame()
    
    def get_forex_data(self, base_currency, quote_currency, interval='d', days_back=365):
        """
        Get historical forex data
        Example: get_forex_data('EUR', 'USD') for EUR/USD
        """
        try:
            forex_symbol = f"{base_currency.upper()}{quote_currency.upper()}"
            return self.get_historical_data(forex_symbol, market='FX', interval=interval, days_back=days_back)
            
        except Exception as e:
            logging.error(f"Error fetching forex data for {base_currency}/{quote_currency}: {e}")
            return pd.DataFrame()
    
    def get_multiple_quotes(self, symbols_list, market='US'):
        """
        Get current quotes for multiple symbols
        Note: Makes individual requests to be respectful to the service
        """
        quotes_data = []
        
        for symbol in symbols_list:
            try:
                quote_df = self.get_current_quote(symbol, market, include_headers=False)
                if not quote_df.empty:
                    quotes_data.append(quote_df.iloc[0].to_dict())
                # Add delay between requests
                time.sleep(1)
            except Exception as e:
                logging.error(f"Error fetching quote for {symbol}: {e}")
                continue
        
        if quotes_data:
            return pd.DataFrame(quotes_data)
        
        return pd.DataFrame()
    
    def get_market_summary(self, symbols_list, market='US'):
        """
        Get market summary for multiple symbols
        """
        try:
            return self.get_multiple_quotes(symbols_list, market)
        except Exception as e:
            logging.error(f"Error getting market summary: {e}")
            return pd.DataFrame()
    
    def search_symbol(self, query):
        """
        Basic symbol search functionality
        Note: Stooq doesn't have a search API, this provides common symbol patterns
        """
        common_symbols = {
            # US Stocks
            'apple': 'AAPL.US',
            'microsoft': 'MSFT.US', 
            'google': 'GOOGL.US',
            'amazon': 'AMZN.US',
            'tesla': 'TSLA.US',
            
            # Indices
            'dow': '^DJI',
            'sp500': '^GSPC',
            'nasdaq': '^IXIC',
            'ftse': '^UK100',
            'nikkei': '^N225',
            
            # Crypto
            'bitcoin': 'BTC.V',
            'ethereum': 'ETH.V',
            'litecoin': 'LTC.V',
            'ripple': 'XRP.V',
            
            # Forex
            'eurusd': 'EURUSD.FX',
            'gbpusd': 'GBPUSD.FX',
            'usdjpy': 'USDJPY.FX'
        }
        
        query_lower = query.lower()
        matches = {}
        
        for name, symbol in common_symbols.items():
            if query_lower in name or name in query_lower:
                matches[name] = symbol
        
        return matches

def main():
    """Example usage of stooq_md class"""
    try:
        stooq = stooq_md(1)
        
        print("========== Stooq Market Data Test ==========")
        
        test_symbol = "AAPL"
        print(f"Getting historical data for {test_symbol}...")
        historical = stooq.get_historical_data(test_symbol, 'US', 'd', days_back=30)
        if not historical.empty:
            print(f"Historical data shape: {historical.shape}")
            print(historical.head())
        
        print(f"\nGetting current quote for {test_symbol}...")
        quote = stooq.get_current_quote(test_symbol, 'US')
        if not quote.empty:
            print(f"Quote data: {quote.iloc[0].to_dict()}")
        
        print(f"\nGetting index data for S&P 500...")
        index_data = stooq.get_index_data('GSPC', 'd', days_back=10)
        if not index_data.empty:
            print(f"Index data shape: {index_data.shape}")
            print(index_data.tail(3))
        
        print(f"\nGetting crypto data for Bitcoin...")
        crypto_data = stooq.get_crypto_data('BTC', 'd', days_back=10)
        if not crypto_data.empty:
            print(f"Crypto data shape: {crypto_data.shape}")
            print(crypto_data.tail(3))
        
        print(f"\nSearching for symbols...")
        search_results = stooq.search_symbol('apple')
        print(f"Search results: {search_results}")
            
    except Exception as e:
        print(f"Error in main: {e}")
        logging.error(f"Error in stooq_md main: {e}")

if __name__ == '__main__':
    main()