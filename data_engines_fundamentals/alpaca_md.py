#! python3

import os
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import time
from dotenv import load_dotenv
import json
import logging

class alpaca_md:
    """
    Alpaca Market Data retriever class
    Provides live quotes and OHLCV candlestick data via Alpaca API
    """
    
    def __init__(self, inst_id, args=None):
        self.inst_id = inst_id
        self.args = args if args else {}
        self.api_key = None
        self.secret_key = None
        self.base_url = "https://data.alpaca.markets/v2/"
        self.quote_data = {}
        self.bars_data = {}
        self.symbols_list = []
        
        # Initialize credentials
        self._load_credentials()
        
    def _load_credentials(self):
        """Load Alpaca API credentials from environment variables"""
        load_status = load_dotenv()
        if not load_status:
            raise RuntimeError('Environment variables not loaded.')
            
        self.api_key = os.getenv("ALPACA_API-KEY")
        self.secret_key = os.getenv("ALPACA_SEC-KEY")
        
        if not self.api_key or not self.secret_key:
            raise ValueError("Alpaca API credentials not found in environment variables.")
            
    def get_headers(self):
        """Return headers for Alpaca API requests"""
        return {
            "accept": "application/json",
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key
        }
    
    def get_live_quote(self, symbol):
        """Get live quote for a single symbol"""
        url = f"{self.base_url}stocks/quotes/latest"
        params = {"symbols": symbol.upper()}
        
        try:
            response = requests.get(url, headers=self.get_headers(), params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'quotes' in data and symbol.upper() in data['quotes']:
                quote = data['quotes'][symbol.upper()]
                self.quote_data[symbol.upper()] = self._format_quote_data(quote, symbol.upper())
                return self.quote_data[symbol.upper()]
            else:
                logging.warning(f"No quote data found for symbol: {symbol}")
                return None
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching quote for {symbol}: {e}")
            return None
            
    def get_bars(self, symbol, timeframe="1Min", start_date=None, end_date=None, limit=100):
        """Get OHLCV bars for a symbol"""
        url = f"{self.base_url}stocks/bars"
        
        # Set default dates if not provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%dT09:30:00-04:00")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%dT16:00:00-04:00")
            
        params = {
            "symbols": symbol.upper(),
            "timeframe": timeframe,
            "start": start_date,
            "end": end_date,
            "limit": limit,
            "adjustment": "raw",
            "feed": "sip",
            "sort": "asc"
        }
        
        try:
            response = requests.get(url, headers=self.get_headers(), params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'bars' in data and symbol.upper() in data['bars']:
                bars = data['bars'][symbol.upper()]
                self.bars_data[symbol.upper()] = self._format_bars_data(bars, symbol.upper())
                return self.bars_data[symbol.upper()]
            else:
                logging.warning(f"No bars data found for symbol: {symbol}")
                return None
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching bars for {symbol}: {e}")
            return None
            
    def get_multiple_quotes(self, symbols_list):
        """Get live quotes for multiple symbols"""
        if isinstance(symbols_list, str):
            symbols_list = [symbols_list]
            
        symbols_str = ",".join([s.upper() for s in symbols_list])
        url = f"{self.base_url}stocks/quotes/latest"
        params = {"symbols": symbols_str}
        
        try:
            response = requests.get(url, headers=self.get_headers(), params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'quotes' in data:
                for symbol, quote in data['quotes'].items():
                    self.quote_data[symbol] = self._format_quote_data(quote, symbol)
                return self.quote_data
            else:
                logging.warning("No quotes data found")
                return {}
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching multiple quotes: {e}")
            return {}
            
    def _format_quote_data(self, quote, symbol):
        """Format quote data into standardized dictionary"""
        return {
            'Symbol': symbol,
            'Bid_price': quote.get('bp', 0.0),
            'Ask_price': quote.get('ap', 0.0),
            'Bid_size': quote.get('bs', 0),
            'Ask_size': quote.get('as', 0),
            'Last_price': quote.get('ap', 0.0),  # Use ask price as last price approximation
            'Timestamp': quote.get('t', ''),
            'Exchange': quote.get('bx', 'Unknown'),
            'Source': 'Alpaca'
        }
        
    def _format_bars_data(self, bars, symbol):
        """Format bars data into pandas DataFrame"""
        if not bars:
            return pd.DataFrame()
            
        bars_list = []
        for bar in bars:
            bars_list.append({
                'Symbol': symbol,
                'Timestamp': bar.get('t', ''),
                'Open': bar.get('o', 0.0),
                'High': bar.get('h', 0.0),
                'Low': bar.get('l', 0.0),
                'Close': bar.get('c', 0.0),
                'Volume': bar.get('v', 0),
                'VWAP': bar.get('vw', 0.0),
                'Trade_count': bar.get('n', 0)
            })
            
        return pd.DataFrame(bars_list)
        
    def build_quote_df(self):
        """Build DataFrame from collected quote data"""
        if not self.quote_data:
            return pd.DataFrame()
            
        quotes_list = []
        for symbol, quote in self.quote_data.items():
            quotes_list.append(quote)
            
        return pd.DataFrame(quotes_list)
        
    def print_quotes(self):
        """Print formatted quote data"""
        if not self.quote_data:
            print("No quote data available")
            return
            
        print("========== Alpaca Live Quotes ==========")
        for symbol, quote in self.quote_data.items():
            print(f"{symbol}: Bid: ${quote['Bid_price']:.2f} Ask: ${quote['Ask_price']:.2f} Last: ${quote['Last_price']:.2f}")
            
    def get_market_status(self):
        """Check if market is open"""
        url = f"{self.base_url.replace('data.', '')}clock"
        try:
            response = requests.get(url, headers=self.get_headers())
            response.raise_for_status()
            data = response.json()
            return data.get('is_open', False)
        except:
            return False

def show_data(data):
    """Legacy function for backward compatibility"""
    my_keys = data.keys()
    list_of_my_keys = list(my_keys)
    print ( f"Look at the {list_of_my_keys[0]} data in a Pretty & Nicer format..." )
    print ( f"-----------------------------------------------------------------------" )
    data_list = data[list_of_my_keys[0]]
    for i in range(len(data_list)):
        print ( f"DATA ITEM #{i} is -> {data_list[i]}" )

    return data_list

############################## MAIN #############################################
# Example usage and testing of the alpaca_md class
def main():
    """Example usage of alpaca_md class"""
    try:
        # Initialize Alpaca market data instance
        alpaca = alpaca_md(1)
        
        print("========== Alpaca Market Data Test ==========")
        print(f"Market Status: {'Open' if alpaca.get_market_status() else 'Closed'}")
        print()
        
        # Test single quote
        test_symbol = "AAPL"
        print(f"Getting quote for {test_symbol}...")
        quote = alpaca.get_live_quote(test_symbol)
        if quote:
            print(f"Quote data: {quote}")
        
        # Test multiple quotes
        test_symbols = ["AAPL", "GOOGL", "MSFT"]
        print(f"\nGetting quotes for {test_symbols}...")
        quotes = alpaca.get_multiple_quotes(test_symbols)
        alpaca.print_quotes()
        
        # Test bars data
        print(f"\nGetting bars data for {test_symbol}...")
        bars_df = alpaca.get_bars(test_symbol, timeframe="1Min", limit=10)
        if bars_df is not None and not bars_df.empty:
            print(f"Bars data shape: {bars_df.shape}")
            print(bars_df.head())
        
        # Build and display quotes DataFrame
        quotes_df = alpaca.build_quote_df()
        if not quotes_df.empty:
            print(f"\nQuotes DataFrame:")
            print(quotes_df)
            
    except Exception as e:
        print(f"Error in main: {e}")
        logging.error(f"Error in alpaca_md main: {e}")

if __name__ == '__main__':
    main()
