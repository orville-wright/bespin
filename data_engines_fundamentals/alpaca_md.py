#! python3

import os
import requests
import pandas as pd
from datetime import datetime, timedelta, date
import time
from dotenv import load_dotenv
import logging

# #################### class
class alpaca_md:
    """
    Alpaca Market Data retriever class
    Provides live quotes and OHLCV candlestick data via Alpaca API
    """

    DATA_BASE_URL = "https://data.alpaca.markets/v2/"
    PAPER_TRADING_BASE_URL = "https://paper-api.alpaca.markets/v2/"
    LIVE_TRADING_BASE_URL = "https://api.alpaca.markets/v2/"
    VALID_FEEDS = {"iex", "sip", "delayed_sip", "boats", "overnight", "otc"}
    
    def __init__(self, inst_id, args=None):
        load_dotenv()
        self.inst_id = inst_id
        self.args = vars(args) if args is not None and not isinstance(args, dict) else (args or {})
        self.api_key = None
        self.secret_key = None
        self.api_key_env_var = None
        self.secret_key_env_var = None
        self.base_url = self._normalize_base_url(
            os.getenv("APCA_API_DATA_URL"),
            self.DATA_BASE_URL,
        )
        self.trading_base_url = self._get_trading_base_url()
        self.feed = self._get_feed()
        self.timeout = float(os.getenv("ALPACA_TIMEOUT", "30"))
        self.quote_data = {}
        self.bars_data = {}
        self.symbols_list = []
        
        # Initialize credentials
        self._load_credentials()

# #################### 1
# _INIT_ helper method
#
    def _load_credentials(self):
        """Load Alpaca API credentials from environment variables.

        Alpaca's documented environment names are APCA_API_KEY_ID and
        APCA_API_SECRET_KEY. The legacy ALPACA_* names are kept for backwards
        compatibility with this project.
        """
        self.api_key, self.api_key_env_var = self._first_env_value(
            "APCA_API_KEY_ID",
            "ALPACA_API_KEY",
            "ALPACA_API-KEY",
        )
        self.secret_key, self.secret_key_env_var = self._first_env_value(
            "APCA_API_SECRET_KEY",
            "ALPACA_SECRET_KEY",
            "ALPACA_API_SECRET_KEY",
            "ALPACA_SEC-KEY",
        )
        
        if not self.api_key or not self.secret_key:
            raise ValueError(
                "Alpaca API credentials not found. Set APCA_API_KEY_ID and "
                "APCA_API_SECRET_KEY in .env. Legacy ALPACA_API-KEY and "
                "ALPACA_SEC-KEY are also supported."
            )

# #################### 2
# _INIT_ helper method
#
    def _first_env_value(self, *names):
        """Return the first non-empty environment variable value and its name."""
        for name in names:
            value = os.getenv(name)
            if value and value.strip():
                return value.strip(), name
        return None, None

# #################### 3
# _INIT_ helper method
#
    def _normalize_base_url(self, env_value, default):
        """Normalize Alpaca base URLs so endpoint joins are predictable."""
        base_url = (env_value or default).strip().rstrip("/")
        if not base_url.endswith("/v2"):
            base_url = f"{base_url}/v2"
        return f"{base_url}/"

# #################### 4
    def _get_trading_base_url(self):
        """Return trading API base URL used by the clock endpoint."""
        env_base_url = os.getenv("APCA_API_BASE_URL") or os.getenv("ALPACA_TRADING_BASE_URL")
        if env_base_url:
            return self._normalize_base_url(env_base_url, self.PAPER_TRADING_BASE_URL)

        paper_setting = os.getenv("ALPACA_PAPER", "true").strip().lower()
        default_url = (
            self.LIVE_TRADING_BASE_URL
            if paper_setting in {"0", "false", "no"}
            else self.PAPER_TRADING_BASE_URL
        )
        return self._normalize_base_url(None, default_url)

# #################### 5
# _INIT_ helper method
#
    def _get_feed(self):
        """Return configured stock data feed, defaulting to free-plan IEX."""
        feed = self.args.get("alpaca_feed") if isinstance(self.args, dict) else None
        feed = feed or os.getenv("ALPACA_DATA_FEED") or os.getenv("APCA_API_DATA_FEED") or "iex"
        feed = feed.strip().lower()
        if feed not in self.VALID_FEEDS:
            raise ValueError(
                f"Invalid Alpaca data feed '{feed}'. "
                f"Valid feeds: {', '.join(sorted(self.VALID_FEEDS))}"
            )
        return feed

##################################################################
# Core class methods
##################################################################

# #################### 6
    def _get_json(self, url, params, action):
        """GET an Alpaca JSON endpoint and log useful details on API errors."""
        try:
            response = requests.get(
                url,
                headers=self.get_headers(),
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError:
            self._log_response_error(response, action)
            raise

# #################### 6
# Helper method for _get_json()
#
    def get_headers(self):
        """Return headers for Alpaca API requests"""
        return {
            "accept": "application/json",
            "APCA-API-KEY-ID": self.api_key,
            "APCA-API-SECRET-KEY": self.secret_key
        }

# #################### 8
# Helper method for _get_json()
#
    def _log_response_error(self, response, action):
        """Log Alpaca error responses without exposing credentials."""
        try:
            response_body = response.json()
        except ValueError:
            response_body = response.text.strip()

        base_message = (
            f"{action} failed: HTTP {response.status_code} for {response.url}. "
            f"Alpaca response: {response_body}"
        )

        if response.status_code == 401:
            logging.error(
                "%s. Check that %s and %s contain a matching Alpaca key/secret "
                "pair for this account/environment, and that the APCA-* auth "
                "headers are being accepted.",
                base_message,
                self.api_key_env_var,
                self.secret_key_env_var,
            )
            return

        response_text = str(response_body).lower()
        if (
            response.status_code in {403, 422}
            and "subscription" in response_text
            and "sip" in response_text
        ):
            logging.error(
                "%s. Your account likely cannot query realtime SIP data. Use "
                "ALPACA_DATA_FEED=iex for the free feed or subscribe to SIP.",
                base_message,
            )
            return

        logging.error(base_message)

 # #################### 9
 # builds a list of quote data for 1 single symbol
    def get_live_quote(self, symbol, feed=None):
        """Get live quote for a single symbol"""
        url = f"{self.base_url}stocks/quotes/latest"
        params = {"symbols": symbol.upper(), "feed": feed or self.feed}
        
        try:
            data = self._get_json(url, params, f"fetching quote for {symbol.upper()}")
            
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

# #################### 10
    def get_bars(self, symbol, timeframe="1Min", start_date=None, end_date=None, limit=100, feed=None):
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
            "feed": feed or self.feed,
            "sort": "asc"
        }
        
        try:
            data = self._get_json(url, params, f"fetching bars for {symbol.upper()}")
            
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

# #################### 11
# send in a list of multiple symbols
#
    def get_multiple_quotes(self, symbols_list, feed=None):
        """Get live quotes for multiple symbols"""
        if isinstance(symbols_list, str):
            symbols_list = [symbols_list]
            
        symbols_str = ",".join([s.upper() for s in symbols_list])
        url = f"{self.base_url}stocks/quotes/latest"
        params = {"symbols": symbols_str, "feed": feed or self.feed}
        
        try:
            data = self._get_json(url, params, "fetching multiple quotes")
            
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

# #################### 12
# A Helper method
# WARNING: his method is BADLY named !!
# - It doesnt primarily format the data.... 
# - It actually does the real API data get() via the network
#
# leverged by: 
# - get_multiple_quotes()
# - get_live_quote
#
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

# #################### 13
# A Helper methodf
# used by get_live_quote()
# TARNING: his method is BADLY named !!
# - It doesnt primarily format the bar data.... 
# - It actually does the real API data get() via the network
#
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

# #################### 14
# assumes a list of symbols & their quotes allready exists in 
# # the prebuilt working dict: quote_data{}
#
    def build_quote_df(self):
        """Build DataFrame from collected quote data"""
        if not self.quote_data:
            return pd.DataFrame()
            
        quotes_list = []
        for symbol, quote in self.quote_data.items():
            quotes_list.append(quote)
            
        return pd.DataFrame(quotes_list)

# #################### 15
# assumes a list of symbols & their quotes allready exists in 
# # the prebuilt working dict: quote_data{}
    def print_quotes(self):
        """Print formatted quote data"""
        if not self.quote_data:
            print("No quote data available")
            return
            
        print("========== Alpaca Live Quotes ==========")
        for symbol, quote in self.quote_data.items():
            print(f"{symbol}: Bid: ${quote['Bid_price']:.2f} Ask: ${quote['Ask_price']:.2f} Last: ${quote['Last_price']:.2f}")

# #################### 16
    def get_market_status(self):
        """Check if market is open"""
        url = f"{self.trading_base_url}clock"
        try:
            data = self._get_json(url, None, "fetching market status")
            return data.get('is_open', False)
        except requests.exceptions.RequestException:
            return False

# #################### 17
"""
# DISABLED
def show_data(data):
    # Legacy function for backward compatibility
    my_keys = data.keys()
    list_of_my_keys = list(my_keys)
    print ( f"Look at the {list_of_my_keys[0]} data in a Pretty & Nicer format..." )
    print ( f"-----------------------------------------------------------------------" )
    data_list = data[list_of_my_keys[0]]
    for i in range(len(data_list)):
        print ( f"DATA ITEM #{i} is -> {data_list[i]}" )

    return data_list
"""

############################## MAIN #############################################
#
# This is example usage and testing code for  the alpaca_md class
# Tghe module is a class only. Not a executable code.
# TODO: Delete main() when module is tagged as STABLE


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
        print(f"TEST #1: Getting a single quote for 1 symbol{test_symbol}...")
        quote = alpaca.get_live_quote(test_symbol)
        if quote:
            print(f"Quote data: {quote}")
        
        # Test multiple quotes
        test_symbols = ["IBM", "MU", "AMD", "AAPL", "GOOGL", "MSFT", "NVDA", "RKLB", "TSLA"]
        print(f"\nTEST #1: Getting multiple quotes for {test_symbols}...")
        quotes = alpaca.get_multiple_quotes(test_symbols)
        alpaca.print_quotes()
        
        # Test bars data
        print(f"\nTEST #3: Getting bars data for 1 symbol {test_symbol}...")
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
