#!/usr/bin/env python3
import requests
import pandas as pd
import logging
import os
import dotenv
from datetime import datetime, timedelta
from rich import print

class finnhub_md:
    """
    Finnhub API data extractor for real-time and historical market data
    Provides stock quotes, historical data, company fundamentals, and news
    Requires Finnhub API key (free tier: 60 calls/minute at https://finnhub.io)
    """
    
    def __init__(self, instance_id, global_args=None):
        self.instance_id = instance_id
        self.args = global_args or {}
        self.base_url = "https://finnhub.io/api/v1"
        
        # Load environment variables from .env file
        load_status = dotenv.load_dotenv()
        if load_status is False:
            logging.warning('Environment variables not loaded from .env file.')
        
        # Get API key from environment
        self.api_key = os.getenv('FINNHUB_API_KEY')
        if not self.api_key:
            logging.warning("FINNHUB_API_KEY not found in environment. Some features may not work.")
        
        self.session = requests.Session()
        self.quotes_df = pd.DataFrame()
        self.candles_df = pd.DataFrame()
        self.fundamentals_df = pd.DataFrame()
        
        logging.info(f"Finnhub data extractor initialized - Instance #{instance_id}")
    
    def get_headers(self):
        """Return headers for Finnhub API requests"""
        return {
            'X-Finnhub-Token': self.api_key
        }
    
    def get_quote(self, symbol):
        """
        Get real-time quote for a symbol
        Returns current price, change, percent change, high, low, open, previous close
        """
        if not self.api_key:
            logging.error("Finnhub API key required")
            return {}
        
        try:
            url = f"{self.base_url}/quote"
            params = {'symbol': symbol.upper()}
            
            response = self.session.get(url, headers=self.get_headers(), params=params)
            response.raise_for_status()
            
            data = response.json()
            if data and 'c' in data:
                return {
                    'symbol': symbol.upper(),
                    'current_price': data.get('c'),
                    'change': data.get('d'),
                    'percent_change': data.get('dp'),
                    'high': data.get('h'),
                    'low': data.get('l'),
                    'open': data.get('o'),
                    'previous_close': data.get('pc'),
                    'timestamp': data.get('t'),
                    'source': 'Finnhub'
                }
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching quote for {symbol}: {e}")
            return {}
    
    def get_candles(self, symbol, resolution='D', days_back=30):
        """
        Get historical candlestick data for a symbol
        resolution: 1, 5, 15, 30, 60, D, W, M
        """
        if not self.api_key:
            logging.error("Finnhub API key required")
            return pd.DataFrame()
        
        try:
            end_time = int(datetime.now().timestamp())
            start_time = int((datetime.now() - timedelta(days=days_back)).timestamp())
            
            url = f"{self.base_url}/stock/candle"
            params = {
                'symbol': symbol.upper(),
                'resolution': resolution,
                'from': start_time,
                'to': end_time
            }
            
            response = self.session.get(url, headers=self.get_headers(), params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('s') == 'ok' and data.get('c'):
                df = pd.DataFrame({
                    'timestamp': pd.to_datetime(data['t'], unit='s'),
                    'open': data['o'],
                    'high': data['h'],
                    'low': data['l'],
                    'close': data['c'],
                    'volume': data['v']
                })
                df['symbol'] = symbol.upper()
                df = df[['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume']]
                
                self.candles_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching candles for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_company_profile(self, symbol):
        """
        Get company profile information
        """
        if not self.api_key:
            logging.error("Finnhub API key required")
            return {}
        
        try:
            url = f"{self.base_url}/stock/profile2"
            params = {'symbol': symbol.upper()}
            
            response = self.session.get(url, headers=self.get_headers(), params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                return {
                    'symbol': symbol.upper(),
                    'name': data.get('name'),
                    'country': data.get('country'),
                    'currency': data.get('currency'),
                    'exchange': data.get('exchange'),
                    'ipo_date': data.get('ipo'),
                    'market_cap': data.get('marketCapitalization'),
                    'phone': data.get('phone'),
                    'shares_outstanding': data.get('shareOutstanding'),
                    'ticker': data.get('ticker'),
                    'website': data.get('weburl'),
                    'industry': data.get('finnhubIndustry'),
                    'logo': data.get('logo')
                }
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching company profile for {symbol}: {e}")
            return {}
    
    def get_basic_financials(self, symbol):
        """
        Get basic financial metrics for a symbol
        """
        if not self.api_key:
            logging.error("Finnhub API key required")
            return {}
        
        try:
            url = f"{self.base_url}/stock/metric"
            params = {
                'symbol': symbol.upper(),
                'metric': 'all'
            }
            
            response = self.session.get(url, headers=self.get_headers(), params=params)
            response.raise_for_status()
            
            data = response.json()
            if data and 'metric' in data:
                metrics = data['metric']
                return {
                    'symbol': symbol.upper(),
                    'pe_ratio': metrics.get('peBasicExclExtraTTM'),
                    'market_cap': metrics.get('marketCapitalization'),
                    'dividend_yield': metrics.get('dividendYielTTM'),
                    'beta': metrics.get('beta'),
                    'eps_ttm': metrics.get('epsExclExtraItemsTTM'),
                    'revenue_ttm': metrics.get('revenueTTM'),
                    'profit_margin': metrics.get('netProfitMarginTTM'),
                    'roa': metrics.get('roaTTM'),
                    'roe': metrics.get('roeTTM'),
                    'debt_to_equity': metrics.get('totalDebt/totalEquityQuarterly'),
                    'current_ratio': metrics.get('currentRatioQuarterly'),
                    '52_week_high': metrics.get('52WeekHigh'),
                    '52_week_low': metrics.get('52WeekLow')
                }
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching basic financials for {symbol}: {e}")
            return {}
    
    def get_company_news(self, symbol, days_back=7):
        """
        Get recent news for a company
        """
        if not self.api_key:
            logging.error("Finnhub API key required")
            return pd.DataFrame()
        
        try:
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/company-news"
            params = {
                'symbol': symbol.upper(),
                'from': start_date,
                'to': end_date
            }
            
            response = self.session.get(url, headers=self.get_headers(), params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                if not df.empty:
                    df['datetime'] = pd.to_datetime(df['datetime'], unit='s')
                    df['symbol'] = symbol.upper()
                    df = df[['symbol', 'datetime', 'headline', 'summary', 'url', 'source']]
                
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching news for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_dividends(self, symbol, start_date=None, end_date=None):
        """
        Get dividend history for a symbol
        """
        if not self.api_key:
            logging.error("Finnhub API key required")
            return pd.DataFrame()
        
        try:
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365*2)).strftime('%Y-%m-%d')
            if not end_date:
                end_date = datetime.now().strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/stock/dividend"
            params = {
                'symbol': symbol.upper(),
                'from': start_date,
                'to': end_date
            }
            
            response = self.session.get(url, headers=self.get_headers(), params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                if not df.empty:
                    df['date'] = pd.to_datetime(df['date'])
                    df['symbol'] = symbol.upper()
                    df = df[['symbol', 'date', 'amount']]
                
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching dividends for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_market_summary(self, symbols_list):
        """
        Get market summary for multiple symbols
        """
        summary_data = []
        
        for symbol in symbols_list:
            try:
                quote = self.get_quote(symbol)
                profile = self.get_company_profile(symbol)
                
                if quote and profile:
                    summary_data.append({
                        'symbol': symbol.upper(),
                        'name': profile.get('name', 'N/A'),
                        'exchange': profile.get('exchange', 'N/A'),
                        'current_price': quote.get('current_price'),
                        'change': quote.get('change'),
                        'percent_change': quote.get('percent_change'),
                        'high': quote.get('high'),
                        'low': quote.get('low'),
                        'open': quote.get('open'),
                        'previous_close': quote.get('previous_close'),
                        'market_cap': profile.get('market_cap'),
                        'currency': profile.get('currency')
                    })
                    
            except Exception as e:
                logging.error(f"Error in market summary for {symbol}: {e}")
                continue
        
        return pd.DataFrame(summary_data)

def main():
    """Example usage of finnhub_md class"""
    try:
        finnhub = finnhub_md(1)
        
        print("========== Finnhub Market Data Test ==========")
        
        test_symbol = "AAPL"
        print(f"Getting quote for {test_symbol}...")
        quote = finnhub.get_quote(test_symbol)
        if quote:
            print(f"Quote data: {quote}")
        
        print(f"\nGetting company profile for {test_symbol}...")
        profile = finnhub.get_company_profile(test_symbol)
        if profile:
            print(f"Company: {profile.get('name')} ({profile.get('exchange')})")
        
        print(f"\nGetting historical data for {test_symbol}...")
        candles = finnhub.get_candles(test_symbol, resolution='D', days_back=10)
        if not candles.empty:
            print(f"Candles data shape: {candles.shape}")
            print(candles.head())
        
        test_symbols = ["AAPL", "GOOGL", "MSFT"]
        print(f"\nGetting market summary for {test_symbols}...")
        summary = finnhub.get_market_summary(test_symbols)
        if not summary.empty:
            print(summary[['symbol', 'name', 'current_price', 'percent_change']])
            
    except Exception as e:
        print(f"Error in main: {e}")
        logging.error(f"Error in finnhub_md main: {e}")

if __name__ == '__main__':
    main()