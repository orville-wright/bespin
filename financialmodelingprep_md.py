#!/usr/bin/env python3
import requests
import pandas as pd
import logging
import os
import dotenv
from datetime import datetime, timedelta
from rich import print

class financialmodelingprep_md:
    """
    FinancialModelingPrep API extractor for comprehensive financial data
    Provides real-time quotes, historical data, fundamentals, and financial statements
    Free tier: 250 API calls/day (requires API key from https://financialmodelingprep.com)
    """
    
    def __init__(self, instance_id, global_args=None):
        self.instance_id = instance_id
        self.args = global_args or {}
        self.base_url = "https://financialmodelingprep.com/api/v3"
        
        # Load environment variables from .env file
        load_status = dotenv.load_dotenv()
        if load_status is False:
            logging.warning('Environment variables not loaded from .env file.')
        
        # Get API key from environment
        self.api_key = os.getenv('FINANCIALMODELINGPREP_API_KEY')
        if not self.api_key:
            logging.warning("FINANCIALMODELINGPREP_API_KEY not found in environment. Some features may not work.")
        
        self.session = requests.Session()
        self.quotes_df = pd.DataFrame()
        self.historical_df = pd.DataFrame()
        self.fundamentals_df = pd.DataFrame()
        
        logging.info(f"FinancialModelingPrep extractor initialized - Instance #{instance_id}")
    
    def get_quote(self, symbols):
        """
        Get real-time quotes for symbols
        symbols: string or list of ticker symbols (max 3 for batch requests)
        """
        if not self.api_key:
            logging.error("FinancialModelingPrep API key required")
            return pd.DataFrame()
        
        try:
            if isinstance(symbols, str):
                symbols = [symbols]
            
            # API supports batch requests with comma-separated symbols
            symbols_str = ','.join([s.upper() for s in symbols[:3]])  # Limit to 3 symbols
            
            url = f"{self.base_url}/quote/{symbols_str}"
            params = {
                'apikey': self.api_key
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                
                # Standardize column names
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s', errors='coerce')
                
                # Add calculated fields
                if 'changesPercentage' not in df.columns and 'change' in df.columns and 'price' in df.columns:
                    df['changesPercentage'] = (df['change'] / (df['price'] - df['change'])) * 100
                
                self.quotes_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching quotes: {e}")
            return pd.DataFrame()
    
    def get_historical_data(self, symbol, date_from=None, date_to=None, timeseries=None):
        """
        Get historical daily price data
        timeseries: number of recent periods to return (overrides date range)
        """
        if not self.api_key:
            logging.error("FinancialModelingPrep API key required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/historical-price-full/{symbol.upper()}"
            params = {
                'apikey': self.api_key
            }
            
            if timeseries:
                params['timeseries'] = timeseries
            else:
                if date_from:
                    params['from'] = date_from
                if date_to:
                    params['to'] = date_to
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if 'historical' in data and data['historical']:
                df = pd.DataFrame(data['historical'])
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = data.get('symbol', symbol.upper())
                
                # Standardize column names and order
                columns_order = ['symbol', 'date', 'open', 'high', 'low', 'close', 'adjClose', 'volume', 'unadjustedVolume', 'change', 'changePercent', 'vwap', 'label', 'changeOverTime']
                existing_cols = [col for col in columns_order if col in df.columns]
                df = df[existing_cols]
                df = df.sort_values('date')
                
                self.historical_df = df
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching historical data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_company_profile(self, symbol):
        """
        Get company profile information
        """
        if not self.api_key:
            logging.error("FinancialModelingPrep API key required")
            return {}
        
        try:
            url = f"{self.base_url}/profile/{symbol.upper()}"
            params = {
                'apikey': self.api_key
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data and len(data) > 0:
                profile = data[0]
                return {
                    'symbol': profile.get('symbol'),
                    'companyName': profile.get('companyName'),
                    'price': profile.get('price'),
                    'beta': profile.get('beta'),
                    'volAvg': profile.get('volAvg'),
                    'mktCap': profile.get('mktCap'),
                    'lastDiv': profile.get('lastDiv'),
                    'range': profile.get('range'),
                    'changes': profile.get('changes'),
                    'currency': profile.get('currency'),
                    'cik': profile.get('cik'),
                    'isin': profile.get('isin'),
                    'cusip': profile.get('cusip'),
                    'exchange': profile.get('exchange'),
                    'exchangeShortName': profile.get('exchangeShortName'),
                    'industry': profile.get('industry'),
                    'website': profile.get('website'),
                    'description': profile.get('description'),
                    'ceo': profile.get('ceo'),
                    'sector': profile.get('sector'),
                    'country': profile.get('country'),
                    'fullTimeEmployees': profile.get('fullTimeEmployees'),
                    'phone': profile.get('phone'),
                    'address': profile.get('address'),
                    'city': profile.get('city'),
                    'state': profile.get('state'),
                    'zip': profile.get('zip'),
                    'dcfDiff': profile.get('dcfDiff'),
                    'dcf': profile.get('dcf'),
                    'image': profile.get('image'),
                    'ipoDate': profile.get('ipoDate'),
                    'defaultImage': profile.get('defaultImage'),
                    'isEtf': profile.get('isEtf'),
                    'isActivelyTrading': profile.get('isActivelyTrading')
                }
            
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching company profile for {symbol}: {e}")
            return {}
    
    def get_income_statement(self, symbol, period='annual', limit=5):
        """
        Get income statement data
        period: annual or quarter
        """
        if not self.api_key:
            logging.error("FinancialModelingPrep API key required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/income-statement/{symbol.upper()}"
            params = {
                'apikey': self.api_key,
                'period': period,
                'limit': limit
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = symbol.upper()
                df = df.sort_values('date', ascending=False)
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching income statement for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_balance_sheet(self, symbol, period='annual', limit=5):
        """
        Get balance sheet data
        period: annual or quarter
        """
        if not self.api_key:
            logging.error("FinancialModelingPrep API key required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/balance-sheet-statement/{symbol.upper()}"
            params = {
                'apikey': self.api_key,
                'period': period,
                'limit': limit
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = symbol.upper()
                df = df.sort_values('date', ascending=False)
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching balance sheet for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_cash_flow(self, symbol, period='annual', limit=5):
        """
        Get cash flow statement data
        period: annual or quarter
        """
        if not self.api_key:
            logging.error("FinancialModelingPrep API key required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/cash-flow-statement/{symbol.upper()}"
            params = {
                'apikey': self.api_key,
                'period': period,
                'limit': limit
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = symbol.upper()
                df = df.sort_values('date', ascending=False)
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching cash flow for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_key_metrics(self, symbol, period='annual', limit=5):
        """
        Get key financial metrics
        """
        if not self.api_key:
            logging.error("FinancialModelingPrep API key required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/key-metrics/{symbol.upper()}"
            params = {
                'apikey': self.api_key,
                'period': period,
                'limit': limit
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = symbol.upper()
                df = df.sort_values('date', ascending=False)
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching key metrics for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_financial_ratios(self, symbol, period='annual', limit=5):
        """
        Get financial ratios
        """
        if not self.api_key:
            logging.error("FinancialModelingPrep API key required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/ratios/{symbol.upper()}"
            params = {
                'apikey': self.api_key,
                'period': period,
                'limit': limit
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                df['date'] = pd.to_datetime(df['date'])
                df['symbol'] = symbol.upper()
                df = df.sort_values('date', ascending=False)
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching financial ratios for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_market_gainers(self):
        """
        Get market gainers
        """
        if not self.api_key:
            logging.error("FinancialModelingPrep API key required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/stock_market/gainers"
            params = {
                'apikey': self.api_key
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching market gainers: {e}")
            return pd.DataFrame()
    
    def get_market_losers(self):
        """
        Get market losers
        """
        if not self.api_key:
            logging.error("FinancialModelingPrep API key required")
            return pd.DataFrame()
        
        try:
            url = f"{self.base_url}/stock_market/losers"
            params = {
                'apikey': self.api_key
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data:
                df = pd.DataFrame(data)
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching market losers: {e}")
            return pd.DataFrame()
    
    def get_market_summary(self, symbols_list):
        """
        Get market summary for multiple symbols
        """
        try:
            return self.get_quote(symbols_list)
        except Exception as e:
            logging.error(f"Error getting market summary: {e}")
            return pd.DataFrame()

def main():
    """Example usage of financialmodelingprep_md class"""
    try:
        fmp = financialmodelingprep_md(1)
        
        print("========== FinancialModelingPrep API Test ==========")
        
        test_symbol = "AAPL"
        print(f"Getting quote for {test_symbol}...")
        quote = fmp.get_quote([test_symbol])
        if not quote.empty:
            print(f"Quote data: {quote.iloc[0].to_dict()}")
        
        print(f"\nGetting company profile for {test_symbol}...")
        profile = fmp.get_company_profile(test_symbol)
        if profile:
            print(f"Company: {profile.get('companyName')} ({profile.get('sector')})")
            print(f"Market Cap: {profile.get('mktCap')}")
        
        print(f"\nGetting historical data for {test_symbol}...")
        historical = fmp.get_historical_data(test_symbol, timeseries=5)
        if not historical.empty:
            print(f"Historical data shape: {historical.shape}")
            print(historical.head())
        
        test_symbols = ["AAPL", "GOOGL", "MSFT"]
        print(f"\nGetting market summary for {test_symbols}...")
        summary = fmp.get_market_summary(test_symbols)
        if not summary.empty:
            print(summary[['symbol', 'price', 'changesPercentage']])
            
    except Exception as e:
        print(f"Error in main: {e}")
        logging.error(f"Error in financialmodelingprep_md main: {e}")

if __name__ == '__main__':
    main()