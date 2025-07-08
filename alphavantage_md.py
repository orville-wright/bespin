#!/usr/bin/env python3
import requests
import pandas as pd
import logging
import os
import dotenv
import json
from datetime import datetime, timedelta
from rich import print

class alphavantage_md:
    """
    Alpha Vantage API data extractor for stock prices, technical indicators, and fundamental data
    Provides real-time quotes, historical data, technical indicators, and company fundamentals
    Requires Alpha Vantage API key (free tier available at https://www.alphavantage.co/support/#api-key)
    """

    instance_id = 0
    
    def __init__(self, instance_id, global_args=None):
        cmi_debug = __name__+"::"+self.__init__.__name__
        self.instance_id = instance_id
        logging.info( f'%s  - Instantiate.#{instance_id}' % cmi_debug )
        self.args = global_args or {}
        self.base_url = "https://www.alphavantage.co/query"
        
        # Load environment variables from .env file
        load_status = dotenv.load_dotenv()
        logging.info( f'%s  - Load keys from .env file' % cmi_debug )
        if load_status is False:
            logging.warning(f'%s - Env loaded failed !' % cmi_debug)
        
        # Get API key from environment
        self.api_key = os.getenv('ALPHAVANTAGE_API_KEY')
        if not self.api_key:
            logging.warning( f"%s - ALPHAVANTAGE_API_KEY not found!" % cmi_debug)
        
        self.session = requests.Session()
        self.quotes_df = pd.DataFrame()
        self.timeseries_df = pd.DataFrame()
        self.indicators_df = pd.DataFrame()
        self.fundamentals_df = pd.DataFrame()
    
    def get_global_quote(self, symbol):
        """
        Get real-time quote for a symbol
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return {}
        
        try:
            params = {
                'function': 'GLOBAL_QUOTE',
                'symbol': symbol.upper(),
                'apikey': self.api_key
            }
            
            cmi_debug = __name__+"::"+self.get_global_quote.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Global Quote' in data:
                quote = data['Global Quote']
                return {
                    'symbol': quote.get('01. symbol'),
                    'open': float(quote.get('02. open', 0)),
                    'high': float(quote.get('03. high', 0)),
                    'low': float(quote.get('04. low', 0)),
                    'price': float(quote.get('05. price', 0)),
                    'volume': int(quote.get('06. volume', 0)),
                    'latest_trading_day': quote.get('07. latest trading day'),
                    'previous_close': float(quote.get('08. previous close', 0)),
                    'change': float(quote.get('09. change', 0)),
                    'change_percent': quote.get('10. change percent', '0%').rstrip('%')
                }
            
            # Check for error messages
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
            
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching global quote for {symbol}: {e}")
            return {}
    
    def get_intraday_data(self, symbol, interval='5min', outputsize='compact'):
        """
        Get intraday time series data
        interval: 1min, 5min, 15min, 30min, 60min
        outputsize: compact (latest 100 data points) or full (full-length data)
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return pd.DataFrame()
        
        try:
            params = {
                'function': 'TIME_SERIES_INTRADAY',
                'symbol': symbol.upper(),
                'interval': interval,
                'outputsize': outputsize,
                'apikey': self.api_key
            }
            
            cmi_debug = __name__+"::"+self.get_intraday_data.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for time series data
            time_series_key = f'Time Series ({interval})'
            if time_series_key in data:
                time_series = data[time_series_key]
                
                df_data = []
                for timestamp, values in time_series.items():
                    df_data.append({
                        'symbol': symbol.upper(),
                        'timestamp': pd.to_datetime(timestamp),
                        'open': float(values['1. open']),
                        'high': float(values['2. high']),
                        'low': float(values['3. low']),
                        'close': float(values['4. close']),
                        'volume': int(values['5. volume'])
                    })
                
                df = pd.DataFrame(df_data)
                df = df.sort_values('timestamp').reset_index(drop=True)
                
                self.timeseries_df = df
                return df
            
            # Check for error messages
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching intraday data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_daily_data(self, symbol, outputsize='compact'):
        """
        Get daily time series data
        outputsize: compact (latest 100 data points) or full (20+ years)
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return pd.DataFrame()
        
        try:
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol.upper(),
                'outputsize': outputsize,
                'apikey': self.api_key
            }
            
            cmi_debug = __name__+"::"+self.get_daily_data.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )            
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Time Series (Daily)' in data:
                time_series = data['Time Series (Daily)']
                
                df_data = []
                for date, values in time_series.items():
                    df_data.append({
                        'symbol': symbol.upper(),
                        'date': pd.to_datetime(date),
                        'open': float(values['1. open']),
                        'high': float(values['2. high']),
                        'low': float(values['3. low']),
                        'close': float(values['4. close']),
                        'volume': int(values['5. volume'])
                    })
                
                df = pd.DataFrame(df_data)
                df = df.sort_values('date').reset_index(drop=True)
                
                self.timeseries_df = df
                return df
            
            # Check for error messages
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching daily data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_weekly_data(self, symbol):
        """
        Get weekly time series data
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return pd.DataFrame()
        
        try:
            params = {
                'function': 'TIME_SERIES_WEEKLY',
                'symbol': symbol.upper(),
                'apikey': self.api_key
            }

            cmi_debug = __name__+"::"+self.get_weekly_data.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )               
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Weekly Time Series' in data:
                time_series = data['Weekly Time Series']
                
                df_data = []
                for date, values in time_series.items():
                    df_data.append({
                        'symbol': symbol.upper(),
                        'date': pd.to_datetime(date),
                        'open': float(values['1. open']),
                        'high': float(values['2. high']),
                        'low': float(values['3. low']),
                        'close': float(values['4. close']),
                        'volume': int(values['5. volume'])
                    })
                
                df = pd.DataFrame(df_data)
                df = df.sort_values('date').reset_index(drop=True)
                
                return df
            
            # Check for error messages
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching weekly data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_monthly_data(self, symbol):
        """
        Get monthly time series data
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return pd.DataFrame()
        
        try:
            params = {
                'function': 'TIME_SERIES_MONTHLY',
                'symbol': symbol.upper(),
                'apikey': self.api_key
            }

            cmi_debug = __name__+"::"+self.get_monthly_data.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )               
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Monthly Time Series' in data:
                time_series = data['Monthly Time Series']
                
                df_data = []
                for date, values in time_series.items():
                    df_data.append({
                        'symbol': symbol.upper(),
                        'date': pd.to_datetime(date),
                        'open': float(values['1. open']),
                        'high': float(values['2. high']),
                        'low': float(values['3. low']),
                        'close': float(values['4. close']),
                        'volume': int(values['5. volume'])
                    })
                
                df = pd.DataFrame(df_data)
                df = df.sort_values('date').reset_index(drop=True)
                
                return df
            
            # Check for error messages
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching monthly data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_sma(self, symbol, interval='daily', time_period=20, series_type='close'):
        """
        Get Simple Moving Average (SMA) technical indicator
        interval: 1min, 5min, 15min, 30min, 60min, daily, weekly, monthly
        time_period: number of data points used to calculate SMA
        series_type: close, open, high, low
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return pd.DataFrame()
        
        try:
            params = {
                'function': 'SMA',
                'symbol': symbol.upper(),
                'interval': interval,
                'time_period': time_period,
                'series_type': series_type,
                'apikey': self.api_key
            }

            cmi_debug = __name__+"::"+self.get_sma.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )               
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Technical Analysis: SMA' in data:
                technical_data = data['Technical Analysis: SMA']
                
                df_data = []
                for date, values in technical_data.items():
                    df_data.append({
                        'symbol': symbol.upper(),
                        'date': pd.to_datetime(date),
                        'sma': float(values['SMA'])
                    })
                
                df = pd.DataFrame(df_data)
                df = df.sort_values('date').reset_index(drop=True)
                
                self.indicators_df = df
                return df
            
            # Check for error messages
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching SMA for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_rsi(self, symbol, interval='daily', time_period=14, series_type='close'):
        """
        Get Relative Strength Index (RSI) technical indicator
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return pd.DataFrame()
        
        try:
            params = {
                'function': 'RSI',
                'symbol': symbol.upper(),
                'interval': interval,
                'time_period': time_period,
                'series_type': series_type,
                'apikey': self.api_key
            }

            cmi_debug = __name__+"::"+self.get_rsi.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )               
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'Technical Analysis: RSI' in data:
                technical_data = data['Technical Analysis: RSI']
                
                df_data = []
                for date, values in technical_data.items():
                    df_data.append({
                        'symbol': symbol.upper(),
                        'date': pd.to_datetime(date),
                        'rsi': float(values['RSI'])
                    })
                
                df = pd.DataFrame(df_data)
                df = df.sort_values('date').reset_index(drop=True)
                
                return df
            
            # Check for error messages
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching RSI for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_company_overview(self, symbol):
        """
        Get company overview and fundamental data
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return {}
        
        try:
            params = {
                'function': 'OVERVIEW',
                'symbol': symbol.upper(),
                'apikey': self.api_key
            }

            cmi_debug = __name__+"::"+self.get_company_overview.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )               
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for error messages first
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
                return {}
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
                return {}
            
            # Return company overview data
            if 'Symbol' in data:
                return {
                    'symbol': data.get('Symbol'),
                    'name': data.get('Name'),
                    'description': data.get('Description'),
                    'exchange': data.get('Exchange'),
                    'currency': data.get('Currency'),
                    'country': data.get('Country'),
                    'sector': data.get('Sector'),
                    'industry': data.get('Industry'),
                    'market_cap': data.get('MarketCapitalization'),
                    'pe_ratio': data.get('PERatio'),
                    'peg_ratio': data.get('PEGRatio'),
                    'book_value': data.get('BookValue'),
                    'dividend_per_share': data.get('DividendPerShare'),
                    'dividend_yield': data.get('DividendYield'),
                    'eps': data.get('EPS'),
                    'revenue_per_share_ttm': data.get('RevenuePerShareTTM'),
                    'profit_margin': data.get('ProfitMargin'),
                    'operating_margin_ttm': data.get('OperatingMarginTTM'),
                    'return_on_assets_ttm': data.get('ReturnOnAssetsTTM'),
                    'return_on_equity_ttm': data.get('ReturnOnEquityTTM'),
                    'revenue_ttm': data.get('RevenueTTM'),
                    'gross_profit_ttm': data.get('GrossProfitTTM'),
                    'diluted_eps_ttm': data.get('DilutedEPSTTM'),
                    'quarterly_earnings_growth_yoy': data.get('QuarterlyEarningsGrowthYOY'),
                    'quarterly_revenue_growth_yoy': data.get('QuarterlyRevenueGrowthYOY'),
                    'analyst_target_price': data.get('AnalystTargetPrice'),
                    'trailing_pe': data.get('TrailingPE'),
                    'forward_pe': data.get('ForwardPE'),
                    'price_to_sales_ratio_ttm': data.get('PriceToSalesRatioTTM'),
                    'price_to_book_ratio': data.get('PriceToBookRatio'),
                    'ev_to_revenue': data.get('EVToRevenue'),
                    'ev_to_ebitda': data.get('EVToEBITDA'),
                    'beta': data.get('Beta'),
                    '52_week_high': data.get('52WeekHigh'),
                    '52_week_low': data.get('52WeekLow'),
                    '50_day_moving_average': data.get('50DayMovingAverage'),
                    '200_day_moving_average': data.get('200DayMovingAverage'),
                    'shares_outstanding': data.get('SharesOutstanding'),
                    'dividend_date': data.get('DividendDate'),
                    'ex_dividend_date': data.get('ExDividendDate')
                }
            
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching company overview for {symbol}: {e}")
            return {}
    
    def get_income_statement(self, symbol):
        """
        Get annual income statement data
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return pd.DataFrame()
        
        try:
            params = {
                'function': 'INCOME_STATEMENT',
                'symbol': symbol.upper(),
                'apikey': self.api_key
            }

            cmi_debug = __name__+"::"+self.get_income_statement.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )               
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'annualReports' in data:
                annual_reports = data['annualReports']
                
                df_data = []
                for report in annual_reports:
                    df_data.append({
                        'symbol': symbol.upper(),
                        'fiscal_date_ending': report.get('fiscalDateEnding'),
                        'reported_currency': report.get('reportedCurrency'),
                        'gross_profit': report.get('grossProfit'),
                        'total_revenue': report.get('totalRevenue'),
                        'cost_of_revenue': report.get('costOfRevenue'),
                        'cost_of_goods_and_services_sold': report.get('costofGoodsAndServicesSold'),
                        'operating_income': report.get('operatingIncome'),
                        'selling_general_administrative': report.get('sellingGeneralAndAdministrative'),
                        'research_and_development': report.get('researchAndDevelopment'),
                        'operating_expenses': report.get('operatingExpenses'),
                        'investment_income_net': report.get('investmentIncomeNet'),
                        'net_interest_income': report.get('netInterestIncome'),
                        'interest_income': report.get('interestIncome'),
                        'interest_expense': report.get('interestExpense'),
                        'non_interest_income': report.get('nonInterestIncome'),
                        'other_non_operating_income': report.get('otherNonOperatingIncome'),
                        'depreciation': report.get('depreciation'),
                        'depreciation_and_amortization': report.get('depreciationAndAmortization'),
                        'income_before_tax': report.get('incomeBeforeTax'),
                        'income_tax_expense': report.get('incomeTaxExpense'),
                        'interest_and_debt_expense': report.get('interestAndDebtExpense'),
                        'net_income_from_continuing_ops': report.get('netIncomeFromContinuingOps'),
                        'comprehensive_income_net_of_tax': report.get('comprehensiveIncomeNetOfTax'),
                        'ebit': report.get('ebit'),
                        'ebitda': report.get('ebitda'),
                        'net_income': report.get('netIncome')
                    })
                
                df = pd.DataFrame(df_data)
                df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending'])
                df = df.sort_values('fiscal_date_ending').reset_index(drop=True)
                
                self.fundamentals_df = df
                return df
            
            # Check for error messages
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching income statement for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_search_results(self, keywords):
        """
        Search for symbols by keywords
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return pd.DataFrame()
        
        try:
            params = {
                'function': 'SYMBOL_SEARCH',
                'keywords': keywords,
                'apikey': self.api_key
            }

            cmi_debug = __name__+"::"+self.get_search_results.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )               
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'bestMatches' in data:
                matches = data['bestMatches']
                
                df_data = []
                for match in matches:
                    df_data.append({
                        'symbol': match.get('1. symbol'),
                        'name': match.get('2. name'),
                        'type': match.get('3. type'),
                        'region': match.get('4. region'),
                        'market_open': match.get('5. marketOpen'),
                        'market_close': match.get('6. marketClose'),
                        'timezone': match.get('7. timezone'),
                        'currency': match.get('8. currency'),
                        'match_score': float(match.get('9. matchScore', 0))
                    })
                
                df = pd.DataFrame(df_data)
                df = df.sort_values('match_score', ascending=False).reset_index(drop=True)
                
                return df
            
            # Check for error messages
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
            
            return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error searching for symbols with keywords '{keywords}': {e}")
            return pd.DataFrame()
    
    def get_market_status(self):
        """
        Get current market status
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return {}
        
        try:
            params = {
                'function': 'MARKET_STATUS',
                'apikey': self.api_key
            }

            cmi_debug = __name__+"::"+self.get_market_status.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )               
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'markets' in data:
                return data
            
            # Check for error messages
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
            
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching market status: {e}")
            return {}
    
    def get_top_gainers_losers(self):
        """
        Get top gainers and losers
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return {}
        
        try:
            params = {
                'function': 'TOP_GAINERS_LOSERS',
                'apikey': self.api_key
            }

            cmi_debug = __name__+"::"+self.get_top_gainers_losers.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )               
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            #print ( f"#### DEBUG: JSON:\n{json.dumps(data, indent=2)}" ) 
            
            if 'top_gainers' in data:
                cmi_debug = __name__+"::"+self.get_top_gainers_losers.__name__+".#"+str(self.instance_id)
                logging.info( f'%s  - Build DICT from JSON get()' % cmi_debug )
                result = {
                    'metadata': data.get('metadata', {}),
                    'last_updated': data.get('last_updated', {}),
                    'top_gainers': pd.DataFrame(data.get('top_gainers', [])),
                    'top_losers': pd.DataFrame(data.get('top_losers', [])),
                    'most_actively_traded': pd.DataFrame(data.get('most_actively_traded', []))
                }
                logging.info( f'%s  - DICT result sucessfully built' % cmi_debug )
                #print ( f"#### DEBUG: {type(result)}\n{result}" )
                return result       # dict
            
            # Check for error messages
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
            
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching top gainers/losers: {e}")
            return {}

    ############################## news ##########################
    def market_news(self, tickers=None, topics=None, time_from=None, time_to=None, sort='LATEST', limit=50):
        """
        Get live and historical market news with sentiment analysis
        tickers: Filter by stock/crypto/forex symbols (e.g., 'AAPL' or 'AAPL,MSFT')
        topics: Filter by news categories ('technology', 'earnings', 'blockchain', etc.)
        time_from: Start time for articles (format: YYYYMMDDTHHMM)
        time_to: End time for articles (format: YYYYMMDDTHHMM)
        sort: Sort order ('LATEST', 'EARLIEST', 'RELEVANCE')
        limit: Number of results (default 50, max 1000)
        """
        if not self.api_key:
            logging.error("Alpha Vantage API key required")
            return {}
        
        try:
            params = {
                'function': 'NEWS_SENTIMENT',
                'apikey': self.api_key
            }
            
            # Add optional parameters if provided
            if tickers:
                params['tickers'] = tickers
            if topics:
                params['topics'] = topics
            if time_from:
                params['time_from'] = time_from
            if time_to:
                params['time_to'] = time_to
            if sort:
                params['sort'] = sort
            if limit:
                params['limit'] = str(limit)
            
            cmi_debug = __name__+"::"+self.market_news.__name__+".#"+str(self.instance_id)
            logging.info( f'%s  - API endpoint get()' % cmi_debug )
            response = self.session.get(self.base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            #print ( f"#### DEBUG: JSON:\n{json.dumps(data, indent=2)}" )
            
            if 'feed' in data:
                cmi_debug = __name__+"::"+self.market_news.__name__+".#"+str(self.instance_id)
                logging.info( f'%s  - Build result from JSON' % cmi_debug )
                
                # Process news articles into structured format
                articles = []
                for article in data.get('feed', []):
                    article_data = {
                        'title': article.get('title'),
                        'url': article.get('url'),
                        'time_published': article.get('time_published'),
                        'authors': article.get('authors', []),
                        'summary': article.get('summary'),
                        'banner_image': article.get('banner_image'),
                        'source': article.get('source'),
                        'category_within_source': article.get('category_within_source'),
                        'source_domain': article.get('source_domain'),
                        'topics': article.get('topics', []),
                        'overall_sentiment_score': float(article.get('overall_sentiment_score', 0)),
                        'overall_sentiment_label': article.get('overall_sentiment_label'),
                        'ticker_sentiment': article.get('ticker_sentiment', [])
                    }
                    articles.append(article_data)
                
                result = {
                    'items': data.get('items'),
                    'sentiment_score_definition': data.get('sentiment_score_definition'),
                    'relevance_score_definition': data.get('relevance_score_definition'),
                    'feed': articles
                }
                
                logging.info( f'%s  - News data successfully processed' % cmi_debug )
                return result
            
            # Check for error messages
            if 'Error Message' in data:
                logging.error(f"Alpha Vantage API error: {data['Error Message']}")
            elif 'Note' in data:
                logging.warning(f"Alpha Vantage API note: {data['Note']}")
            elif 'Information' in data:
                logging.warning(f"Alpha Vantage API info: {data['Information']}")
            
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching market news: {e}")
            return {}

############################## MAIN #############################################
# Example usage and testing of the alphavantage_md class
def main():
    """Example usage of alphavantage_md class"""
    try:
        # Initialize Alpha Vantage market data instance
        av = alphavantage_md(1)
        
        print("========== Alpha Vantage Market Data Test ==========")
        
        # Test symbol search
        test_search = "Apple"
        print(f"Searching for '{test_search}'...")
        search_results = av.get_search_results(test_search)
        if not search_results.empty:
            print(f"Search results shape: {search_results.shape}")
            print(search_results.head())
        
        # Test global quote
        test_symbol = "AAPL"
        print(f"\nGetting global quote for {test_symbol}...")
        quote = av.get_global_quote(test_symbol)
        if quote:
            print(f"Quote data: {quote}")
        
        # Test daily data
        print(f"\nGetting daily data for {test_symbol}...")
        daily_df = av.get_daily_data(test_symbol, outputsize='compact')
        if not daily_df.empty:
            print(f"Daily data shape: {daily_df.shape}")
            print(daily_df.head())
        
        # Test company overview
        print(f"\nGetting company overview for {test_symbol}...")
        overview = av.get_company_overview(test_symbol)
        if overview:
            print(f"Company: {overview.get('name')} ({overview.get('symbol')})")
            print(f"Sector: {overview.get('sector')}")
            print(f"Market Cap: {overview.get('market_cap')}")
            print(f"P/E Ratio: {overview.get('pe_ratio')}")
        
        # Test technical indicator
        print(f"\nGetting SMA for {test_symbol}...")
        sma_df = av.get_sma(test_symbol, interval='daily', time_period=20)
        if not sma_df.empty:
            print(f"SMA data shape: {sma_df.shape}")
            print(sma_df.head())
        
        # Test market status
        print(f"\nGetting market status...")
        market_status = av.get_market_status()
        if market_status:
            print(f"Market status retrieved successfully")
        
        # Test market news
        print(f"\nGetting market news for {test_symbol}...")
        news_data = av.market_news(tickers=test_symbol, limit=5)
        if news_data and 'feed' in news_data:
            print(f"News articles retrieved: {len(news_data['feed'])}")
            if news_data['feed']:
                first_article = news_data['feed'][0]
                print(f"Latest article: {first_article.get('title')}")
                print(f"Sentiment: {first_article.get('overall_sentiment_label')} ({first_article.get('overall_sentiment_score')})")
        
    except Exception as e:
        print(f"Error in main: {e}")
        logging.error(f"Error in alphavantage_md main: {e}")

if __name__ == '__main__':
    main()