#!/usr/bin/env python3
import requests
import pandas as pd
import logging
import os
import dotenv
from datetime import datetime, timedelta
from rich import print
from polygon import RESTClient

class polygon_md:
    """
    Polygon.io API data extractor for real-time and historical market data
    Requires Polygon API key (free tier available at https://polygon.io)
    """
    
    def __init__(self, instance_id, global_args=None):
        self.instance_id = instance_id
        self.args = global_args or {}
        self.base_url = "https://api.polygon.io"
        
        # Load environment variables from .env file
        load_status = dotenv.load_dotenv()
        if load_status is False:
            logging.warning('Environment variables not loaded from .env file.')
        
        # Get API key from environment
        self.api_key = os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            logging.warning("POLYGON_API_KEY not found in environment. Some features may not work.")
        
        self.session = requests.Session()
        self.quotes_df = pd.DataFrame()
        self.bars_df = pd.DataFrame()
        
        logging.info(f"Polygon.io data extractor initialized - Instance #{instance_id}")
    ############################## end init ##############################

    def get_last_quote(self, symbol):
        """
        Get the most recent quote for a symbol
        WARN: This is a PREMIUM data service. 
              Free API key does not have entitlement to this data
              But, we cna get it from other servies. We have many.
        Error JSON looks like this...
        {"status":"NOT_AUTHORIZED","request_id":"4b507f34bd5c54dc530a10d0a14b5706","message":"You are not entitled to this data. Please upgrade your plan at https://polygon.io/pricing"}
              
        """
        if not self.api_key:
            logging.error("Polygon API key required")
            return {}
        
        try:
            url = f"{self.base_url}/v2/last/nbbo/{symbol.upper()}"
            params = {'apikey': self.api_key}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'OK':
                results = data.get('results', {})
                return {
                    'symbol': symbol.upper(),
                    'bid': results.get('bid'),
                    'bid_size': results.get('bidSize'),
                    'ask': results.get('ask'),
                    'ask_size': results.get('askSize'),
                    'timestamp': results.get('timestamp'),
                    'spread': results.get('ask', 0) - results.get('bid', 0) if results.get('ask') and results.get('bid') else None
                }
            return {}       # return and empty dict if data.get != OK
        except Exception as e:
            #logging.error(f"Error fetching last quote for {symbol}: {e}")
            data = response.json()
            if data["status"] == "NOT_AUTHORIZED":
                ERROR = {'status': 'NOT_AUTHORIZED', 'reason': '[Premium $$ service requried for this data]'}
                #print (f"\n#### DEBUG:\n{data["status"]}")
            return ERROR

    ############################## 1 ##############################
    def get_aggregates(self, symbol, multiplier=1, timespan='day', from_date=None, to_date=None, limit=120):
        """
        Get aggregate bars for a symbol
        timespan: minute, hour, day, week, month, quarter, year
        """
        if not self.api_key:
            logging.error("Polygon API key required")
            return pd.DataFrame()

        logging.info(f"Fetching polygon.io aggregates for {symbol}")
        try:
            if not from_date:
                from_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d')
            if not to_date:
                to_date = datetime.now().strftime('%Y-%m-%d')
            
            logging.info(f"Date rage set - From: {from_date} / To: {to_date}")
            url = f"{self.base_url}/v2/aggs/ticker/{symbol.upper()}/range/{multiplier}/{timespan}/{from_date}/{to_date}"
            params = {
                'apikey': self.api_key,
                'adjusted': 'true',
                'sort': 'asc',
                'limit': limit
            }
            
            # directly manipulate and construct the  URL as the params var seems to not work.
            params="adjusted=true&sort=asc&limit=120&apiKey="+self.api_key
            final_url = url+"?"+"params="+params
            
            # REferenc code
            # TODO: convert the main function below to this new method
            # WARN: This is the new method
            #      With the new new formatted LIST[] data structure reponse
            # Data struct: 
            # [Agg(open=279.305, high=284.5, low=278.6657, close=281.83, volume=3685321.0, vwap=282.291, timestamp=1750046400000, transactions=73186, otc=None)]
            poly_client = RESTClient(self.api_key)
            aggs = []
            for a in poly_client.list_aggs(
                symbol.upper(),
                multiplier,
                timespan,
                from_date,
                to_date,
                adjusted="true",
                sort="asc",
                limit=120,
            ):
                aggs.append(a)                 
            # accessor method...
            # item = aggs[0]
            # print (f"{item.open}")
            # print (f"Number of rows: {len(aggs)}")

            # WARN: This is the old method
            #       with the old JSON dict response
            # Data struct:
            # {'ticker': 'IBM', 'queryCount': 20, 'resultsCount': 20, 'adjusted': True, 'results': 
            # [{'v': 3418007.0, 'vw': 281.3442, 'o': 281.53, 'c': 281.03, 'h': 283.06, 'l': 279.83, 't': 1749700800000, 'n': 63263}], 
            # 'status': 'DELAYED', 'request_id': '39be2ce16c054f3ef77f8fb2ff91a9c7', 'count': 20}
            response = self.session.get(final_url)
            response.raise_for_status()
            logging.info(f"resp: {self.session.params}...")
          
            data = response.json()
            if data.get('results'):
                logging.info(f"Good Data extracted...")
                results = data['results']
                
                logging.info(f"Build Dataframe...")
                df = pd.DataFrame(results)
                df.columns = ['vol', 'vwap', 'open', 'close', 'high', 'low', 'time', 'xtns']     # name columns       
                df['time'] = pd.to_datetime(df['time'], unit='ms')              # Convert timestamp to datetime
                df['symbol'] = symbol.upper()                                   # add sumbol columns and daya
                df = df[['symbol', 'time', 'open', 'high', 'low', 'close', 'vol', 'vwap', 'xtns']]   # Reorder columns
                
                self.bars_df = df
                logging.info(f"Data extracted...")
                return self.bars_df
            else:
                logging.info(f"NO data extratced... {data.get('status')}")
                return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"Error fetching aggregates for {symbol}: {e}")
            return pd.DataFrame()

    ############################## 2 ##############################
    def get_daily_open_close(self, symbol, date=None):
        """
        Get open, high, low, close for a specific date
        """
        if not self.api_key:
            logging.error("Polygon API key required")
            return {}
        
        try:
            if not date:
                date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            
            url = f"{self.base_url}/v1/open-close/{symbol.upper()}/{date}"
            params = {'apikey': self.api_key, 'adjusted': 'true'}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'OK':
                return {
                    'symbol': data.get('symbol'),
                    'date': data.get('from'),
                    'open': data.get('open'),
                    'high': data.get('high'),
                    'low': data.get('low'),
                    'close': data.get('close'),
                    'volume': data.get('volume'),
                    'pre_market': data.get('preMarket'),
                    'after_hours': data.get('afterHours')
                }
            return {}

        except Exception as e:
            logging.error(f"Error fetching daily OHLC for {symbol}: {e}")
            return {}
    
############################## 3 ##############################            
    def get_market_holidays(self, year=None):
        """
        Get market holidays
        """
        if not self.api_key:
            logging.error("Polygon API key required")
            return []
        
        try:
            if not year:
                year = datetime.now().year
                
            url = f"{self.base_url}/v1/marketstatus/upcoming"
            params = {'apikey': self.api_key}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get('results', [])
            
        except Exception as e:
            logging.error(f"Error fetching market holidays: {e}")
            return []

############################## 4 ##############################    
    def get_market_status(self):
        """
        Get current market status
        """
        if not self.api_key:
            logging.error("Polygon API key required")
            return {}
        
        try:
            url = f"{self.base_url}/v1/marketstatus/now"
            params = {'apikey': self.api_key}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data
            
        except Exception as e:
            logging.error(f"Error fetching market status: {e}")
            return {}

############################## 5 ##############################
    def get_company_info(self, symbol):
        """
        Get detailed information about a ticker
        """
        if not self.api_key:
            logging.error("Polygon API key required")
            return {}
        
        try:
            url = f"{self.base_url}/v3/reference/tickers/{symbol.upper()}"
            params = {'apikey': self.api_key}
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            if data.get('status') == 'OK':
                results = data.get('results', {})
                return {
                    'ticker': results.get('ticker'),
                    'name': results.get('name'),
                    'market': results.get('market'),
                    'locale': results.get('locale'),
                    'primary_exchange': results.get('primary_exchange'),
                    'type': results.get('type'),
                    'currency_name': results.get('currency_name'),
                    'description': results.get('description'),
                    'market_cap': results.get('market_cap'),
                    'share_class_shares_outstanding': results.get('share_class_shares_outstanding'),
                    'weighted_shares_outstanding': results.get('weighted_shares_outstanding')
                }
            return {}
            
        except Exception as e:
            logging.error(f"Error fetching ticker details for {symbol}: {e}")
            return {}