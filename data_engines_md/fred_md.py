#!/usr/bin/env python3
import requests
import pandas as pd
import logging
import os
import dotenv
from datetime import datetime, timedelta
from rich import print

class fred_md:
    """
    Federal Reserve Economic Data (FRED) API extractor
    Provides economic indicators, interest rates, inflation data
    Requires FRED API key (free from https://fred.stlouisfed.org/docs/api/api_key.html)
    """
    
    def __init__(self, instance_id, global_args=None):
        self.instance_id = instance_id
        self.args = global_args or {}
        self.base_url = "https://api.stlouisfed.org/fred"
        
        # Load environment variables from .env file
        load_status = dotenv.load_dotenv()
        if load_status is False:
            logging.warning('Environment variables not loaded from .env file.')
        
        # Get API key from environment
        self.api_key = os.getenv('FRED_API_KEY')
        if not self.api_key:
            logging.warning("FRED_API_KEY not found in environment. Some features may not work.")
        
        self.session = requests.Session()
        self.economic_df = pd.DataFrame()
        
        # Common economic indicators
        self.indicators = {
            'fed_funds_rate': 'FEDFUNDS',
            'unemployment_rate': 'UNRATE', 
            'inflation_rate': 'CPIAUCSL',
            'gdp_growth': 'GDP',
            'treasury_10yr': 'GS10',
            'treasury_2yr': 'GS2',
            'consumer_sentiment': 'UMCSENT',
            'housing_starts': 'HOUST',
            'retail_sales': 'RSXFS',
            'industrial_production': 'INDPRO'
        }
        
        logging.info(f"FRED economic data extractor initialized - Instance #{instance_id}")
 
 ########################### end init ######################################## 
 
    ######################### 1 ###############################  
    def get_economic_snapshot(self):
        """
        Get current snapshot of key economic indicators
        """
        snapshot = {}
        
        for name, series_id in self.indicators.items():
            df = self.get_fred_data(series_id, limit=1)
            #print (f"\n#### DEBUG:\n{df}" )
            if not df.empty:
                latest = df.iloc[-1]
                snapshot[name] = {
                    'value': latest['value'],
                    'rt_sdate': latest['realtime_start'],
                    'rt_edate': latest['realtime_end'],
                    'date': latest['date'].strftime('%Y-%m-%d'),
                    
                    'series_id': series_id
                }
        
        return snapshot

    ######################### 2 ###############################    
    def get_yield_curve(self):
        """
        Get current Treasury yield curve data
        """
        yield_series = {
            '1_month': 'GS1M',
            '3_month': 'GS3M', 
            '6_month': 'GS6M',
            '1_year': 'GS1',
            '2_year': 'GS2',
            '3_year': 'GS3',
            '5_year': 'GS5',
            '7_year': 'GS7',
            '10_year': 'GS10',
            '20_year': 'GS20',
            '30_year': 'GS30'
        }
        
        yield_data = {}
        for maturity, series_id in yield_series.items():
            df = self.get_fred_data(series_id, limit=1)
            if not df.empty:
                yield_data[maturity] = df.iloc[-1]['value']
        
        return yield_data
    
    ######################### 3 ###############################    
    def get_economic_trends(self, days_back=365):
        """
        Get trends for key economic indicators over specified period
        """
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        trends = {}
        
        for name, series_id in self.indicators.items():
            df = self.get_fred_data(series_id, limit=1000, start_date=start_date)
            if len(df) > 1:
                current = df.iloc[-1]['value']
                previous = df.iloc[0]['value']
                change = current - previous
                pct_change = (change / previous) * 100 if previous != 0 else 0
                
                trends[name] = {
                    'current': current,
                    'start_period': previous, 
                    'change': change,
                    'pct_change': pct_change,
                    'period_days': days_back
                }
        
        return trends
    
    ######################### 4 ###############################    
    def search_series(self, search_text, limit=25):
        """
        Search for FRED data series by text
        """
        if not self.api_key:
            logging.error("FRED API key required")
            return pd.DataFrame()
        
        try:
            params = {
                'search_text': search_text,
                'api_key': self.api_key,
                'file_type': 'json',
                'limit': limit
            }
            
            url = f"{self.base_url}/series/search"
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            series = data.get('seriess', [])
            
            if not series:
                return pd.DataFrame()
            
            df = pd.DataFrame(series)
            return df[['id', 'title', 'frequency', 'units', 'last_updated']]
            
        except Exception as e:
            logging.error(f"Error searching FRED series: {e}")
            return pd.DataFrame()

    ######################### 5 ###############################
    def get_fred_data(self, series_id, limit=100, start_date=None):
            """
            Get data for a specific FRED series
            """
            if not self.api_key:
                logging.error("FRED API key required")
                return pd.DataFrame()
            
            try:
                params = {
                    'series_id': series_id,
                    'api_key': self.api_key,
                    'file_type': 'json',
                    'limit': limit
                }
                
                if start_date:
                    params['start_date'] = start_date
                
                url = f"{self.base_url}/series/observations"
                response = self.session.get(url, params=params)
                response.raise_for_status()
                
                data = response.json()
                observations = data.get('observations', [])
                
                if not observations:
                    return pd.DataFrame()
                
                df = pd.DataFrame(observations)
                df['date'] = pd.to_datetime(df['date'])
                df['value'] = pd.to_numeric(df['value'], errors='coerce')
                df = df.dropna(subset=['value'])
                
                #print (f"\n{df}")
                return df
                
            except Exception as e:
                logging.error(f"Error fetching FRED series {series_id}: {e}")
                return pd.DataFrame()