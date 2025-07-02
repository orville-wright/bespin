#!/usr/bin/env python3
import requests
import pandas as pd
import logging
import time
import dotenv
from datetime import datetime
from rich import print

#################################################

class sec_md:
    """
    SEC EDGAR API data extractor for company filings and insider trading data
    Free API with no authentication required
    """
    
    def __init__(self, instance_id, global_args=None):
        self.instance_id = instance_id
        self.args = global_args or {}
        self.base_url = "https://data.sec.gov"
        
        # Load environment variables from .env file
        load_status = dotenv.load_dotenv()
        if load_status is False:
            logging.warning('Environment variables not loaded from .env file.')
        
        self.session = requests.Session()
        
        # SEC requires User-Agent header
        self.session.headers.update({
            'User-Agent': 'financial-data-extractor contact@example.com',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'data.sec.gov'
        })
        
        self.filings_df = pd.DataFrame()
        self.insider_df = pd.DataFrame()
        
        logging.info(f"SEC EDGAR extractor initialized - Instance #{instance_id}")

    #################################################
    
    def get_company_filings(self, cik, form_type="10-K", limit=10):
        """
        Get recent filings for a company by CIK
        form_type: 10-K, 10-Q, 8-K, etc.
        """
        try:
            # Format CIK to 10 digits with leading zeros
            cik_formatted = str(cik).zfill(10)
            
            url = f"{self.base_url}/submissions/CIK{cik_formatted}.json"
            response = self.session.get(url)
            response.raise_for_status()
            
            data = response.json()
            filings = data.get('filings', {}).get('recent', {})
            
            if not filings:
                return pd.DataFrame()
            
            # Create DataFrame
            df = pd.DataFrame({
                'form': filings.get('form', []),
                'filingDate': filings.get('filingDate', []),
                'accessionNumber': filings.get('accessionNumber', []),
                'primaryDocument': filings.get('primaryDocument', []),
                'reportDate': filings.get('reportDate', [])
            })
            
            # Filter by form type if specified
            if form_type:
                df = df[df['form'] == form_type]
            
            # Limit results
            df = df.head(limit)
            
            self.filings_df = df
            return df
            
        except Exception as e:
            logging.error(f"Error fetching SEC filings: {e}")
            return pd.DataFrame()

    #################################################
    
    def get_insider_transactions(self, cik, limit=20):
        """
        Get insider trading transactions for a company
        """
        try:
            cik_formatted = str(cik).zfill(10)
            
            # This would require parsing XML files from SEC
            # For now, return placeholder structure
            url = f"{self.base_url}/submissions/CIK{cik_formatted}.json"
            response = self.session.get(url)
            response.raise_for_status()
            
            # Placeholder for insider transaction data
            # Real implementation would parse Form 4 filings
            insider_data = {
                'date': [],
                'insider_name': [],
                'title': [],
                'transaction_type': [],
                'shares': [],
                'price': []
            }
            
            self.insider_df = pd.DataFrame(insider_data)
            return self.insider_df
            
        except Exception as e:
            logging.error(f"Error fetching insider transactions: {e}")
            return pd.DataFrame()

    #################################################   
    
    def search_company_by_ticker(self, ticker):
        """
        Find company CIK by ticker symbol
        """
        try:
            url = f"{self.base_url}/files/company_tickers.json"
            response = self.session.get(url)
            response.raise_for_status()
            
            companies = response.json()
            
            for company_data in companies.values():
                if company_data.get('ticker', '').upper() == ticker.upper():
                    return {
                        'cik': company_data.get('cik_str'),
                        'title': company_data.get('title'),
                        'ticker': company_data.get('ticker')
                    }
            
            return None
            
        except Exception as e:
            logging.error(f"Error searching company by ticker: {e}")
            return None

    #################################################

    def get_latest_filing_url(self, cik, form_type="10-K"):
        """
        Get URL for latest filing document
        """
        df = self.get_company_filings(cik, form_type, limit=1)
        if df.empty:
            return None
        
        row = df.iloc[0]
        accession = row['accessionNumber'].replace('-', '')
        document = row['primaryDocument']
        
        url = f"{self.base_url}/Archives/edgar/data/{cik}/{accession}/{document}"
        return url