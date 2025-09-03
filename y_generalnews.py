#! python3
import requests
from requests_html import HTMLSession
from bs4 import BeautifulSoup
import pandas as pd
#import modin.pandas as pd
import numpy as np
import re
import logging
import argparse
import time
import hashlib
#from rich import print
#from rich.markup import escape

import asyncio
import os
import json
import time
from pathlib import Path
from typing import List

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, CrawlResult
from crawl4ai import JsonCssExtractionStrategy
from crawl4ai import LLMConfig
from crawl4ai import BrowserConfig

__cur_dir__ = Path(__file__).parent

# logging setup
logging.basicConfig(level=logging.INFO)

#####################################################
class y_generalnews:
    """
    ABANDONED + DEPRECDATED
    DELETE ME
    Class to extract Genral Macro news from finance.yahoo.com
    """
    # global accessors
    tg_df0 = None        # DataFrame - Full list of top gainers
    tg_df1 = None        # DataFrame - Ephemerial list of top 10 gainers. Allways overwritten
    tg_df2 = None        # DataFrame - Top 10 ever 10 secs for 60 secs
    all_tag_tr = None    # BS4 handle of the <tr> extracted data
    rows_extr = 0        # number of rows of data extracted
    ext_req = None       # request was handled by y_cookiemonster
    yti = 0
    cycle = 0            # class thread loop counter
    get_counter = 0      # count of get() requests
    yfn_jsdb = {}
    yfn_htmldata = None

    dummy_url = "https://finance.yahoo.com/markets/stocks/most-active/"

    yahoo_headers = { \
                        'authority': 'finance.yahoo.com', \
                        'path': '/markets/stocks/most-active/', \
                        'referer': 'https://finance.yahoo.com/markets/', \
                        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="99", "Chromium";v="123"', \
                        'sec-ch-ua-mobile': '"?0"', \
                        'sec-fetch-mode': 'cors', \
                        'sec-fetch-site': 'cross-site', \
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36' }

    def __init__(self, yti):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s Instance.#{yti}' % cmi_debug )
        # init empty DataFrame with present colum names
        self.tg_df0 = pd.DataFrame(columns=[ 'Row', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time'] )
        #self.tg_df1 = pd.DataFrame(columns=[ 'ERank', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time'] )
        #self.tg_df2 = pd.DataFrame(columns=[ 'ERank', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time'] )
        self.yti = yti
        return

#method 1
    def init_dummy_session(self):
        self.dummy_resp0 = requests.get(self.dummy_url, stream=True, headers=self.yahoo_headers, cookies=self.yahoo_headers, timeout=5 )
        hot_cookies = requests.utils.dict_from_cookiejar(self.dummy_resp0.cookies)
        #self.js_session.cookies.update({'A1': self.js_resp0.cookies['A1']} )    # yahoo cookie hack
        return

#method 1.2

    def update_headers(self, ch):

        # HACK to help logging() f-string bug to handle strings with %
        cmi_debug = __name__+"::"+self.update_headers.__name__+".#"+str(self.yti)+"  - "+ch
        logging.info('%s' % cmi_debug )
        cmi_debug = __name__+"::"+self.update_headers.__name__+".#"+str(self.yti)
        self.path = ch
        self.ext_req.cookies.update({'path': self.path} )
 
        if self.args['bool_xray'] is True:
            print ( f"=========================== {self.yti} / session cookies ===========================" )
            for i in self.ext_req.cookies.items():
                print ( f"{i}" )

        return

###############################################################

    def do_simple_get(self, url):
        """
        get simple raw HTML data structure (data not processed by JAVAScript engine)
        NOTE: get URL is assumed to have allready been set (self.yfqnews_url)
                Copies exact pattern from working y_topgainers.py file
        """
        cmi_debug = __name__+"::"+self.do_simple_get.__name__+".#"+str(self.yti)
        logging.info( f'%s  - CYCLE: {self.get_counter}' % cmi_debug )

        js_session = HTMLSession()                  # Create a new session        
        with js_session.get(url) as self.js_resp0:  # must do a get() - NO setting cookeis/headers)
            logging.info(f'%s  - Simple HTML Request get()...' % cmi_debug )

        # HACK to help logging() f-string bug to handle strings with %
            cmi_debug = __name__+"::"+self.do_simple_get.__name__+".#"+str(self.yti)+"  - JS get() success: "+url
            logging.info('%s' % cmi_debug )
            #logging.info( f"%s - JS_session.get() sucessful: {url}" % cmi_debug )
            cmi_debug = __name__+"::"+self.do_simple_get.__name__+".#"+str(self.yti)    # reset cmi_debug
            if self.js_resp0.status_code != 200:
                    logging.error(f'{cmi_debug} - HTTP {self.js_resp0.status_code}: HTML fetch FAILED')
                    return None

        self.get_counter += 1
        logging.info( f"%s  - js.render()... diasbled" % cmi_debug )
        logging.info( f'%s  - Store basic HTML dataset' % cmi_debug )
        self.js_resp2 = self.js_resp0               # Set js_resp2 to the same response as js_resp0 for now
        hot_cookies = requests.utils.dict_from_cookiejar(self.js_resp0.cookies)
        logging.info( f"%s - Swap {len(hot_cookies)} cookies into LOCAL yahoo_headers" % cmi_debug )

        self.yfn_htmldata = self.js_resp0.text
        auh = hashlib.sha256(url.encode())          # hash the url
        aurl_hash = auh.hexdigest()
        logging.info( f'%s  - CREATE cache entry: [ {aurl_hash} ]' % cmi_debug )
        self.yfn_jsdb[aurl_hash] = self.js_resp0    # create CACHE entry in jsdb !!response, not full page TEXT data !!

        # Xray DEBUG
        '''
        if self.args['bool_xray'] is True:
            print ( f"========================== {self.yti} / HTML get() session cookies ================================" )
            logging.info( f'%s  - resp0 type: {type(self.js_resp0)}' % cmi_debug )
            for i in self.js_resp0.cookies.items():
                print ( f"{i}" )
        '''

        return self.js_resp0

###################################################################################
# method #2
    def ext_get_data(self, yti):
        """
        Connect to finance.yahoo.com and extract (scrape) the raw string data out of
        the webpage data tables. Returns a BS4 handle.
        Send hint which engine processed & rendered the html page
        not implimented yet...
            0. Simple HTML engine
            1. JAVASCRIPT HTML render engine (down redering a complex JS page in to simple HTML)
        """
        self.yti = yti
        cmi_debug = __name__+"::"+self.ext_get_data.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        logging.info('%s - ext request pre-processed by cookiemonster' % cmi_debug )
        # use preexisting resposne from  managed req (handled by cookie monster) 
        #self.yfn_htmldata
        r = self.ext_req
        #print (f"###DEBUG:\n{r.text}")
        
        logging.info( f"%s - Craw4ai stream processing... {r.url}" % cmi_debug )
        #self.soup = BeautifulSoup(self.yfn_htmldata, 'html.parser')
        #self.soup = BeautifulSoup(r.text, 'html.parser')
        
        # Craw4ai testing...
        # CSS zone and selectors
        # BS4 style:  <div> class=column-container yf-1ce4p3e
        # craw4ai:    div.news-stream.yf-19i1jx3 ul li a.subtle-link.titles
        # alt:        <ul> stream-items yf-1drgw5l
        #             class="stream-item story-item yf-1drgw5l"
        #self.tag_tbody = self.soup.find('column-container yf-1ce4p3e')
        #self.tag_tbody = self.soup.ul.find('stream-items')
        # ("div", attrs={"class": "column column--full supportive-data"} )
        # soup.div.find_all(attrs={'class': 'D(tbc)'} )

        #self.tag_tbody = self.soup.find_all(attrs={"class": "column-container"} )
        #self.tag_tbody = self.soup.find_all(attrs={"class": "news-stream yf-19i1jx3"} )
        #print ( f"#### DEBUG\n#### COUNT: {len(self.tag_tbody)}\n#### DATA: {self.tag_tbody}" )
        #print ( f"#### DEBUG\n{r.text}" )       # the raw html page we opened. before BS4 processing
        print ( f"URL extracted: {r.url} ")
        asyncio.run(self.css_struct_extract_schema(r.url))
        #self.tr_rows = self.tag_tbody.find_all("li")
        logging.info('%s Page processed by BS4 engine' % cmi_debug )
        return

######################################################################################
# craw4ai
    async def css_struct_extract_schema(self, this_url):
        """Extract structured data using CSS selectors"""
        # Check if schema file exists
        print ( f"### DEBUG: Loading YF schema / recvd URL: {this_url}" )
        schema_file_path = f"{__cur_dir__}/YF_MainNews_schema.json"
        if os.path.exists(schema_file_path):
            with open(schema_file_path, "r") as f:
                schema = json.load(f)
                print ( f"{json.dumps(schema, indent=2)}" )
                extraction_strategy = JsonCssExtractionStrategy(schema)
                config = CrawlerRunConfig(extraction_strategy=extraction_strategy, verbose=True)
            
                # Use the fast CSS extraction (no LLM calls during extraction)
                async with AsyncWebCrawler(config=BrowserConfig(headless=True,
                                                                verbose=True,
                                                                )) as crawler:
                #async with AsyncWebCrawler() as crawler:
                    results: List[CrawlResult] = await crawler.arun(
                        this_url, config=config
                    )

                    for result in results:
                        print(f"#### DEBUG: URL: {result.url}")
                        print(f"#### DEBUG: Success: {result.success}")
                        if result.success:
                            #print ( f"{result.cleaned_html}" )
                            data = json.loads(result.extracted_content)
                            print(json.dumps(data, indent=2))
                            print (f"###DEBUG:\n{result}")
                        else:
                            print("Failed to extract structured data")
        else:
            # Generate schema using LLM (one-time setup)
            print ( f"### DEBUG: FAILED to load YF schema..." )
            return 1
                    # Create no-LLM extraction strategy with the generated schema
        return

######################################################################################
 
# method #3
    def build_df0(self):
        """
        Build-out a fully populated Pandas DataFrame containg all the extracted/scraped fields from the
        html/markup table data Wrangle, clean/convert/format the data correctly.
        """

        cmi_debug = __name__+"::"+self.build_df0.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        time_now = time.strftime("%H:%M:%S", time.localtime() )
        logging.info('%s - Create clean NULL DataFrame' % cmi_debug )
        self.tg_df0 = pd.DataFrame()             # new df, but is NULLed
        x = 0

        # CSS zone:  div.news-stream.yf-19i1jx3 ul li a.subtle-link.titles
        #            <div> class=column-container yf-1ce4p3e
        self.rows_extr = int( len(self.tag_tbody.find_all('li')) )
        self.rows_tr_rows = int( len(self.tr_rows) )
        #logging.info( f'%s - Rows 1 extracted: {self.rows_extr}' % cmi_debug )
        #logging.info( f'%s - Rows 2 extracted: {self.rows_tr_rows}' % cmi_debug )

        for datarow in self.tr_rows:

            # >>>DEBUG<< for whedatarow.stripped_stringsn yahoo.com changes data model...
            y = 1
            print ( f"===================== Debug =========================" )
            #print ( f"Data {y}: {datarow}" )
            for i in datarow.find_all("a"):
                print ( f"===================================================" )
                print ( f"Data {y}: {i.text}" )
                print ( f"Data g: {next(i.stripped_strings)}" )
                #logging.info( f'%s - Data: {debug_data.strings}' % cmi_debug )
                y += 1
            print ( f"===================== Debug =========================" )
            # >>>DEBUG<< for when yahoo.com changes data model...
          
            # Data Extractor Generator
            def extr_gen(): 
                for i in datarow.find_all("li"):
                    yield ( f"{next(i.stripped_strings)}" )

            ################################ 1 ####################################
            extr_strs = extr_gen()
            co_sym = next(extr_strs)             # 1 : ticker symbol info / e.g "NWAU"
            co_name = next(extr_strs)            # 2 : company name / e.g "Consumer Automotive Finance, Inc."
            mini_chart = next(extr_strs)         # 3 : embeded mini GFX chart
            price = next(extr_strs)              # 3 : price (Intraday) / e.g "0.0031"

            ################################ 2 ####################################

            change_sign = next(extr_strs)        # 4 : test for dedicated column for +/- indicator
            logging.info( f"{cmi_debug} : {co_sym} : Check $ CHANGE dedicated [+-] field..." )
            if change_sign == "+" or change_sign == "-":    # 4 : is $ change sign [+/-] a dedciated field
                change_val = next(extr_strs)     # 4 : Yes, advance iterator to next field (ignore dedciated sign field)
            else:
                change_val = change_sign         # 4 : get $ change, but its possibly +/- signed
                #if (re.search(r'\+', change_val)) or (re.search(r'\-', change_val)) is True:
                if (re.search(r'\+', change_val)) or (re.search(r'\-', change_val)) is not None:
                    logging.info( f"{cmi_debug} : $ CHANGE: {change_val} [+-], stripping..." )
                    change_cl = re.sub(r'[\+\-]', "", change_val)       # remove +/- sign
                    logging.info( f"{cmi_debug} : $ CHANGE cleaned to: {change_cl}" )
                else:
                    logging.info( f"{cmi_debug} : {change_val} : $ CHANGE is NOT signed [+-]" )
                    change_cl = re.sub(r'[\,]', "", change_val)       # remove
                    logging.info( f"{cmi_debug} : $ CHANGE: {change_cl}" )

            pct_sign = next(extr_strs)              # 5 : test for dedicated column for +/- indicator
            logging.info( f"{cmi_debug} : {co_sym} : Check % CHANGE dedicated [+-] field..." )
            if pct_sign == "+" or pct_sign == "-":  # 5 : is %_change sign [+/-] a dedciated field
                pct_val = next(extr_strs)           # 5 : advance iterator to next field (ignore dedciated sign field)
            else:
                pct_val = pct_sign                  # 5 get % change, but its possibly +/- signed
                if (re.search(r'\+', pct_val)) or (re.search(r'\-', pct_val)) is not None:
                    logging.info( f"{cmi_debug} : % CHANGE {pct_val} [+-], stripping..." )
                    pct_cl = re.sub(r'[\+\-\%]', "", pct_val)       # remove +/-/% signs
                    logging.info( f"{cmi_debug} : % CHANGE cleaned to: {pct_cl}" )
                else:
                    logging.info( f"{cmi_debug} : {pct_val} : % CHANGE is NOT signed [+-]" )
                    change_cl = re.sub(r'[\,\%]', "", pct_val)       # remove
                    logging.info( f"{cmi_debug} : % CHANGE: {pct_val}" )
 
            ################################ 3 ####################################
            vol = next(extr_strs)            # 6 : volume with scale indicator/ e.g "70.250k"
            avg_vol = next(extr_strs)        # 7 : Avg. vol over 3 months) / e.g "61,447"
            mktcap = next(extr_strs)         # 8 : Market cap with scale indicator / e.g "15.753B"
            peratio = next(extr_strs)        # 9 : PE ratio TTM (Trailing 12 months) / e.g "N/A"
            #mini_gfx = next(extr_strs)      # 10 : IGNORED = mini-canvas graphic 52-week rnage (no TXT/strings avail)

            ################################ 4 ####################################
            # now wrangle the data...
            co_sym_lj = f"{co_sym:<6}"                                   # left justify TXT in DF & convert to raw string
            co_name_lj = np.array2string(np.char.ljust(co_name, 60) )    # left justify TXT in DF & convert to raw string
            co_name_lj = (re.sub(r'[\'\"]', '', co_name_lj) )             # remove " ' and strip leading/trailing spaces     
            price_cl = (re.sub(r'\,', '', price))                         # remove ,
            price_clean = float(price_cl)
            change_clean = float(change_val)

            if pct_val == "N/A":
                pct_val = float(0.0)                               # Bad data. FOund a filed with N/A instead of read num
            else:
                pct_cl = re.sub(r'[\%\+\-,]', "", pct_val )
                pct_clean = float(pct_cl)

            ################################ 5 ####################################
            mktcap = (re.sub(r'[N\/A]', '0', mktcap))               # handle N/A
            TRILLIONS = re.search('T', mktcap)
            BILLIONS = re.search('B', mktcap)
            MILLIONS = re.search('M', mktcap)

            if TRILLIONS:
                mktcap_clean = float(re.sub('T', '', mktcap))
                mb = "LT"
                logging.info( f'%s : #{x} : {co_sym_lj} Mkt Cap: TRILLIONS : T' % cmi_debug )

            if BILLIONS:
                mktcap_clean = float(re.sub('B', '', mktcap))
                mb = "LB"
                logging.info( f'%s : #{x} : {co_sym_lj} Mkt cap: BILLIONS : B' % cmi_debug )

            if MILLIONS:
                mktcap_clean = float(re.sub('M', '', mktcap))
                mb = "LM"
                logging.info( f'%s : #{x} : {co_sym_lj} Mkt cap: MILLIONS : M' % cmi_debug )

            if not TRILLIONS and not BILLIONS and not MILLIONS:
                mktcap_clean = 0    # error condition - possible bad data
                mb = "LZ"           # Zillions
                logging.info( f'%s : #{x} : {co_sym_lj} bad mktcap data N/A : Z' % cmi_debug )
                # handle bad data in mktcap html page field

            ################################ 6 ####################################
            # now construct our list for concatinating to the dataframe 
            logging.info( f"%s ============= Data prepared for DF =============" % cmi_debug )

            self.list_data = [[ \
                       x, \
                       re.sub(r'\'', '', co_sym_lj), \
                       co_name_lj, \
                       price_clean, \
                       change_clean, \
                       pct_clean, \
                       mktcap_clean, \
                       mb, \
                       time_now ]]

            ################################ 6 ####################################
            self.df_1_row = pd.DataFrame(self.list_data, columns=[ 'Row', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time' ], index=[x] )
            self.tg_df0 = pd.concat([self.tg_df0, self.df_1_row])  
            x+=1

        logging.info('%s - populated new DF0 dataset' % cmi_debug )
        return x        # number of rows inserted into DataFrame (0 = some kind of #FAIL)
                        # sucess = lobal class accessor (y_toplosers.tg_df0) populated & updated

# method #4
    def topg_listall(self):
        """Print the full DataFrame table list of Yahoo Finance Top Gainers"""
        """Sorted by % Change"""

        cmi_debug = __name__+"::"+self.topg_listall.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        pd.set_option('display.max_rows', None)
        pd.set_option('max_colwidth', 30)
        print ( self.tg_df0.sort_values(by='Pct_change', ascending=False ) )    # only do after fixtures datascience dataframe has been built
        return

# method #5
    def build_top10(self):
        """
        Get top gainers from main DF (df0) -> temp DF (df1)
        Number of rows to grab is now set from num of rows that BS4 actually extracted (rows_extr)
        df1 is ephemerial. Is allways overwritten on each run
        """

        cmi_debug = __name__+"::"+self.build_top10.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        logging.info('%s - Drop all rows from DF1' % cmi_debug )
        self.tg_df1.drop(self.tg_df1.index, inplace=True)
        logging.info('%s - Copy DF0 -> ephemerial DF1' % cmi_debug )
        self.tg_df1 = self.tg_df0.sort_values(by='Pct_change', ascending=False ).head(self.rows_extr).copy(deep=True)    # create new DF via copy of top 10 entries
        self.tg_df1.rename(columns = {'Row':'ERank'}, inplace = True)    # Rank is more accurate for this Ephemerial DF
        self.tg_df1.reset_index(inplace=True, drop=True)    # reset index each time so its guaranteed sequential
        return

# method #6
    def print_top10(self):
        """
        Prints the Top 10 Dataframe
        Number of rows to print is now set from num of rows that BS4 actually extracted (rows_extr)
        """

        cmi_debug = __name__+"::"+self.print_top10.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        pd.set_option('display.max_rows', None)
        pd.set_option('max_colwidth', 30)
        self.tg_df1.style.set_properties(**{'text-align': 'left'})
        print ( f"{self.tg_df1.sort_values(by='Pct_change', ascending=False ).head(self.rows_extr)}" )
        return

# method #7
    def build_tenten60(self, cycle):
        """Build-up 10x10x060 historical DataFrame (df2) from source df1"""
        """Generally called on some kind of cycle"""

        cmi_debug = __name__+"::"+self.build_tenten60.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        self.tg_df2 = self.tg_df2.append(self.tg_df1, ignore_index=False)    # merge top 10 into
        self.tg_df2.reset_index(inplace=True, drop=True)    # ensure index is allways unique + sequential
        return
