#!/usr/bin/env python3

from requests_html import HTMLSession
import requests
import pandas as pd
#import modin.pandas as pd
import logging
import argparse
import time
import threading
import re
from urllib.parse import urlparse
from rich import print

# logging setup
logging.basicConfig(level=logging.INFO)

# my private classes & methods
from y_topgainers import y_topgainers
from y_daylosers import y_daylosers
from y_smallcaps import smallcap_screen
from nasdaq_uvoljs import un_volumes
from nasdaq_quotes import nquote
from shallow_logic import combo_logic
from bigcharts_md import bc_quote
from data_engines_fundamentals.alpaca_md import alpaca_md
from ml_urlhinter import url_hinter
from ml_nlpreader import ml_nlpreader
from y_techevents import y_techevents
from nasdaq_wrangler import nq_wrangler
from y_cookiemonster import y_cookiemonster
from ml_sentiment import ml_sentiment
from db_graph import db_graph
from sec_md import sec_md
from data_engines_fundamentals.fred_md import fred_md
from data_engines_fundamentals.polygon_md import polygon_md
from tiingo_md import tiingo_md
from data_engines_fundamentals.alphavantage_md import alphavantage_md
from finnhub_md import finnhub_md
from marketstack_md import marketstack_md
from stockdata_md import stockdata_md
from twelvedata_md import twelvedata_md
from eodhistoricaldata_md import eodhistoricaldata_md
from financialmodelingprep_md import financialmodelingprep_md
from stooq_md import stooq_md
from y_generalnews import y_generalnews

# Globals
work_inst = 0
global args
args = {}
global parser
parser = argparse.ArgumentParser(description="Entropy apperture engine")
parser.add_argument('-a','--allnews', help='ML/NLP News sentiment AI for all stocks', action='store_true', dest='bool_news', required=False, default=False)
parser.add_argument('--alpaca', help='Get Alpaca live quotes for symbol', action='store', dest='alpaca_symbol', required=False, default=False)
parser.add_argument('--alpaca-bars', help='Get Alpaca OHLCV bars for symbol', action='store', dest='alpaca_bars', required=False, default=False)
parser.add_argument('--sec', help='Get SEC filings for symbol', action='store', dest='sec_symbol', required=False, default=False)
parser.add_argument('--fred', help='Get FRED economic data snapshot', action='store_true', dest='bool_fred', required=False, default=False)
parser.add_argument('--polygon', help='Get Polygon.io quote for symbol', action='store', dest='polygon_symbol', required=False, default=False)
parser.add_argument('--tiingo', help='Get Tiingo comprehensive data for symbol', action='store', dest='tiingo_symbol', required=False, default=False)
parser.add_argument('--tiingo-news', help='Get Tiingo financial news', action='store_true', dest='bool_tiingo_news', required=False, default=False)
parser.add_argument('--alphavantage', help='Get Alpha Vantage quote and data for symbol', action='store', dest='alphavantage_symbol', required=False, default=False)
parser.add_argument('--alphavantage-overview', help='Get Alpha Vantage company overview for symbol', action='store', dest='alphavantage_overview', required=False, default=False)
parser.add_argument('--alphavantage-intraday', help='Get Alpha Vantage intraday data for symbol', action='store', dest='alphavantage_intraday', required=False, default=False)
parser.add_argument('--alphavantage-gainers', help='Get Alpha Vantage top gainers/losers', action='store_true', dest='bool_alphavantage_gainers', required=False, default=False)
parser.add_argument('--finnhub', help='Get Finnhub quote and data for symbol', action='store', dest='finnhub_symbol', required=False, default=False)
parser.add_argument('--finnhub-news', help='Get Finnhub financial news for symbol', action='store', dest='finnhub_news_symbol', required=False, default=False)
parser.add_argument('--marketstack', help='Get Marketstack EOD and intraday data for symbol', action='store', dest='marketstack_symbol', required=False, default=False)
parser.add_argument('--stockdata', help='Get StockData.org quote and data for symbol', action='store', dest='stockdata_symbol', required=False, default=False)
parser.add_argument('--twelvedata', help='Get Twelve Data comprehensive data for symbol', action='store', dest='twelvedata_symbol', required=False, default=False)
parser.add_argument('--eodhistoricaldata', help='Get EOD Historical Data for symbol', action='store', dest='eodhistoricaldata_symbol', required=False, default=False)
parser.add_argument('--financialmodelingprep', help='Get FinancialModelingPrep data for symbol', action='store', dest='financialmodelingprep_symbol', required=False, default=False)
parser.add_argument('--stooq', help='Get Stooq historical data for symbol', action='store', dest='stooq_symbol', required=False, default=False)
parser.add_argument('-c','--cycle', help='Ephemerial top 10 every 10 secs for 60 secs', action='store_true', dest='bool_tenten60', required=False, default=False)
parser.add_argument('-d','--deep', help='Deep converged multi data list', action='store_true', dest='bool_deep', required=False, default=False)
parser.add_argument('-n','--newsai', help='ML/NLP News sentiment AI for 1 stock', action='store', dest='newsymbol', required=False, default=False)
parser.add_argument('-p','--perf', help='Tech event performance sentiment', action='store_true', dest='bool_te', required=False, default=False)
parser.add_argument('-q','--quote', help='Get ticker price action quote', action='store', dest='qsymbol', required=False, default=False)
parser.add_argument('-s','--screen', help='Small cap screener logic', action='store_true', dest='bool_scr', required=False, default=False)
parser.add_argument('-t','--tops', help='show top ganers/losers', action='store_true', dest='bool_tops', required=False, default=False)
parser.add_argument('-u','--unusual', help='unusual up & down volume', action='store_true', dest='bool_uvol', required=False, default=False)
parser.add_argument('-v','--verbose', help='verbose error logging', action='store_true', dest='bool_verbose', required=False, default=False)
parser.add_argument('-x','--xray', help='dump detailed debug data structures', action='store_true', dest='bool_xray', required=False, default=False)

#  globals
yti = 1

# global accessors
symbol = None           # Unique company symbol
yfqnews_url = None      # SET by form_endpoint - the URL that is being worked on
js_session = None       # SET by this class during __init__ - main requests session
js_resp0 = None         # HTML session get() - response handle
js_resp2 = None         # JAVAScript session get() - response handle
yfn_all_data = None     # JSON dataset contains ALL data
yfn_htmldata = None     # Page in HTML
yfn_jsdata = None       # Page in JavaScript-HTML
yfn_jsdb = {}           # database to hold response handles from multiple js.session_get() ops
ml_brief = []           # ML TXT matrix for Naieve Bayes Classifier pre Count Vectorizer
ml_ingest = {}          # ML ingested NLP candidate articles
ml_sent = None
ul_tag_dataset = None   # BS4 handle of the <tr> extracted data
li_superclass = None    # all possible News articles
yti = 0                 # Unique instance identifier
cycle = 0               # class thread loop counter
nlp_x = 0
get_counter = 0         # count of get() requests
ext_req = ""            # HTMLSession request handle
sen_stats_df = None     # Aggregated sentiment stats for this 1 article
nsoup = None            # BS4 shared handle between UP & DOWN (1 URL, 2 embeded data sets in HTML doc)
args = []               # class dict to hold global args being passed in from main() methods
yfn_uh = None           # global url hinter class
url_netloc = None
a_urlp = None
article_url = "https://www.defaul_instance_url.com"
this_article_url = "https://www.default_interpret_page_url.com"
dummy_url = "https://finance.yahoo.com/screener/predefined/day_losers"

yahoo_headers = { \
                    'authority': 'finance.yahoo.com', \
                    'path': '/screener/predefined/day_gainers/', \
                    'referer': 'https://finance.yahoo.com/screener/', \
                    'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"', \
                    'sec-ch-ua-mobile': '"?0"', \
                    'sec-fetch-mode': 'navigate', \
                    'sec-fetch-user': '"?1', \
                    'sec-fetch-site': 'same-origin', \
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36' }

#######################################################################
############################# main() ##################################
#######################################################################

def main():
    cmi_debug = "aop::"+__name__+"::main()"
    global args
    args = vars(parser.parse_args())        # args as a dict []
    print ( " " )
    print ( "########## Initalizing ##########" )
    print ( " " )
    print ( "CMDLine args:", parser.parse_args() )
    if args['bool_verbose'] is True:        # Logging level
        print ( "Enabeling verbose info logging..." )
        logging.disable(0)                  # Log level = OFF
    else:
        logging.disable(20)                 # Log lvel = INFO

    print ( " " )

###################################### 1 ###########################################
# method 6
    def init_dummy_session(self):
        self.dummy_resp0 = requests.get(self.dummy_url, stream=True, headers=self.yahoo_headers, cookies=self.yahoo_headers, timeout=5 )
        #hot_cookies = requests.utils.dict_from_cookiejar(self.dummy_resp0.cookies)
        return

###################################### 2 ###########################################

    def init_live_session(self, id_url):
        '''
        A key objetcive acheived here is populating the existing yahoo_headers with live cookies
        from the live session. This is done by the requests.get() method
        But, we dont need the response object, so we dont store it
        Thats allready been captured at stored in: self.ext_req object
        '''
        self.live_resp0 = requests.get(id_url, stream=True, headers=self.yahoo_headers, cookies=self.yahoo_headers, timeout=5 )
        return

##################################### 3 ############################################

    def update_headers(self, ch):

        # HACK to help logging() f-string bug to handle strings with %
        # ch = url path (exluding the "https://"")
        cmi_debug = __name__+"::"+self.update_headers.__name__+".#"+str(self.yti)+"  - "+ch
        logging.info('%s' % cmi_debug )
        cmi_debug = __name__+"::"+self.update_headers.__name__+".#"+str(self.yti)
        self.path = ch
        self.cookies.update({'path': self.path} )
 
        if self.args['bool_xray'] is True:
            print ( f"=========================== {self.yti} / session cookies ===========================" )
            for i in self.cookies.items():
                print ( f"{i}" )

        return
            
###################################### 4 ###########################################

    def form_endpoint(self, symbol):
        """
        This is the explicit NEWS URL that is used for the request get()
        NOTE: assumes that path header/cookie has been set first
        #
        URL endpoints available (examples)
        All related news        - https://finance.yahoo.com/quote/IBM/?p=IBM
        Symbol specific news    - https://finance.yahoo.com/quote/IBM/news?p=IBM
        Symbol press releases   - https://finance.yahoo.com/quote/IBM/press-releases?p=IBM
        Symbol research reports - https://finance.yahoo.com/quote/IBM/reports?p=IBM
        """

        cmi_debug = __name__+"::"+self.form_endpoint.__name__+".#"+str(self.yti)
        logging.info( f"%s  - form URL endpoint for: {symbol}" % cmi_debug )
        self.yfqnews_url = 'https://finance.yahoo.com/'    # use global accessor (so all paths are consistent)
        logging.info( f"%s  - API endpoint URL: {self.yfqnews_url}" % cmi_debug )
        # NOTE | WARN: Class global attribute. Used in MANY places once its self set, so be careful
        return

##################################### 5 ############################################

    def share_hinter(self, hinst):
        cmi_debug = __name__+"::"+self.share_hinter.__name__+".#"+str(self.yti)
        logging.info( f'%s - IN {type(hinst)}' % cmi_debug )
        self.yfn_uh = hinst
        return

###################################### 6 ###########################################

    def update_cookies(self):
        # assumes that the requests session has already been established
        cmi_debug = __name__+"::"+self.update_cookies.__name__+".#"+str(self.yti)
        logging.info('%s - REDO the cookie extract & update  ' % cmi_debug )
        self.js_session.cookies.update({'A1': self.js_resp0.cookies['A1']} )    # yahoo cookie hack
        return

###################################### 7 ###########################################

    def ext_do_js_get(self, idx_x):
        cmi_debug = __name__+"::"+self.ext_do_js_get.__name__+".#"+str(self.yti)+"."+str(idx_x)
        
        # URL validation
        if not self.yfqnews_url or not isinstance(self.yfqnews_url, str):
            logging.error(f'{cmi_debug} - Invalid URL: {self.yfqnews_url}')
            return None
            
        logging.info( f'ml_yahoofinews::ext_do_js_get.#{self.yti}.{idx_x} - %s', self.yfqnews_url )
        r = self.ext_req
        r.html.render(timeout=10)
        
        logging.info( f'%s - JS rendered for Idx: [ {idx_x} ]' % cmi_debug )
        self.yfn_jsdata = r.html.text           # store Full JAVAScript response TEXT page
        self.yfn_htmldata = r.html.text
        auh = hashlib.sha256(self.yfqnews_url.encode())     # hash the url
        aurl_hash = auh.hexdigest()
        self.yfn_jsdb[aurl_hash] = r            # create CACHE entry in jsdb, response, not full page TEXT data !!
        logging.info( f'%s - CREATED cache entry: [ {aurl_hash} ]' % cmi_debug )

        #print (f"r.html.html: {escape(r.html.html)}...")  # Print first 100 characters of the HTML content for debugging 
        return aurl_hash

###################################### 7.1 ###########################################

    def ext_pw_js_get(self, idx_x):
        cmi_debug = __name__+"::"+self.ext_pw_js_get.__name__+".#"+str(self.yti)+"."+str(idx_x)
        
        # URL validation
        if not self.yfqnews_url or not isinstance(self.yfqnews_url, str):
            logging.error(f'{cmi_debug} - Invalid URL: {self.yfqnews_url}')
            return None
            
        logging.info( f'ml_yahoofinews::ext_pw_js_get.#{self.yti}.{idx_x} - %s', self.yfqnews_url )
        
        with sync_playwright() as p:
            browser = p.chromium.launch()
            page = browser.new_page()
            
            # Set headers similar to requests_html
            page.set_extra_http_headers(self.yahoo_headers)
            
            # Navigate to the URL with timeout
            try:
                page.goto(self.yfqnews_url, timeout=5000)
                page.wait_for_load_state('networkidle', timeout=5000)
            except Exception as e:
                logging.error(f'{cmi_debug} - Failed to load page: {e}')
                browser.close()
                return None
            
            # Get page content
            content = page.content()
            text_content = page.evaluate('() => document.body.innerText')
            
            browser.close()
        
        logging.info( f'%s - PW rendered JS for Idx: [ {idx_x} ]' % cmi_debug )
        self.yfn_jsdata = text_content           # store Full JavaScript response TEXT page
        self.yfn_htmldata = content
        auh = hashlib.sha256(self.yfqnews_url.encode())     # hash the url
        aurl_hash = auh.hexdigest()
        
        # Create a mock response object to maintain compatibility with existing code
        class MockResponse:
            def __init__(self, content, text, url):
                self.html = MockHtml(content, text)
                self.text = text
                self.url = url
        
        class MockHtml:
            def __init__(self, content, text):
                self.html = content
                self.text = text
        
        mock_response = MockResponse(content, text_content, self.yfqnews_url)
        self.yfn_jsdb[aurl_hash] = mock_response            # create CACHE entry in jsdb, response, not full page TEXT data !!
        logging.info( f'%s - CREATED cache entry: [ {aurl_hash} ]' % cmi_debug )

        return aurl_hash

###################################### 8 ###########################################

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
        if self.args['bool_xray'] is True:
            print ( f"========================== {self.yti} / HTML get() session cookies ================================" )
            logging.info( f'%s  - resp0 type: {type(self.js_resp0)}' % cmi_debug )
            for i in self.js_resp0.cookies.items():
                print ( f"{i}" )

        return aurl_hash

########### 3 10x10x60 ################
    # do 10x10x60 build-out cycle
    # currently fails to produce a unique data set each threat cycle. Don't know why
    if args['bool_tenten60'] is True:
        print ( "============================== Testing Craw4ai YAHOO News REader  =================================" )
        print ( " " )
        ############## hacking on general news
        #genews_reader = y_cookiemonster(3)
        genews_dataset = y_generalnews(3)
        genews_dataset.init_dummy_session()
        update_headers("/news")
        genews_dataset.ext_req = genews_dataset.do_simple_get()
        #genews_dataset.ext_req = genews_reader.get_js_data('finance.yahoo.com/')
        genews_dataset.ext_get_data(3)
        gx = genews_dataset.build_df0()

    else:
        pass

########################################################################
# Techncial ANalysis
# Get the Bull/Bear Technical performance Sentiment for all stocks in combo DF ######################
    """
    Bullish/Neutral/Bearish Technical indicators for each symbol
    Yahoo.com data is inconsistent and randomly unreliable (for Bull/Bear/Neutral state).
    Yahoo wants you to PAY for this info, so they make it difficult to extract.
    """
    if args['bool_te'] is True:
        cmi_debug = __name__+"::Tech_events_all.#1"
        te = y_techevents(1)

        ssot_te = combo_logic(1, mlx_top_dataset, small_cap_dataset, un_vol_activity, args )
        ssot_te.polish_combo_df(1)
        ssot_te.tag_dupes()
        ssot_te.tag_uniques()
        #x.rank_hot()
        #x.rank_unvol()
        #x.rank_caps()
        ssot_te.combo_df.sort_values(by=['Symbol'])         # sort by sumbol name (so dupes are linearly grouped)
        ssot_te.reindex_combo_df()                          # re-order a new index (PERMENANT write)

        print ( f"DEBUG: dump combo_df - {ssot_te}" )
        te.build_te_summary(ssot_te, 1)                     # x = main INSTANCE:: combo_logic
        #
        # TODO: populate build_te_summary with symbol co_name, Cur_price  Prc_change  Pct_change, volume
        # would be good to check if this symbol is also in the UNUSUAL UP table also.
        #     If it is, then add Vol_pct to table also
        #     Also add Index # from main Full Combo table  (make visual lookup quicker/easier)
        #  te_uniques = x.list_uniques()
        print ( f"\n\n" )
        print ( f"========== Hottest stocks Bullish status =============" )
        print ( f"{te.te_df0[['Symbol', 'Today', 'Short', 'Mid', 'Long', 'Bullcount', 'Senti']].sort_values(by=['Bullcount', 'Senti'], ascending=False)}" )
        print ( f"------------------------------------------------------" )
        #
        # HACKING : show uniques from COMBO def
        print ( f"***** Hacking ***** " )
        # might not be necessary now, since I've changed the logic surrounding COMBO DF dupes.
        # c_uniques = x.unique_symbols()
        c_uniques = ssot_te.combo_listall_nodupes()
        te.te_df0.merge(c_uniques, left_on='Symbol', right_on='Symbol')
        # x.combo_listall_nodupes
        print ( f"{te.te_df0}" )
    else:
        pass

#################################################################################
#                               QUOITES
#################################################################################
# 3 differnt methods to get a live quote
# NOTE: These 3 routines are *examples* of how to get quotes from the 3 live quote classes::
# TODO: Add a 4th method - via alpaca live API

    """
    EXAMPLE: #1
    nasdaq.com - live quotes via native JSON API test GET
    quote price data is 5 mins delayed
    10 data fields provided
    """

    if args['qsymbol'] is not False:
        nq = nquote(1, args)                          # Nasdqa quote instance from nasdqa_quotes.py
        nq.init_dummy_session()                       # note: this will set nasdaq magic cookie
        nq_symbol = args['qsymbol'].upper()
        logging.info( f"%s - Get Nasdaq.com quote for symbol {nq_symbol}" % cmi_debug )
        nq.update_headers(nq_symbol, "stocks")        # set path: header object. doesnt touch secret nasdaq cookies
        nq.form_api_endpoint(nq_symbol, "stocks")     # set API endpoint url - default GUESS asset_class=stocks
        ac = nq.learn_aclass(nq_symbol)

        if ac != "stocks":
            logging.info( f"%s - re-shape asset class endpoint to: {ac}" % cmi_debug )
            nq.form_api_endpoint(nq_symbol, ac)       # re-form API endpoint if default asset_class guess was wrong)
            nq.get_nquote(nq_symbol.upper())          # get a live quote
            wq = nq_wrangler(1, args)                 # instantiate a class for Quote Data Wrangeling
            wq.asset_class = ac
        else:
            nq.get_nquote(nq_symbol.rstrip())
            wq = nq_wrangler(1, args)                 # instantiate a class for Quote Data Wrangeling
            wq.asset_class = ac                       # wrangeler class MUST know the class of asset its working on

        logging.info( f"============ Getting nasdaq quote data for asset class: {ac} ==========" )
        wq.setup_zones(1, nq.quote_json1, nq.quote_json2, nq.quote_json3)
        wq.do_wrangle()
        wq.clean_cast()
        wq.build_data_sets()
        # add Tech Events Sentiment to quote dict{}
        te_nq_quote = wq.qd_quote
        """
        te = y_techevents(2)
        te.form_api_endpoints(nq_symbol)
        success = te.get_te_zones(2)
        if success == 0:
            te.build_te_data(2)
            te.te_into_nquote(te_nq_quote)
            #nq.quote.update({"today_only": te.te_sentiment[0][2]} )
            #nq.quote.update({"short_term": te.te_sentiment[1][2]} )
            #nq.quote.update({"med_term": te.te_sentiment[2][2]} )
            #nq.quote.update({"long_term": te.te_sentiment[3][2]} )
        else:
            te.te_is_bad()                     # FORCE Tech Events to be N/A
            te.te_into_nquote(te_nq_quote)     # NOTE: needs to be the point to new refactored class nasdqa_wrangler::nq_wrangler qd_quote{}
        """

        print ( f"===================== Nasdaq quote data =======================" )
        print ( f"                          {nq_symbol}" )
        print ( f"===============================================================" )
        c = 1
        for k, v in wq.qd_quote.items():
            print ( f"{c} - {k} : {v}" )
            c += 1
        """
        print ( f"===================== Technial Events =========================" )
        te.build_te_df(1)
        te.reset_te_df0()
        print ( f"{te.te_df0}" )
        print ( f"===============================================================" )
        """

    """
    EXAMPLE #2
    bigcharts.marketwatch.com - data via BS4 scraping
    quote price data is 15 mins delayed
    10 data fields provided
    """
    if args['qsymbol'] is not False:
        bc = bc_quote(5, args)                  # setup an emphemerial dict
        bc_symbol = args['qsymbol'].upper()     # what symbol are we getting a quote for?
        bc.get_basicquote(bc_symbol)            # get the quote
        print ( " " )
        print ( f"Get BIGCharts.com BasicQuote for: {bc_symbol}" )
        print ( f"================= basicquote data =======================" )
        c = 1
        for k, v in bc.quote.items():
            print ( f"{c} - {k} : {v}" )
            c += 1
        print ( f"========================================================" )
        print ( " " )

    """
    EXAMPLE #3
    bigcharts.marketwatch.com - data via BS4 scraping
    quote data is 15 mins delayed
    40 data fields provided
    """
    if args['qsymbol'] is not False:
        bc = bc_quote(5, args)                  # setup an emphemerial dict
        bc_symbol = args['qsymbol'].upper()     # what symbol are we getting a quote for?
        bc.get_quickquote(bc_symbol)            # get the quote
        bc.q_polish()                           # wrangel the data elements
        print ( " " )
        print ( f"Get BIGCharts.com QuickQuote for: {bc_symbol}" )
        print ( f"================= quickquote data =======================" )
        c = 1
        for k, v in bc.quote.items():
            print ( f"{c} - {k} : {v}" )
            c += 1
        print ( f"========================================================" )
        print ( " " )

#################################################################################
# ALPACA API Integration - Live quotes and bars ################################
#################################################################################

    """
    ALPACA API INTEGRATION
    Live quotes via Alpaca API - real-time data during market hours
    OHLCV bars data with 1-minute granularity
    """
    
    if args['alpaca_symbol'] is not False:
        alpaca_symbol = args['alpaca_symbol'].upper()
        print(f"========== Alpaca Live Quote for: {alpaca_symbol} ==========")
        
        try:
            alpaca = alpaca_md(1, args)
            market_open = alpaca.get_market_status()
            print(f"Market Status: {'Open' if market_open else 'Closed'}")
            
            # Get live quote
            quote = alpaca.get_live_quote(alpaca_symbol)
            if quote:
                print(f"Live Quote Data:")
                for k, v in quote.items():
                    print(f"  {k}: {v}")
            else:
                print(f"No quote data available for {alpaca_symbol}")
                
        except Exception as e:
            print(f"Error getting Alpaca quote: {e}")
            logging.error(f"Alpaca quote error for {alpaca_symbol}: {e}")
        
        print(" ")
        
    if args['alpaca_bars'] is not False:
        bars_symbol = args['alpaca_bars'].upper()
        print(f"========== Alpaca OHLCV Bars for: {bars_symbol} ==========")
        
        try:
            alpaca = alpaca_md(2, args)
            
            # Get bars data (last 20 minutes of 1-minute bars)
            bars_df = alpaca.get_bars(bars_symbol, timeframe="1Min", limit=20)
            if bars_df is not None and not bars_df.empty:
                print(f"Recent {len(bars_df)} minute bars:")
                pd.set_option('display.max_columns', None)
                pd.set_option('display.width', None)
                print(bars_df.to_string(index=False))
                
                # Calculate some basic stats
                if len(bars_df) > 1:
                    latest_close = bars_df.iloc[-1]['Close']
                    previous_close = bars_df.iloc[-2]['Close']
                    price_change = latest_close - previous_close
                    pct_change = (price_change / previous_close) * 100
                    
                    print(f"\nRecent Price Movement:")
                    print(f"  Latest Close: ${latest_close:.2f}")
                    print(f"  Previous Close: ${previous_close:.2f}")
                    print(f"  Change: ${price_change:.2f} ({pct_change:.2f}%)")
                    print(f"  Volume (latest bar): {bars_df.iloc[-1]['Volume']:,}")
            else:
                print(f"No bars data available for {bars_symbol}")
                
        except Exception as e:
            print(f"Error getting Alpaca bars: {e}")
            logging.error(f"Alpaca bars error for {bars_symbol}: {e}")
        
        print(" ")

#################################################################################
# NEW DATA SOURCES - SEC, FRED, Polygon.io Integration ########################
#################################################################################

    # SEC EDGAR filings integration
    if args['sec_symbol'] is not False:
        sec_symbol = args['sec_symbol'].upper()
        print(f"========== SEC EDGAR Filings for: {sec_symbol} ==========")
        
        try:
            sec = sec_md(1, args)
            
            # Find company CIK by ticker
            company_info = sec.search_company_by_ticker(sec_symbol)
            if company_info:
                print(f"Company: {company_info['title']}")
                print(f"CIK: {company_info['cik']}")
                
                # Get recent 10-K filings
                filings_10k = sec.get_company_filings(company_info['cik'], '10-K', limit=5)
                if not filings_10k.empty:
                    print(f"\nRecent 10-K Filings:")
                    for idx, filing in filings_10k.iterrows():
                        print(f"  {filing['filingDate']}: {filing['accessionNumber']}")
                
                # Get recent 10-Q filings  
                filings_10q = sec.get_company_filings(company_info['cik'], '10-Q', limit=5)
                if not filings_10q.empty:
                    print(f"\nRecent 10-Q Filings:")
                    for idx, filing in filings_10q.iterrows():
                        print(f"  {filing['filingDate']}: {filing['accessionNumber']}")
                        
            else:
                print(f"Company not found for ticker: {sec_symbol}")
                
        except Exception as e:
            print(f"Error fetching SEC data: {e}")
            logging.error(f"SEC data error for {sec_symbol}: {e}")
        
        print(" ")

    # FRED economic data integration
    if args['bool_fred'] is True:
        print("========== FRED Economic Data Snapshot ==========")
        
        try:
            fred = fred_md(1, args)
            
            # Get economic snapshot
            snapshot = fred.get_economic_snapshot()
            if snapshot:
                print("Key Economic Indicators:")
                for indicator, data in snapshot.items():
                    print(f"  {indicator.replace('_', ' ').title()}: {data['value']} ({data['date']})")
            
            # Get yield curve
            yield_curve = fred.get_yield_curve()
            if yield_curve:
                print(f"\nTreasury Yield Curve:")
                for maturity, rate in yield_curve.items():
                    print(f"  {maturity.replace('_', ' ')}: {rate}%")
                    
        except Exception as e:
            print(f"Error fetching FRED data: {e}")
            logging.error(f"FRED data error: {e}")
        
        print(" ")

    # Polygon.io integration
    if args['polygon_symbol'] is not False:
        polygon_symbol = args['polygon_symbol'].upper()
        print(f"========== Polygon.io Data for: {polygon_symbol} ==========")
        
        try:
            polygon = polygon_md(1, args)
            
            # Get market status
            market_status = polygon.get_market_status()
            if market_status:
                print(f"Market Status: {market_status.get('market', 'Unknown')}")
            
            # Get last quote
            quote = polygon.get_last_quote(polygon_symbol)
            if quote:
                print(f"Last Quote:")
                print(f"  Bid: ${quote.get('bid', 'N/A')} x {quote.get('bid_size', 'N/A')}")
                print(f"  Ask: ${quote.get('ask', 'N/A')} x {quote.get('ask_size', 'N/A')}")
                if quote.get('spread'):
                    print(f"  Spread: ${quote['spread']:.4f}")
            
            # Get ticker details
            details = polygon.get_ticker_details(polygon_symbol)
            if details:
                print(f"\nCompany Details:")
                print(f"  Name: {details.get('name', 'N/A')}")
                print(f"  Market: {details.get('market', 'N/A')}")
                print(f"  Exchange: {details.get('primary_exchange', 'N/A')}")
                if details.get('market_cap'):
                    print(f"  Market Cap: ${details['market_cap']:,}")
            
            # Get recent daily bars
            bars = polygon.get_aggregates(polygon_symbol, timespan='day', limit=5)
            if not bars.empty:
                print(f"\nRecent Daily Bars:")
                for idx, bar in bars.iterrows():
                    print(f"  {bar['timestamp'].strftime('%Y-%m-%d')}: O:{bar['open']:.2f} H:{bar['high']:.2f} L:{bar['low']:.2f} C:{bar['close']:.2f} V:{bar['volume']:,}")
                    
        except Exception as e:
            print(f"Error fetching Polygon data: {e}")
            logging.error(f"Polygon data error for {polygon_symbol}: {e}")
        
        print(" ")

    # Tiingo comprehensive data integration
    if args['tiingo_symbol'] is not False:
        tiingo_symbol = args['tiingo_symbol'].upper()
        print(f"========== Tiingo Comprehensive Data for: {tiingo_symbol} ==========")
        
        try:
            tiingo = tiingo_md(1, args)
            
            # Get ticker metadata
            metadata = tiingo.get_ticker_metadata(tiingo_symbol)
            if metadata:
                print(f"Company: {metadata.get('name', 'N/A')}")
                print(f"Description: {metadata.get('description', 'N/A')}")
                print(f"Exchange: {metadata.get('exchange_code', 'N/A')}")
                print(f"Data Range: {metadata.get('start_date', 'N/A')} to {metadata.get('end_date', 'N/A')}")
            
            # Get latest price
            latest_prices = tiingo.get_latest_prices(tiingo_symbol)
            if not latest_prices.empty:
                latest = latest_prices.iloc[0]
                print(f"\nLatest Price Data ({latest['date'].strftime('%Y-%m-%d')}):")
                print(f"  Open: ${latest['open']:.2f}")
                print(f"  High: ${latest['high']:.2f}")
                print(f"  Low: ${latest['low']:.2f}")
                print(f"  Close: ${latest['close']:.2f}")
                print(f"  Adj Close: ${latest['adjClose']:.2f}")
                print(f"  Volume: {latest['volume']:,}")
            
            # Get recent daily prices (last 10 days)
            daily_prices = tiingo.get_daily_prices(tiingo_symbol)
            if not daily_prices.empty:
                print(f"\nRecent Daily Prices (Last 5 days):")
                for idx, day in daily_prices.tail(5).iterrows():
                    price_change = day['close'] - day['open']
                    pct_change = (price_change / day['open']) * 100 if day['open'] != 0 else 0
                    print(f"  {day['date'].strftime('%Y-%m-%d')}: ${day['close']:.2f} ({price_change:+.2f}, {pct_change:+.2f}%)")
            
            # Get fundamental data if available
            try:
                fundamentals = tiingo.get_fundamentals_daily(tiingo_symbol)
                if not fundamentals.empty:
                    print(f"\nLatest Fundamental Data:")
                    fund_data = fundamentals.iloc[0]
                    # Display key fundamental metrics if available
                    key_metrics = ['marketCap', 'enterpriseVal', 'peRatio', 'pbRatio', 'trailingPEG1Y']
                    for metric in key_metrics:
                        if metric in fund_data and pd.notna(fund_data[metric]):
                            print(f"  {metric}: {fund_data[metric]}")
            except Exception as fund_e:
                print(f"\nFundamental data not available: {fund_e}")
                
        except Exception as e:
            print(f"Error fetching Tiingo data: {e}")
            logging.error(f"Tiingo data error for {tiingo_symbol}: {e}")
        
        print(" ")

    # Tiingo financial news integration
    if args['bool_tiingo_news'] is True:
        print("========== Tiingo Financial News ==========")
        
        try:
            tiingo = tiingo_md(2, args)
            
            # Get recent financial news
            news = tiingo.get_news(limit=10)
            if not news.empty:
                print("Recent Financial News:")
                for idx, article in news.iterrows():
                    published_date = article['publishedDate'].strftime('%Y-%m-%d %H:%M')
                    title = article.get('title', 'N/A')
                    source = article.get('source', 'N/A')
                    tickers = ', '.join(article.get('tickers', [])) if article.get('tickers') else 'General'
                    
                    print(f"\n  [{published_date}] {source}")
                    print(f"  {title}")
                    print(f"  Tickers: {tickers}")
                    
                    # Show tags if available
                    if article.get('tags'):
                        tags = ', '.join(article['tags'][:3])  # Show first 3 tags
                        print(f"  Tags: {tags}")
                    
                    print("  " + "-" * 80)
            else:
                print("No recent news available")
                
        except Exception as e:
            print(f"Error fetching Tiingo news: {e}")
            logging.error(f"Tiingo news error: {e}")
        
        print(" ")

#################################################################################
# Alpha Vantage Integration ####################################################
#################################################################################

    # Alpha Vantage quote and basic data
    if args['alphavantage_symbol'] is not False:
        alphavantage_symbol = args['alphavantage_symbol'].upper()
        print(f"========== Alpha Vantage Data for: {alphavantage_symbol} ==========")
        
        try:
            av = alphavantage_md(1, args)
            
            # Get global quote
            quote = av.get_global_quote(alphavantage_symbol)
            if quote:
                print(f"Global Quote:")
                print(f"  Symbol: {quote.get('symbol')}")
                print(f"  Price: ${quote.get('price', 0):.2f}")
                print(f"  Change: ${quote.get('change', 0):.2f} ({quote.get('change_percent', '0')}%)")
                print(f"  Open: ${quote.get('open', 0):.2f}")
                print(f"  High: ${quote.get('high', 0):.2f}")
                print(f"  Low: ${quote.get('low', 0):.2f}")
                print(f"  Previous Close: ${quote.get('previous_close', 0):.2f}")
                print(f"  Volume: {quote.get('volume', 0):,}")
                print(f"  Latest Trading Day: {quote.get('latest_trading_day', 'N/A')}")
            else:
                print(f"No quote data available for {alphavantage_symbol}")
                
        except Exception as e:
            print(f"Error getting Alpha Vantage data: {e}")
            logging.error(f"Alpha Vantage data error for {alphavantage_symbol}: {e}")
        
        print(" ")

    # Alpha Vantage company overview
    if args['alphavantage_overview'] is not False:
        overview_symbol = args['alphavantage_overview'].upper()
        print(f"========== Alpha Vantage Company Overview for: {overview_symbol} ==========")
        
        try:
            av = alphavantage_md(2, args)
            
            # Get company overview
            overview = av.get_company_overview(overview_symbol)
            if overview:
                print(f"Company Information:")
                print(f"  Name: {overview.get('name', 'N/A')}")
                print(f"  Symbol: {overview.get('symbol', 'N/A')}")
                print(f"  Exchange: {overview.get('exchange', 'N/A')}")
                print(f"  Currency: {overview.get('currency', 'N/A')}")
                print(f"  Country: {overview.get('country', 'N/A')}")
                print(f"  Sector: {overview.get('sector', 'N/A')}")
                print(f"  Industry: {overview.get('industry', 'N/A')}")
                
                print(f"\nValuation Metrics:")
                print(f"  Market Cap: {overview.get('market_cap', 'N/A')}")
                print(f"  P/E Ratio: {overview.get('pe_ratio', 'N/A')}")
                print(f"  PEG Ratio: {overview.get('peg_ratio', 'N/A')}")
                print(f"  Book Value: {overview.get('book_value', 'N/A')}")
                print(f"  EPS: {overview.get('eps', 'N/A')}")
                print(f"  Beta: {overview.get('beta', 'N/A')}")
                print(f"  52-Week High: {overview.get('52_week_high', 'N/A')}")
                print(f"  52-Week Low: {overview.get('52_week_low', 'N/A')}")
                
                print(f"\nFinancial Metrics:")
                print(f"  Revenue TTM: {overview.get('revenue_ttm', 'N/A')}")
                print(f"  Profit Margin: {overview.get('profit_margin', 'N/A')}")
                print(f"  Operating Margin TTM: {overview.get('operating_margin_ttm', 'N/A')}")
                print(f"  Return on Assets TTM: {overview.get('return_on_assets_ttm', 'N/A')}")
                print(f"  Return on Equity TTM: {overview.get('return_on_equity_ttm', 'N/A')}")
                
                if overview.get('description'):
                    print(f"\nDescription: {overview.get('description')[:200]}...")
                    
            else:
                print(f"No company overview available for {overview_symbol}")
                
        except Exception as e:
            print(f"Error getting Alpha Vantage company overview: {e}")
            logging.error(f"Alpha Vantage overview error for {overview_symbol}: {e}")
        
        print(" ")

    # Alpha Vantage intraday data
    if args['alphavantage_intraday'] is not False:
        intraday_symbol = args['alphavantage_intraday'].upper()
        print(f"========== Alpha Vantage Intraday Data for: {intraday_symbol} ==========")
        
        try:
            av = alphavantage_md(3, args)
            
            # Get 5-minute intraday data
            intraday_df = av.get_intraday_data(intraday_symbol, interval='5min', outputsize='compact')
            if not intraday_df.empty:
                print(f"Recent 5-minute intraday data (last 10 intervals):")
                recent_data = intraday_df.tail(10)
                for idx, bar in recent_data.iterrows():
                    print(f"  {bar['timestamp'].strftime('%Y-%m-%d %H:%M')}: O:{bar['open']:.2f} H:{bar['high']:.2f} L:{bar['low']:.2f} C:{bar['close']:.2f} V:{bar['volume']:,}")
                
                # Calculate some basic stats
                if len(intraday_df) > 1:
                    latest = intraday_df.iloc[-1]
                    previous = intraday_df.iloc[-2]
                    price_change = latest['close'] - previous['close']
                    pct_change = (price_change / previous['close']) * 100
                    
                    print(f"\nRecent Price Movement:")
                    print(f"  Latest Close: ${latest['close']:.2f}")
                    print(f"  Previous Close: ${previous['close']:.2f}")
                    print(f"  Change: ${price_change:.2f} ({pct_change:.2f}%)")
                    print(f"  Volume (latest): {latest['volume']:,}")
                    
            else:
                print(f"No intraday data available for {intraday_symbol}")
                
        except Exception as e:
            print(f"Error getting Alpha Vantage intraday data: {e}")
            logging.error(f"Alpha Vantage intraday error for {intraday_symbol}: {e}")
        
        print(" ")

    # Alpha Vantage top gainers/losers
    if args['bool_alphavantage_gainers'] is True:
        print("========== Alpha Vantage Top Gainers/Losers ==========")
        
        try:
            av = alphavantage_md(4, args)
            
            # Get top gainers and losers
            gainers_losers = av.get_top_gainers_losers()
            if gainers_losers:
                metadata = gainers_losers.get('metadata', {})
                print(f"Market data as of: {metadata.get('last_updated', 'N/A')}")
                
                # Top gainers
                top_gainers = gainers_losers.get('top_gainers')
                if not top_gainers.empty:
                    print(f"\nTop Gainers:")
                    for idx, stock in top_gainers.head(10).iterrows():
                        print(f"  {stock.get('ticker', 'N/A')}: ${float(stock.get('price', 0)):.2f} ({stock.get('change_percentage', 'N/A')})")
                
                # Top losers
                top_losers = gainers_losers.get('top_losers')
                if not top_losers.empty:
                    print(f"\nTop Losers:")
                    for idx, stock in top_losers.head(10).iterrows():
                        print(f"  {stock.get('ticker', 'N/A')}: ${float(stock.get('price', 0)):.2f} ({stock.get('change_percentage', 'N/A')})")
                
                # Most actively traded
                most_active = gainers_losers.get('most_actively_traded')
                if not most_active.empty:
                    print(f"\nMost Actively Traded:")
                    for idx, stock in most_active.head(10).iterrows():
                        print(f"  {stock.get('ticker', 'N/A')}: ${float(stock.get('price', 0)):.2f} (Vol: {int(float(stock.get('volume', 0))):,})")
                        
            else:
                print("No gainers/losers data available")
                
        except Exception as e:
            print(f"Error getting Alpha Vantage gainers/losers: {e}")
            logging.error(f"Alpha Vantage gainers/losers error: {e}")
        
        print(" ")

#################################################################################
# NEW MARKET DATA EXTRACTORS - Finnhub, Marketstack, StockData, etc. ##########
#################################################################################

    # Finnhub API integration
    if args['finnhub_symbol'] is not False:
        finnhub_symbol = args['finnhub_symbol'].upper()
        print(f"========== Finnhub Data for: {finnhub_symbol} ==========")
        
        try:
            finnhub = finnhub_md(1, args)
            
            # Get quote
            quote = finnhub.get_quote(finnhub_symbol)
            if quote:
                print(f"Real-time Quote:")
                print(f"  Current Price: ${quote.get('c', 0):.2f}")
                print(f"  Change: ${quote.get('d', 0):.2f} ({quote.get('dp', 0):.2f}%)")
                print(f"  High: ${quote.get('h', 0):.2f}")
                print(f"  Low: ${quote.get('l', 0):.2f}")
                print(f"  Open: ${quote.get('o', 0):.2f}")
                print(f"  Previous Close: ${quote.get('pc', 0):.2f}")
            
            # Get company profile
            profile = finnhub.get_company_profile(finnhub_symbol)
            if profile:
                print(f"\nCompany Profile:")
                print(f"  Name: {profile.get('name', 'N/A')}")
                print(f"  Country: {profile.get('country', 'N/A')}")
                print(f"  Currency: {profile.get('currency', 'N/A')}")
                print(f"  Exchange: {profile.get('exchange', 'N/A')}")
                print(f"  Industry: {profile.get('finnhubIndustry', 'N/A')}")
                print(f"  Market Cap: {profile.get('marketCapitalization', 'N/A')}")
                
        except Exception as e:
            print(f"Error fetching Finnhub data: {e}")
            logging.error(f"Finnhub data error for {finnhub_symbol}: {e}")
        
        print(" ")

    # Finnhub news integration
    if args['finnhub_news_symbol'] is not False:
        news_symbol = args['finnhub_news_symbol'].upper()
        print(f"========== Finnhub News for: {news_symbol} ==========")
        
        try:
            finnhub = finnhub_md(2, args)
            
            # Get company news
            news_df = finnhub.get_company_news(news_symbol)
            if not news_df.empty:
                print("Recent Company News:")
                for idx, article in news_df.head(5).iterrows():
                    print(f"\n  [{article['datetime'].strftime('%Y-%m-%d %H:%M')}]")
                    print(f"  {article['headline']}")
                    print(f"  Source: {article['source']}")
                    if article.get('summary'):
                        summary = article['summary'][:100] + "..." if len(article['summary']) > 100 else article['summary']
                        print(f"  Summary: {summary}")
            else:
                print(f"No recent news available for {news_symbol}")
                
        except Exception as e:
            print(f"Error fetching Finnhub news: {e}")
            logging.error(f"Finnhub news error for {news_symbol}: {e}")
        
        print(" ")

    # Marketstack API integration
    if args['marketstack_symbol'] is not False:
        marketstack_symbol = args['marketstack_symbol'].upper()
        print(f"========== Marketstack Data for: {marketstack_symbol} ==========")
        
        try:
            marketstack = marketstack_md(1, args)
            
            # Get latest EOD data
            latest_eod = marketstack.get_eod_latest([marketstack_symbol])
            if not latest_eod.empty:
                data = latest_eod.iloc[0]
                print(f"Latest EOD Data ({data['date'].strftime('%Y-%m-%d')}):")
                print(f"  Open: ${data['open']:.2f}")
                print(f"  High: ${data['high']:.2f}")
                print(f"  Low: ${data['low']:.2f}")
                print(f"  Close: ${data['close']:.2f}")
                print(f"  Volume: {data['volume']:,}")
                
            # Get recent historical data
            historical = marketstack.get_eod_historical(marketstack_symbol, limit=5)
            if not historical.empty:
                print(f"\nRecent Historical Data (Last 5 days):")
                for idx, day in historical.iterrows():
                    print(f"  {day['date'].strftime('%Y-%m-%d')}: ${day['close']:.2f} (Vol: {day['volume']:,})")
                
        except Exception as e:
            print(f"Error fetching Marketstack data: {e}")
            logging.error(f"Marketstack data error for {marketstack_symbol}: {e}")
        
        print(" ")

    # StockData.org API integration
    if args['stockdata_symbol'] is not False:
        stockdata_symbol = args['stockdata_symbol'].upper()
        print(f"========== StockData.org Data for: {stockdata_symbol} ==========")
        
        try:
            stockdata = stockdata_md(1, args)
            
            # Get quote
            quote = stockdata.get_quote(stockdata_symbol)
            if quote:
                print(f"Real-time Quote:")
                for key, value in quote.items():
                    if key in ['price', 'change', 'change_percent', 'open', 'high', 'low', 'previous_close']:
                        print(f"  {key.replace('_', ' ').title()}: {value}")
            
            # Get recent EOD data
            eod_data = stockdata.get_eod(stockdata_symbol, limit=5)
            if not eod_data.empty:
                print(f"\nRecent EOD Data (Last 5 days):")
                for idx, day in eod_data.iterrows():
                    print(f"  {day['date'].strftime('%Y-%m-%d')}: ${day['close']:.2f} (Vol: {day['volume']:,})")
                
        except Exception as e:
            print(f"Error fetching StockData.org data: {e}")
            logging.error(f"StockData.org data error for {stockdata_symbol}: {e}")
        
        print(" ")

    # Twelve Data API integration
    if args['twelvedata_symbol'] is not False:
        twelvedata_symbol = args['twelvedata_symbol'].upper()
        print(f"========== Twelve Data for: {twelvedata_symbol} ==========")
        
        try:
            twelvedata = twelvedata_md(1, args)
            
            # Get quote
            quote = twelvedata.get_quote(twelvedata_symbol)
            if quote:
                print(f"Real-time Quote:")
                print(f"  Symbol: {quote.get('symbol')}")
                print(f"  Price: ${float(quote.get('close', 0)):.2f}")
                print(f"  Change: {quote.get('change', 'N/A')}")
                print(f"  Percent Change: {quote.get('percent_change', 'N/A')}")
                print(f"  Open: ${float(quote.get('open', 0)):.2f}")
                print(f"  High: ${float(quote.get('high', 0)):.2f}")
                print(f"  Low: ${float(quote.get('low', 0)):.2f}")
                print(f"  Volume: {quote.get('volume', 'N/A')}")
            
            # Get time series data
            time_series = twelvedata.get_time_series(twelvedata_symbol, interval='1day', outputsize=5)
            if not time_series.empty:
                print(f"\nRecent Daily Data (Last 5 days):")
                for idx, day in time_series.iterrows():
                    print(f"  {day['datetime'].strftime('%Y-%m-%d')}: ${day['close']:.2f} (Vol: {day['volume']:,})")
                
        except Exception as e:
            print(f"Error fetching Twelve Data: {e}")
            logging.error(f"Twelve Data error for {twelvedata_symbol}: {e}")
        
        print(" ")

    # EOD Historical Data API integration
    if args['eodhistoricaldata_symbol'] is not False:
        eod_symbol = args['eodhistoricaldata_symbol'].upper()
        print(f"========== EOD Historical Data for: {eod_symbol} ==========")
        
        try:
            eod = eodhistoricaldata_md(1, args)
            
            # Get real-time data
            realtime = eod.get_realtime_data([eod_symbol])
            if not realtime.empty:
                data = realtime.iloc[0]
                print(f"Real-time Data:")
                print(f"  Symbol: {data.get('code', 'N/A')}")
                print(f"  Price: ${float(data.get('close', 0)):.2f}")
                print(f"  Change: {data.get('change_p', 'N/A')}")
                print(f"  Open: ${float(data.get('open', 0)):.2f}")
                print(f"  High: ${float(data.get('high', 0)):.2f}")
                print(f"  Low: ${float(data.get('low', 0)):.2f}")
            
            # Get recent EOD data
            eod_data = eod.get_eod_data(eod_symbol, 'US')
            if not eod_data.empty:
                print(f"\nRecent EOD Data (Last 5 days):")
                for idx, day in eod_data.tail(5).iterrows():
                    print(f"  {day['date'].strftime('%Y-%m-%d')}: ${day['close']:.2f} (Vol: {day['volume']:,})")
                
        except Exception as e:
            print(f"Error fetching EOD Historical Data: {e}")
            logging.error(f"EOD Historical Data error for {eod_symbol}: {e}")
        
        print(" ")

    # FinancialModelingPrep API integration
    if args['financialmodelingprep_symbol'] is not False:
        fmp_symbol = args['financialmodelingprep_symbol'].upper()
        print(f"========== FinancialModelingPrep Data for: {fmp_symbol} ==========")
        
        try:
            fmp = financialmodelingprep_md(1, args)
            
            # Get quote
            quote = fmp.get_quote([fmp_symbol])
            if not quote.empty:
                data = quote.iloc[0]
                print(f"Real-time Quote:")
                print(f"  Symbol: {data.get('symbol')}")
                print(f"  Price: ${float(data.get('price', 0)):.2f}")
                print(f"  Change: ${float(data.get('change', 0)):.2f} ({float(data.get('changesPercentage', 0)):.2f}%)")
                print(f"  Open: ${float(data.get('open', 0)):.2f}")
                print(f"  High: ${float(data.get('dayHigh', 0)):.2f}")
                print(f"  Low: ${float(data.get('dayLow', 0)):.2f}")
                print(f"  Volume: {int(float(data.get('volume', 0))):,}")
            
            # Get company profile
            profile = fmp.get_company_profile(fmp_symbol)
            if profile:
                print(f"\nCompany Profile:")
                print(f"  Name: {profile.get('companyName', 'N/A')}")
                print(f"  Industry: {profile.get('industry', 'N/A')}")
                print(f"  Sector: {profile.get('sector', 'N/A')}")
                print(f"  Market Cap: {profile.get('mktCap', 'N/A')}")
                print(f"  Beta: {profile.get('beta', 'N/A')}")
                
        except Exception as e:
            print(f"Error fetching FinancialModelingPrep data: {e}")
            logging.error(f"FinancialModelingPrep data error for {fmp_symbol}: {e}")
        
        print(" ")

    # Stooq data integration
    if args['stooq_symbol'] is not False:
        stooq_symbol = args['stooq_symbol'].upper()
        print(f"========== Stooq Data for: {stooq_symbol} ==========")
        
        try:
            stooq = stooq_md(1, args)
            
            # Get current quote
            quote = stooq.get_current_quote(stooq_symbol)
            if not quote.empty:
                data = quote.iloc[0]
                print(f"Current Quote:")
                print(f"  Symbol: {data.get('Symbol', 'N/A')}")
                print(f"  Close: ${float(data.get('Close', 0)):.2f}")
                print(f"  Open: ${float(data.get('Open', 0)):.2f}")
                print(f"  High: ${float(data.get('High', 0)):.2f}")
                print(f"  Low: ${float(data.get('Low', 0)):.2f}")
                print(f"  Volume: {int(float(data.get('Volume', 0))):,}")
                print(f"  Date: {data.get('Date', 'N/A')}")
            
            # Get recent historical data
            historical = stooq.get_historical_data(stooq_symbol, days_back=30)
            if not historical.empty:
                print(f"\nRecent Historical Data (Last 5 days):")
                for idx, day in historical.tail(5).iterrows():
                    print(f"  {day['date'].strftime('%Y-%m-%d')}: ${day['close']:.2f} (Vol: {day['volume']:,})")
                
        except Exception as e:
            print(f"Error fetching Stooq data: {e}")
            logging.error(f"Stooq data error for {stooq_symbol}: {e}")
        
        print(" ")


if __name__ == '__main__':
    main()
