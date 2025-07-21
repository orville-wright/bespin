#! python3
import argparse
import asyncio
from bs4 import BeautifulSoup
from datetime import datetime, date
import hashlib
import json
import logging
import numpy as np
import os
import pandas as pd
from pathlib import Path
import requests
from requests_html import HTMLSession
from rich import print
from rich.markup import escape
import time
from typing import List
from urllib.parse import urlparse

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, CrawlResult
from crawl4ai import JsonCssExtractionStrategy

# logging setup
logging.basicConfig(level=logging.INFO)

#####################################################

class yfnews_reader:
    """
    Read Yahoo Finance news reader using crawl4ai, Word Vectorizer, Positive/Negative sentiment analyzer
    """

    # global accessors
    a_urlp = None
    args = []               # class dict to hold global args being passed in from main() methods
    article_url = "https://www.defaul_instance_url.com"
    articles_found = 0
    articles_crawled = {}
    cur_dir = None
    cycle = 0               # class thread loop counter
    dummy_resp0 = None
    ext_req = None          # HTMLSession request handle
    extracted_articles = None  # crawl4ai extracted articles
    get_counter = 0         # count of get() requests
    YF_sym_main_schema = None
    YF_sym_article_schema = None
    li_superclass = None    # all possible News articles
    live_resp0 = None
    ml_brief = []           # ML TXT matrix for Naive Bayes Classifier pre Count Vectorizer
    ml_ingest = {}          # ML ingested NLP candidate articles
    ml_sent = None
    nlp_x = 0
    sen_stats_df = None     # Aggregated sentiment stats for this 1 article
    symbol = None           # Unique company symbol
    this_article_url = "https://www.default_interpret_page_url.com"
    url_netloc = None
    yfn_uh = None           # global url hinter class
    yfn_all_data = None     # JSON dataset contains ALL data
    yfn_all_result = None   # JSON dataset contains ALL data
    yfqnews_url = None      # SET by form_endpoint - the URL that is being worked on
    yfn_crawl_data = None   # Crawl4ai extracted data
    yfn_c4_data = None      # Crawl4ai extracted data
    yfn_c4_result = {}      # Crawl4ai extracted data
    yfn_jsdb = {}           # database to hold response handles from multiple crawl operations
    yti = 0                 # Unique instance identifier
    
    yahoo_headers = {
        'authority': 'finance.yahoo.com',
        'path': '/screener/predefined/day_gainers/',
        'referer': 'https://finance.yahoo.com/screener/',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '"?0"',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '"?1',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36'
    }

    # ################
    def __init__(self, yti, symbol, global_args):
        self.yti = yti
        cmi_debug = __name__+"::" + self.__init__.__name__
        logging.info(f'%s - Instantiate.#{yti}' % cmi_debug)
        # init empty DataFrame with preset column names
        self.args = global_args
        self.symbol = symbol
        self.nlp_x = 0
        self.cycle = 1
        self.sent_df0 = pd.DataFrame(columns=['Row', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time'])
        
        # Setup crawl4ai schema path
        __cur_dir__ = Path(__file__).parent
        self.cur_dir = __cur_dir__
        self.YF_sym_main_schema = f"{self.cur_dir}/json/YF_sym_main_schema.json"
        self.YF_sym_article_schema = f"{self.cur_dir}/json/YF_sym_article_schema.json"
        return

    # ################ 1
    def init_dummy_session(self):
        self.dummy_resp0 = requests.get(self.dummy_url, stream=True, headers=self.yahoo_headers, cookies=self.yahoo_headers, timeout=5 )
        #hot_cookies = requests.utils.dict_from_cookiejar(self.dummy_resp0.cookies)
        return
    
    # ################ 2
    def init_live_session(self, id_url):
        '''
        A key objetcive acheived here is populating the existing yahoo_headers with live cookies
        from the live session. This is done by the requests.get() method
        But, we dont need the response object, so we dont store it
        Thats allready been captured at stored in: self.ext_req object
        '''
        cmi_debug = __name__+"::"+self.init_live_session.__name__+".#"+str(self.yti)
        logging.info(f"%s    - Do basic get()..." % cmi_debug )
        self.live_resp0 = requests.get(id_url, stream=True, headers=self.yahoo_headers, cookies=self.yahoo_headers, timeout=5 )
        logging.info(f"%s    - Saved get() resp {type(self.live_resp0)}" % cmi_debug )
        return self.live_resp0

    # ################ 3
    def update_headers(self, ch):

        # HACK to help logging() f-string bug to handle strings with %
        cmi_debug = __name__+"::"+self.update_headers.__name__+".#"+str(self.yti)+"       - Path: "+ch
        logging.info('%s' % cmi_debug )
        cmi_debug = __name__+"::"+self.update_headers.__name__+".#"+str(self.yti)
        self.path = ch
        self.ext_req.cookies.update({'path': self.path} )
        if self.args['bool_xray'] is True:
            print ( f"=========================== {self.yti} / session cookies ===========================" )
            for i in self.ext_req.cookies.items():
                print ( f"{i}" )

        return    
    # ################ 4
    def form_endpoint(self, symbol):
        """
        This is the explicit NEWS URL that is used for the crawl4ai request
        URL endpoints available (examples)
        All related news        - https://finance.yahoo.com/quote/IBM/?p=IBM
        Symbol specific news    - https://finance.yahoo.com/quote/IBM/news?p=IBM
        Symbol press releases   - https://finance.yahoo.com/quote/IBM/press-releases?p=IBM
        Symbol research reports - https://finance.yahoo.com/quote/IBM/reports?p=IBM
        """
        cmi_debug = __name__+"::" + self.form_endpoint.__name__+".#"+str(self.yti)
        logging.info(f"%s  - form URL endpoint for: {symbol}" % cmi_debug)
        self.yfqnews_url = 'https://finance.yahoo.com/quote/' + symbol + '/news/'
        logging.info(f"%s  - API endpoint URL: {self.yfqnews_url}" % cmi_debug)
        return

    # ################ 5
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
            cmi_debug = __name__+"::"+self.do_simple_get.__name__+".#"+str(self.yti)+"  - JS get(): "+url
            logging.info('%s' % cmi_debug )
            #logging.info( f"%s - JS_session.get() sucessful: {url}" % cmi_debug )
            cmi_debug = __name__+"::"+self.do_simple_get.__name__+".#"+str(self.yti)    # reset cmi_debug
            if self.js_resp0.status_code != 200:
                    logging.error(f'{cmi_debug} - get() failed with error: {self.js_resp0.status_code}')
                    return None

        logging.info(f'{cmi_debug} - get() success status: {self.js_resp0.status_code}')
        self.get_counter += 1
        logging.info( f"%s  - js.render()... diasbled" % cmi_debug )
        logging.info( f'%s  - Store basic HTML dataset' % cmi_debug )
        self.js_resp2 = self.js_resp0               # Set js_resp2 to the same response as js_resp0 for now
        hot_cookies = requests.utils.dict_from_cookiejar(self.js_resp0.cookies)
        logging.info( f"%s  - Swap {len(hot_cookies)} cookies into LOCAL yahoo_headers" % cmi_debug )

        self.yfn_htmldata = self.js_resp0.text      # store entire page HTML text in memory in this class
        auh = hashlib.sha256(url.encode())          # hash the url
        aurl_hash = auh.hexdigest()
        logging.info( f'%s  - CREATE cache entry: [ {aurl_hash} ]' % cmi_debug )
        self.yfn_jsdb[aurl_hash] = self.js_resp0    # create CACHE entry in jsdb with value: js_resp0 (not full page TEXT data)

        # Xray DEBUG
        if self.args['bool_xray'] is True:
            print ( f"========================== {self.yti} / HTML get() session cookies ================================" )
            logging.info( f'%s  - resp0 type: {type(self.js_resp0)}' % cmi_debug )
            for i in self.js_resp0.cookies.items():
                print ( f"{i}" )

        return aurl_hash

    # ################ 6
    def share_hinter(self, hinst):
        cmi_debug = __name__+"::" + self.share_hinter.__name__+".#"+str(self.yti)
        logging.info(f'%s - IN {type(hinst)}' % cmi_debug)
        self.yfn_uh = hinst
        return

    # ################ 7
    async def yahoofin_news_depth0(self, idx_x):
        """
        Depth -> 0
        crawl4ai extractor of Top-level crawler
        Use js_cmds to for next_page() to capture all 200+ artciles to end of page stream
        Store full craw4ai result in GLOBAL class accessor: self.yfn_jsdb
        INFO:
        Surface scan of Top-level news articles list in the news section for 1 stock symbol
        Hash_state: Unique hash of the URL that is being scanned
        Scans YF main News page of an explicit stock ticker. Skimming for all news articles
        Symbol: Stock symbol NEWS FEED for articles (e.g. https://finance.yahoo.com/quote/OTLY/news?p=OTLY )
        Scan_type: 0 = html | 1 = crawl4ai extraction (deprecated)
        Share class accessors of where the New Articles live
        """
        cmi_debug = __name__+"::" + self.yahoofin_news_depth0.__name__+".#"+str(self.yti)+"."+str(idx_x)
        if not self.yfqnews_url or not isinstance(self.yfqnews_url, str):       # set  @async_nlp_read_one by form_endpoint()
            logging.error(f'{cmi_debug} - Invalid URL: {self.yfqnews_url}')
            return None

        logging.info(f'{__name__}::yahoofin_news_depth0.#{self.yti}.{idx_x} - %s', self.yfqnews_url)
        logging.info(f'%s - Load C4 json schema file: [ {self.YF_sym_main_schema} ]' % cmi_debug)
        listall_schema_file_path = f"{self.YF_sym_main_schema}"        
        if os.path.exists(listall_schema_file_path):
            with open(listall_schema_file_path, "r") as f:
                schema = json.load(f)
        else:
            logging.error(f'%s - FAILED to load schema file: [ {self.YF_sym_main_schema} ]' % cmi_debug)
            return None

        logging.info(f'%s - INIT crawl4ai extraction strategy...' % cmi_debug)
        extraction_strategy = JsonCssExtractionStrategy(schema)
        js_cmds = [
            "window.scrollTo(0, document.body.scrollHeight);",
            "await new Promise(resolve => setTimeout(resolve, 2000));"
        ]
        
        config = CrawlerRunConfig(
            extraction_strategy=extraction_strategy,
            scan_full_page=True,
            js_code=js_cmds,
            cache_mode=CacheMode.BYPASS  # Bypass cache for fresh data
        )

        try:
            async with AsyncWebCrawler() as crawler:
                logging.info(f'%s - Exec async webcrawl NOW...' % cmi_debug)
                result = await crawler.arun(self.yfqnews_url, config=config)                
                if result.success:
                    logging.info(f'%s - crawl4ai extraction successful' % cmi_debug)
                    self.yfn_crawl_data = json.loads(result.extracted_content)
                    auh = hashlib.sha256(self.yfqnews_url.encode()) # prep hash
                    aurl_hash = auh.hexdigest()                     # genertae
                    self.yfn_jsdb[aurl_hash] = {
                        'url': self.yfqnews_url,
                        'data': self.yfn_crawl_data,
                        'result': result
                    }
                    
                    logging.info(f'%s - Create cache entry: [ {aurl_hash} ]' % cmi_debug)
                    return aurl_hash
                else:
                    logging.error(f'%s - crawl4ai extraction failed: {result.error}' % cmi_debug)
                    return None                    
        except Exception as e:
            logging.error(f'{cmi_debug} - Error during crawl4ai extraction: {e}')
            return None

    # ################
    def list_newsfeed_candidates(self, symbol, depth, scan_type, hash_state):
        """
        DEPTH -> 0
        Test and report the Depth 0 scan sucess
        - URL hash exists in cache
        - This means URL opened & crawled and data extracted
        - sets GLOBALLY sets Dataset accessor -> self.extracted_articles
        - calc and set helper GLOBAL attribute: articles_found counter 
        
        Does a  nice REPORT of the Depth 0 surface scan
        This does NOT get() or extartc and data, or create ml_ingest dataset
        """
        cmi_debug = __name__+"::" + self.list_newsfeed_candidates.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug)
        symbol = symbol.upper()
        depth = int(depth) 
        
        if scan_type == 1:  # crawl4ai extraction
            logging.info(f'%s - Check Depth 0 cache state: {hash_state}' % cmi_debug)
            try:
                cached_data = self.yfn_jsdb[hash_state]     # get resp - set by craw4al @ yahoofin_news_depth0()
                logging.info(f'%s - URL exists in cache...' % cmi_debug)
                
                # CRITICIAL:  gloablly sets the extratced >>dataset<< to work on for tis article
                self.extracted_articles = cached_data['data']                    
                if isinstance(self.extracted_articles, list):       # test for list
                    article_count = len(self.extracted_articles)    # Count articles found
                    logging.info(f'%s - Depth: 0 Surface skim / Found News Articles: {article_count}' % cmi_debug)
                    print(f"============================== Articles found: {article_count} ===================================")
                    for i, article in enumerate(self.extracted_articles):       # cycle trough articles >>dataset<<
                        if article.get('Title'):
                            safe_i = i + 1
                            print(f"Item: {i+1:03}: {article.get('Title', 'No title')[:60]}... / (Possible News article)")
                        else:
                            print(f"Item: {i+1:003}: Empty no article data")
                else:
                    logging.warning(f'%s - No articles found in extraction' % cmi_debug)
                    
            except KeyError:
                logging.error(f'%s - ERROR URL hash not in cache: {hash_state}' % cmi_debug)
                return None
        self.articles_found = article_count
        return self.articles_found

    # ################
    def eval_news_feed_stories(self, symbol):
        """
        Depth : 1
        Scanning news feed stories and some metadata (@ depth 1)
        Extract key data elements from each article via crawl4ai dataset indexing @ self.extracted_articles
        Build a ML ingest DB dataset of articles
        Dedupe hash's
        No network get() requests are made here
        """
        cmi_debug = __name__+"::" + self.eval_news_feed_stories.__name__+".#"+str(self.yti)
        logging.info('%s - IN ' % cmi_debug)
        time_now = time.strftime("%H:%M:%S", time.localtime())
        symbol = symbol.upper()
        if not self.extracted_articles:         # GLOBAL class accessor : article >>dataset<< extracted by crawl4ai
            logging.error(f'%s - No extracted articles available' % cmi_debug)
            return
        
        cg = 1
        hcycle = 1
        dedupe_set = set()
        logging.info(f'%s - Article Zone scanning / ml_ingest populating...' % cmi_debug)
        for article in self.extracted_articles: # GLOBAL class accessor : article >>dataset<< extracted by crawl4ai
            self.nlp_x += 1
            art_title = article.get('Title', 'ERROR_no_title')      # extract craw4al element
            article_url = article.get('Ext_url', '')                # extract craw4al element
            art_publisher = article.get('Publisher', 'ERROR_no_publisher • ERROR_no_pub_time')  # extract craw4al element
            art_teaser = article.get('Teaser', 'ERROR_no_teaser')   # extract craw4al element
            try:
                publisher = publisher.split('•')[0].strip()
                update_time = publisher.split('•')[1].strip()
            except Exception as e:
                logging.info(f'%s - Error @ {cg} extract pub info: {e}...' % cmi_debug)
                art_publisher = "ERROR_no_publisher"
                update_time = "ERROR_no_pub_time"
            
            print(f"Eval cycle:    Depth 1  ({cg} / {self.articles_found}) ============================================")
            if article_url:
                if article_url.startswith('http'):              # quick safety check that we have a real URL
                    self.article_url = article_url
                    self.a_urlp = urlparse(self.article_url)    # break doin the URL into components
                    schmeme = self.a_urlp.scheme                # http or https
                    self.url_netloc = self.a_urlp.netloc
                    path = self.a_urlp.path                     # /path/to/article
                else:
                    logging.info(f'%s - Mangled source url: {article_url}' % cmi_debug)
                    return 1
                    
                uhint, uhdescr = self.yfn_uh.uhinter(hcycle, self.article_url)
                logging.info(f'%s - Source url [{self.a_urlp.netloc}] / u:{uhint} / {uhdescr}' % cmi_debug)
                if uhint == 0: thint = 0.0      # real news / local page
                elif uhint == 1: thint = 1.0    # fake news / remote-stub @ YFN stub
                elif uhint == 2: thint = 4.0    # video
                elif uhint == 3: thint = 1.1    # remote article
                elif uhint == 4: thint = 7.0    # research report
                elif uhint == 5: thint = 6.0    # bulk yahoo premium service
                else: thint = 9.9               # unknown
                
                inf_type = self.yfn_uh.confidence_lvl(thint)
                ml_atype = uhint
                
                print(f"News article:  {symbol} [ {path} ]")
                print(f"Article type:  {inf_type[0]}")
                print(f"News agency:   {art_publisher} - {update_time} - {time_now}")
                print(f"origin:        {self.url_netloc} - conf: [ t:{ml_atype} u:{uhint} h:{thint} ]")
                print(f"Full URL:      {self.article_url}")
                print(f"Short title:   {art_title}")
                print(f"Long teaser:   {art_teaser}")
                
                self.ml_brief.append(art_title)             # WARNING: I dont knonw whjy this is done
                auh = hashlib.sha256(self.article_url.encode()) # Generate URL hash
                aurl_hash = auh.hexdigest()                     # compute hash
                if aurl_hash not in dedupe_set:                 # dedupe membership test
                    dedupe_set.add(aurl_hash)                   # add aurl_hash to dupe_set for next membership test
                    logging.info( f'{cmi_debug}   - Add unique url hash to ML Ingest DB @ {cg:02}: {aurl_hash[:30]}...' )
                    print(f" ")
                    # Build full AI NLP candidate dict row
                    nd = {
                        "symbol": symbol,
                        "urlhash": aurl_hash,
                        "type": ml_atype,
                        "thint": thint,
                        "uhint": uhint,
                        "publisher": art_publisher,
                        "title": art_title,
                        "teaser": art_teaser,
                        "url": self.article_url
                    }
                    self.ml_ingest.update({self.nlp_x: nd})
                    cg += 1
                    hcycle += 1
                else:
                    logging.warning(f'%s - Duplicate URL found / Skipping... {aurl_hash[:30]}...' % cmi_debug)
                    print(f"Duplicate URL hash found / Skipping... {aurl_hash[:30]}...")
                    continue  # Skip to next article if duplicate URL hash found
            else:
                logging.warning(f'%s - No URL found for article: {art_title[:45]}...' % cmi_debug)
        return

    # ################
    def interpret_page(self, item_idx, data_row):
        """
        Depth : 2 
        Page interpreter. Noit a network get() request done here
        Sets uhint, thint, durl for each article logic processing at Depth 3
        Simplified version that works with crawl4ai extracted data
        """
        cmi_debug = __name__+"::" + self.interpret_page.__name__+".#"+str(item_idx)
        
        symbol = data_row['symbol']
        ttype = data_row['type']
        thint = data_row['thint']
        uhint = data_row['uhint']
        durl = data_row['url']
        cached_state = data_row['urlhash']
        
        logging.info(f'%s - Processing article type: {ttype} / uhint: {uhint} / thint: {thint}' % cmi_debug)
        
        # Determine viability based on article type
        if uhint == 0:  # Local full article
            logging.info(f"%s - Depth: 2.0 / Local Full article / [ u: {uhint} h: {thint} ]" % cmi_debug)
            data_row.update({"viable": 1})
            self.ml_ingest[item_idx] = data_row
            return uhint, thint, durl
            
        elif uhint == 1:  # Fake local news stub
            logging.info(f"%s - Depth: 2.1 / Fake Local news stub / [ u: {uhint} h: {thint} ]" % cmi_debug)
            data_row.update({"viable": 0})
            self.ml_ingest[item_idx] = data_row
            return uhint, thint, durl
            
        elif uhint == 2:  # Video
            logging.info(f"%s - Depth: 2.2 / Video story / [ u: {uhint} h: {thint} ]" % cmi_debug)
            data_row.update({"viable": 0})
            self.ml_ingest[item_idx] = data_row
            return uhint, thint, durl
            
        elif uhint == 3:  # External publication
            logging.info(f"%s - Depth: 2.3 / External publication / [ u: {uhint} h: {thint} ]" % cmi_debug)
            data_row.update({"viable": 0})
            self.ml_ingest[item_idx] = data_row
            return uhint, thint, durl
            
        elif uhint == 4:  # Research report
            logging.info(f"%s - Depth: 2.4 / Research report / [ u: {uhint} h: {thint} ]" % cmi_debug)
            data_row.update({"viable": 0})
            self.ml_ingest[item_idx] = data_row
            return uhint, thint, durl
            
        else:
            logging.info(f"%s - Depth: 2.? / Unknown type / [ u: {uhint} h: {thint} ]" % cmi_debug)
            data_row.update({"viable": 0})
            self.ml_ingest[item_idx] = data_row
            return uhint, 9.9, durl

    # ################
    # HEAVY network data extractor
    # Reads each URL, and crawls that page, extracting key elements
    # This can be refactors to craw4al, but currently uses BS4
    def extract_article_data(self, item_idx, sentiment_ai):
        """
        Depth : 3
        This function is controleed from main()
        Extractor:  BS4
        Build the Text corpus for 1 article
        Calls sentiment computation for 1 article
        Only do this once the article has been evaluated and we know exactly where/what each article is
        Any article we read, should have its resp & BS4 objects cached in yfn_jsdb{}
        Set the Body Data zone, the <p> TAG zone
        Extract all of the full article raw text
        Store it in a Database
        Associate it to the metadata info for this article
        Its now available for the LLM to read and process
        """

        cmi_debug = __name__+"::"+self.extract_article_data.__name__+".#"+str(self.yti)
        logging.info( f'%s - IN / Work on item... [ {item_idx} ]' % cmi_debug )
        data_row = self.ml_ingest[item_idx]
        symbol = data_row['symbol']
        cached_state = data_row['urlhash']
        if 'exturl' in data_row.keys():
            durl = data_row['exturl']
            external = True                 # not a local yahoo.com hosted article
            logging.info( f'%s - exturl found in ml_ingest DB' % cmi_debug )
        else:
            durl = data_row['url']
            external = False               # this is a local yahoo.com hosted article
            logging.info( f'%s - exturl not found in ml_ingest DB' % cmi_debug )

        symbol = symbol.upper()

        # TODO:
        # since this code is exact duplicate of interpret_page(), we
        # shoud make this a method and call it when needed
        # it would retrun self.nsoup and set self.yfn_jsdata
        logging.info( f'%s - urlhash cache lookup: {cached_state}' % cmi_debug )
        cmi_debug = __name__+"::"+self.extract_article_data.__name__+".#"+str(item_idx)+" - URL: "+durl
        logging.info( f'%s' % cmi_debug )     # hack fix for urls containg "%" break logging module (NO FIX
        cmi_debug = __name__+"::"+self.extract_article_data.__name__+".#"+str(item_idx)

        logging.info( f'%s - CHECKING cache... {cached_state}' % cmi_debug )
        try:
            self.yfn_jsdb[cached_state]         # fast key KeyError existance test
            cx_soup = self.yfn_jsdb[cached_state]
            logging.info( f'%s - Found cahce entry: Render data from cache...' % cmi_debug )
            #cx_soup.html.render()           # since we dont cache the raw data, we need to render the page again
            self.yfn_jsdata = cx_soup.text   # store the rendered raw data
            dataset_1 = self.yfn_jsdata
            logging.info( f'%s - Cached object    : {cached_state}' % cmi_debug )
            logging.info( f'%s - Cache req/get    : {type(cx_soup)}' % cmi_debug )
            logging.info( f'%s - Cahce Dataset    : {type(dataset_1)}' % cmi_debug )
            logging.info( f'%s - Cache URL object : {cx_soup.url}' % cmi_debug )
            logging.info( f'%s - BS4 read url now...' % cmi_debug )

            # This is where we refactor to crawl4ai
            self.nsoup = BeautifulSoup(escape(dataset_1), "html.parser")        # BS4 read() <- replace with crawl4ai

        except KeyError:
            logging.info( f'%s - MISSING from cache - must read page' % cmi_debug )
            logging.info( f'%s - Cache URL object   : {type(durl)}' % cmi_debug )
            cmi_debug = __name__+"::"+self.extract_article_data.__name__+".#"+str(item_idx)+" - URL: "+durl
            logging.info( f'%s' % cmi_debug )     # hack fix for urls containg "%" break logging module (NO FIX
            cmi_debug = __name__+"::"+self.extract_article_data.__name__+".#"+str(item_idx)
 
            self.yfqnews_url = durl
            ip_urlp = urlparse(durl)
            ip_headers = ip_urlp.path
            self.ext_req = self.init_live_session(durl)        # uses basic requests modeule. Sould use requests_html at least
            self.update_headers(ip_headers)

            #xhash = self.do_js_get(item_idx)           # for JS get()
            xhash = self.do_simple_get(durl)            # Basic HTML get() - non_JS rendered
            cy_soup = self.yfn_jsdb[cached_state]       # pikup up get() response 
            logging.info( f'%s - Retry cache lookup: {cached_state}' % cmi_debug ) 
            if self.yfn_jsdb[cached_state]:
                logging.info( f'%s - Found cache entry: {cached_state}' % cmi_debug )
                self.yfn_jsdata = cy_soup.text
                dataset_2 = self.yfn_htmldata           # Basic HTML engine  get()
                logging.info ( f'%s - Cache url:     {cy_soup.url}' % cmi_debug )
                logging.info ( f'%s - Cache req/get: {type(cy_soup)}' % cmi_debug )
                logging.info ( f'%s - Cache Dataset: {type(self.yfn_jsdata)}' % cmi_debug )
                
                # This is where we refactor to crawl4ai
                self.nsoup = BeautifulSoup(escape(dataset_2), "html.parser")        # BS4 read() <- replace with crawl4ai

            else:
                logging.info( f'%s - FAIL to set BS4 data !' % cmi_debug )
                return 10, 10.0, "ERROR_unknown_state!"

        logging.info( f'%s - Extract Article TEXT for AI Sentiment reader: {durl[:30]}...' % (cmi_debug) )
        if external is True:    # page is Micro stub Fake news article
            logging.info( f'%s - Skipping Micro article stub... [ {item_idx} ]' % cmi_debug )
            return
            # Do not do deep data extraction
            # just use the CAPTION Teaser text from the YFN local url
            # we extracted that in interpret_page()
        else:
            logging.info( f'%s - set BS4 data zones for article: [ {item_idx} ]' % cmi_debug )
            local_news = self.nsoup.find(attrs={"class": "body yf-1ir6o1g"})             # full news article - locally hosted
            local_news_meta = self.nsoup.find(attrs={"class": "main yf-cfn520"})        # comes above/before article
            local_stub_news = self.nsoup.find_all(attrs={"class": "body yf-3qln1o"})   # full news article - locally hosted
            local_stub_news_p = local_news.find_all("p")    # BS4 all <p> zones (not just 1)

            ####################################################################
            ##### AI M/L Gen AI NLP starts here !!!                      #######
            ##### Heavy CPU utilization / local LLM Model & no GPU       #######
            ####################################################################
            #
            hs = cached_state    # the URL hash (passing it to sentiment_ai for us in DF)
            logging.info( f'%s - Init M/L NLP Tokenizor sentiment-analyzer pipeline...' % cmi_debug )
            total_tokens, total_words, total_scent = sentiment_ai.compute_sentiment(symbol, item_idx, local_stub_news_p, hs)

            print ( f"Total tokens generated: {total_tokens} / Neutral: {sentiment_ai.sentiment_count['neutral']} / Postive: {sentiment_ai.sentiment_count['positive']} / Negative: {sentiment_ai.sentiment_count['negative']}")

            # set up a dataframe to hold the aggregated sentiment for this article in columns.
            # This is helpful for merging the info with other dataframes later on
            self.sen_data = [[ \
                        item_idx, \
                        hs, \
                        sentiment_ai.sentiment_count['positive'], \
                        sentiment_ai.sentiment_count['neutral'], \
                        sentiment_ai.sentiment_count['negative'] ]]

            sen_df_row = pd.DataFrame(self.sen_data, columns=[ 'art', 'urlhash', 'positive', 'neutral', 'negative'] )
            self.sen_stats_df = pd.concat([self.sen_stats_df, sen_df_row])
            
            # create emtries in the Neo4j Graph database
            # - check if KG has existing node entry for this symbol+news_article
            # if not... create one
            print ( f"======================================== End: {item_idx} ===============================================")

        return total_tokens, total_words, total_scent

    # #####################################################################################
    # sync crawl4 implementation of extract_article_data()
        # HEAVY network data extractor
    # Reads each URL, and crawls that page, extracting key elements
    # This can be refactors to craw4al, but currently uses BS4
    def extr_artdata_depth3(self, item_idx, sentiment_ai):
        """
        Depth : 3
        This function is controleed from main()
        Extractor:  crawl4
        Build the Text corpus for 1 article
        Calls sentiment computation for 1 article
        Only do this once the article has been evaluated and we know exactly where/what each article is
        Any article we read, should have its resp & crawl4 objects cached in yfn_jsdb{}
        CSS HTML selectors defined in YF_sym_srticle_schema.json
        Extract all of the full article raw text via crawl4ai selectors
        Store it in a Database
        Associate it to the metadata info for this article
        Its now available for the LLM to read and process
        """

        cmi_debug = __name__+"::"+self.extr_artdata_depth3.__name__+".#"+str(self.yti)
        logging.info( f'%s - IN / Scanning article... [ {item_idx} ]' % cmi_debug )
        data_row = self.ml_ingest[item_idx]
        symbol = data_row['symbol']
        durl = None
        if 'exturl' in data_row.keys():
            durl = data_row['exturl']
            external = True                 # not a local yahoo.com hosted article
            logging.info( f'%s - Ext url found in ml_ingest DB - skipping...' % cmi_debug )
        else:
            durl = data_row['url']
            external = False               # this is a local yahoo.com hosted article
            cached_state = data_row['urlhash']
            symbol = symbol.upper()
            logging.info( f'%s - urlhash cache lookup: {cached_state}' % cmi_debug )
            cmi_debug = __name__+"::"+self.extr_artdata_depth3.__name__+".#"+str(item_idx)+" - URL: "+durl
            logging.info( f'%s' % cmi_debug )     # hack fix for urls containg "%" break logging module (NO FIX
            cmi_debug = __name__+"::"+self.extr_artdata_depth3.__name__+".#"+str(item_idx)
            try:    # cehck for cached_state in yfn_jsdb
                self.yfn_jsdb[cached_state]         # fast key KeyError test for key: urlhash 
                cx = self.yfn_jsdb[cached_state]    # pickup the full dict @ key: urlhash
                logging.info( f'%s - Found cahce entry: Render data from cached resp...' % cmi_debug )
                self.yfn_cxresult = cx['result']      # store the rendered raw data
                dataset_1 = self.yfn_c4_data
                logging.info( f'%s - Cached object    : {cached_state}' % cmi_debug )
                logging.info( f'%s - Cache cx         : {type(cx)}' % cmi_debug )
                logging.info( f'%s - Cahce Dataset    : {type(dataset_1)}' % cmi_debug )
                logging.info( f'%s - Cache URL object : {cx['url']}' % cmi_debug )
                logging.info( f'%s - Sent URL object  : {durl}' % cmi_debug )
                logging.info( f'%s - C4 crawl artcile url now...' % cmi_debug )   
                # Do it this way so that...
                # - we  can spawn multiple async tasks in parallel
                # - self.yfn_jsdb() is not blocking and can handle multiple threads writing to it
                # self.yfn_jsdb[aurl_hash] is set by c4_engine_depth3
                result = asyncio.run(self.c4_engine_depth3(durl, item_idx))  # call the crawl4ai engine to extract 1 article's data
                # result is:
                #   { 'url': self.yfqnews_url,
                #     'data': self.yfn_crawl_data,
                #     'result': result  }
                self.articles_crawled[item_idx] = result  # future feat: parallel crawl4ai extraction
            except KeyError:
                logging.info( f'%s - MISSING from cache...' % cmi_debug )
                logging.info( f'%s - Eval URL object type: {type(durl)}' % cmi_debug )
                cmi_debug = __name__+"::"+self.extr_artdata_depth3.__name__+".#"+str(item_idx)+" - URL: "+durl
                logging.info( f'%s' % cmi_debug )     # hack fix for urls containg "%" break logging module (NO FIX
                cmi_debug = __name__+"::"+self.extr_artdata_depth3.__name__+".#"+str(item_idx)
                # see same note above
                result = asyncio.run(self.c4_engine_depth3(durl, item_idx))  # call the crawl4ai engine to extract 1 article's data
                self.articles_crawled[item_idx] = result  # future feat: parallel crawl4ai extraction
                # see same note above for result structure
                
                self.yfqnews_url = durl
                cy = self.yfn_c4_result[cached_state]   # pikup up result dict
                logging.info( f'%s - Retry cache lookup: {cached_state}' % cmi_debug ) 
                if self.yfn_c4_result[cached_state]:
                    logging.info( f'%s - Located entry:  {cached_state}' % cmi_debug )
                    self.yfn_c4_data = cy['result']      # store the rendered raw data
                    dataset_2 = result                  # Basic HTML engine  get()
                    logging.info( f'%s - c4 pure Result   : {type(result)}' % cmi_debug )
                    logging.info( f'%s - Cached result    : {type(self.yfn_c4_data)}' % cmi_debug )
                    logging.info( f'%s - Cache cy data    : {type(cy['data'])}' % cmi_debug )
                    logging.info( f'%s - Cache Dataset    : {type(dataset_2)}' % cmi_debug )
                    logging.info( f'%s - Cache URL object : {cy['url']}' % cmi_debug )
                    logging.info( f'%s - Sent URL object  : {durl}' % cmi_debug )
                    logging.info( f'%s - C4 crawl artcile url now...' % cmi_debug )  
                    # Do it this way so that...
                    # - we  can spawn multiple async tasks in parallel
                    # - self.yfn_jsdb() is not blocking and can handle multiple threads writing to it
                    # self.yfn_jsdb[aurl_hash] is set by c4_engine_depth3
                    # result = asyncio.run(self.c4_engine_depth3(durl, item_idx))  # call the crawl4ai engine to extract 1 article's data
                    # result is:
                    #   { 'url': self.yfqnews_url,
                    #     'data': self.yfn_crawl_data,
                    #     'result': result  }
                    # self.articles_crawled[item_idx] = result  # future feat: parallel crawl4ai extraction                    
                else:
                    logging.info( f'%s - FAIL to craw article {item_idx}' % cmi_debug )
                    return None
            except Exception as e:
                logging.error(f'{cmi_debug} - Artcile [{item_idx} data Craw failed: {e}')
                return None
            
        # we can now extract all the <p> zone TEXT from the article
        # and pass it to the sentiment_ai module for NLP processing
        logging.info( f'%s - Extract Article TEXT for AI Sentiment reader: {durl[:30]}...' % (cmi_debug) )
        if external is True:    # page is Micro stub Fake news article
            logging.info( f'%s - Skipping Micro article stub... [ {item_idx} ]' % cmi_debug )
            return
            # Do not do deep data extraction
            # just use the CAPTION Teaser text from the YFN local url
            # we extracted that in interpret_page()
        else:
            logging.info( f'%s - Access C4 selector zones in article: [ {item_idx} ]' % cmi_debug )
            # local_news = self.nsoup.find(attrs={"class": "body yf-1ir6o1g"})          # full news article - locally hosted
            # local_news_meta = self.nsoup.find(attrs={"class": "main yf-cfn520"})      # comes above/before article
            # local_stub_news = self.nsoup.find_all(attrs={"class": "body yf-3qln1o"})  # full news article - locally hosted
            # BS4 all <p> zones (not just 1)
            
            p = 0
            #c4_dict = self.articles_crawled[item_idx]
            #c4_dict = self.yfn_c4_data[item_idx]
            c4_dict = self.yfn_c4_result[cached_state]
            print (f"##### DEBUG 1: {type(c4_dict)}")
            print (f"##### DEBUG 2: {type(c4_dict['url'])}")
            print (f"##### DEBUG 3: {type(c4_dict['data'])}")
            print (f"##### DEBUG 4: {type(c4_dict['result'])}")
            
            #self.extracted_elements = c4_dict['data']  # get the craw4al result for this article
            # get the craw4al result for this article
            #for element in self.extracted_elements: # GLOBAL class accessor : article >>dataset<< extracted by crawl4ai
            for i, element in enumerate(c4_dict['data']):
                art_c0 = element.get('Content', 'ERROR_no_title')      # extract craw4al element
                art_a0 = element.get('Article', 'ERROR_no_teaser')   # extract craw4al element
                #art_c1 = element[p]      # extract craw4al element
                #p += 1
                print (f"c0: {art_c0}")
                print (f"a0: {art_a0}")
                print (f"i:  {i}")
            
            
            #print (f"{result.cleaned_html}")
            article = self.articles_crawled[item_idx]  # get the craw4al result for this article
            content_text = article.get('Content', 'ERROR_no_content')  # extract craw4al element 
            article_text = article.get('Article', 'ERROR_no_article')  # extract craw4al element
            
            print (f"========================================= Article: {item_idx} ===============================================")
            print (f"##### DEBUG\n 1: {content_text}")
            print (f"========================================= Article: {item_idx} ===============================================")
            print (f"##### DEBUG\n 2: {article_text}")
            print (f"========================================= Article: {item_idx} ===============================================")
            breakpoint()  # DEBUG: inspect the article data
            
            #title_text = article.get('Title', 'ERROR_no_title')    # extract craw4al element
            
            ####################################################################
            ##### AI M/L Gen AI NLP starts here !!!                      #######
            ##### Heavy CPU utilization / local LLM Model & no GPU       #######
            ####################################################################
            #
            hs = cached_state    # the URL hash (passing it to sentiment_ai for us in DF)
            logging.info( f'%s - Init M/L NLP Tokenizor sentiment-analyzer pipeline...' % cmi_debug )
            total_tokens, total_words, total_scent = sentiment_ai.compute_sentiment(symbol, item_idx, local_stub_news_p, hs)

            print ( f"Total tokens generated: {total_tokens} / Neutral: {sentiment_ai.sentiment_count['neutral']} / Postive: {sentiment_ai.sentiment_count['positive']} / Negative: {sentiment_ai.sentiment_count['negative']}")

            # set up a dataframe to hold the aggregated sentiment for this article in columns.
            # This is helpful for merging the info with other dataframes later on
            self.sen_data = [[ \
                        item_idx, \
                        hs, \
                        sentiment_ai.sentiment_count['positive'], \
                        sentiment_ai.sentiment_count['neutral'], \
                        sentiment_ai.sentiment_count['negative'] ]]

            sen_df_row = pd.DataFrame(self.sen_data, columns=[ 'art', 'urlhash', 'positive', 'neutral', 'negative'] )
            self.sen_stats_df = pd.concat([self.sen_stats_df, sen_df_row])
            
            # create emtries in the Neo4j Graph database
            # - check if KG has existing node entry for this symbol+news_article
            # if not... create one
            print ( f"======================================== End: {item_idx} ===============================================")

        return total_tokens, total_words, total_scent
  
    # ################ 7
    async def c4_engine_depth3(self, durl, item_idx):
        """
        Heler function for extr_artdata_depth3() ONLY - not a public API
        Just the crawl4ai engine for Depth 3
        Dont do anyting else
        """
        config = None
        schema = None
        cmi_debug = __name__+"::" + self.c4_engine_depth3.__name__+".#"+str(self.yti)+"."+str(item_idx)
        if not durl or not isinstance(durl, str):       # empty str or not type(str)
            logging.error(f'{cmi_debug} - Invalid URL: {durl}')
            return None

        logging.info(f'%s  - Load schema file: [ {self.YF_sym_article_schema} ]' % cmi_debug)
        schema_file_path = f"{self.YF_sym_article_schema}"
        if os.path.exists(schema_file_path):
            with open(schema_file_path, "r") as f:
                schema = json.load(f)
                logging.info(f'%s  - crawl4ai schema loaded' % cmi_debug)
                #self.YF_sym_article_schema = schema
                logging.info(f'%s  - INIT extraction strategy...' % cmi_debug)
                extraction_strategy = JsonCssExtractionStrategy(schema)
                '''
                js_cmds = [
                    "window.scrollTo(0, document.body.scrollHeight);",
                    "await new Promise(resolve => setTimeout(resolve, 2000));"
                    ]
                '''
                config = CrawlerRunConfig(
                    extraction_strategy=extraction_strategy,
                    scan_full_page=True,
                    cache_mode=CacheMode.BYPASS  # Bypass cache for fresh data
                    )
        else:
            logging.error(f'%s - FAILED to load schema file: [ {self.YF_sym_article_schema} ]' % cmi_debug)
            return None

        try:
            async with AsyncWebCrawler() as crawler:
                logging.info(f'%s - Crawl article [ {item_idx} ] NOW...' % cmi_debug)
                result = await crawler.arun(durl, config=config)                
                if result.success:
                    logging.info(f'%s  - crawl4ai extraction successful' % cmi_debug)
                    self.yfn_crawl_data = json.loads(result.extracted_content)
                    auh = hashlib.sha256(durl.encode()) # prep hash
                    aurl_hash = auh.hexdigest()         # genertae hash WARN: need to do dedupe check !!
                    # GLOBALLY set the yfn_jsdb DB dict for this artcile @ key = aurl_hash, vale = { }
                    self.yfn_c4_result[aurl_hash] = {
                        'url': durl,
                        'data': self.yfn_crawl_data,
                        'result': result
                    }
                    logging.info(f'%s  - Created cache entry: [ {aurl_hash} ]' % cmi_debug)
                    return result
                else:
                    logging.error(f'%s - crawl4ai extraction failed: {result.error}' % cmi_debug)
                    return None                    
        except Exception as e:
            logging.error(f'{cmi_debug} - Error during crawl4ai extraction: {e}')
            return None
             
    # ###############
    def dump_ml_ingest(self):
        """
        Dump the contents of ml_ingest{}, which holds the NLP candidates
        """
        cmi_debug = __name__+"::" + self.dump_ml_ingest.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug)
        print("===== Dump: ML Ingest DB / Depth 1 / AI NLP candidates ==================")
        
        for k, d in self.ml_ingest.items():
            print (f"Index: {k:03}\n{d}")
            '''
            print(f"{k:03} {d['symbol']:.5} / {d['urlhash']} Hints: [t:{d['type']} u:{d['uhint']} h:{d['thint']}]")
            if 'exturl' in d.keys():
                print(f"          Local:     {d['url']}")
                print(f"          External:  {d['exturl']}")
            else:
                print(f"          Local:     {d['url']}")
            '''
        
        return