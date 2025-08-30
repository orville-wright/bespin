#! python3
import argparse
import asyncio
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, CrawlResult
from crawl4ai import JsonCssExtractionStrategy
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

from datastore_eng_LMDB import lmdb_io_eng

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
    cx = None
    dummy_resp0 = None
    ext_req = None          # HTMLSession request handle
    extracted_articles = None  # crawl4ai extracted articles
    kvio_eng = None
    li_superclass = None    # all possible News articles
    live_resp0 = None
    ml_brief = []           # ML TXT matrix for Naive Bayes Classifier pre Count Vectorizer
    ml_ingest = {}          # ML ingested NLP candidate articles
    ml_sent = None
    nlp_x = 0
    result_engine = "unknown"  # engine used to extract article data
    sent_ai = None          # GLOBALLY shared handle = prob a very bad idea to do it this way
    sen_stats_df = None     # Aggregated sentiment stats for this 1 article
    symbol = None           # Unique company symbol
    this_article_url = "https://www.default_interpret_page_url.com"
    url_netloc = None
    yfn_uh = None           # global url hinter class
    yfn_all_data = None     # JSON dataset contains ALL data
    yfn_all_result = None   # JSON dataset contains ALL data
    YF_sym_main_schema = None
    YF_sym_article_schema = None
    yfn_crawl_data = None   # Crawl4ai extracted data
    yfn_c4_data = None      # Crawl4ai extracted data
    yfqnews_url = None      # SET by form_endpoint - the URL that is being worked on
    yti = 0                 # Unique instance identifier

    yfn_c4_result = {}      # Crawl4ai extracted data net cache from crawl
    #                       { aurl_hash: = {
    #                               url: durl,
    #                               data: self.yfn_crawl_data,
    #                               result: result  }
                    
    yfn_jsdb = {}           # database to hold response handle from multiple crawl operations    
    # yfn_jsdb dict structure...
    #                       { aurl_hash: respons_0,
    #                       url: self.yfqnews_url,
    #                       data: self.yfn_crawl_data,
    #                       result: result  }

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
        logging.info(f"%s    - Force live cookie update via basic get()..." % cmi_debug )
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
    def do_simple_get(self, _url):
        """
        Simple basic HTML data get()  (data not processed by JAVAScript engine)
        NOTE: get URL is assumed to have allready been set (self.yfqnews_url)
              creates the urlhash entry via: yfn_jsdb[aurl_hash] = get(resp)
        """
        cmi_debug = __name__+"::"+self.do_simple_get.__name__+".#"+str(self.yti)

        js_session = HTMLSession()                  # Create a new session        
        with js_session.get(_url) as self.js_resp0:  # must do a get() - NO setting cookeis/headers)
            logging.info(f'%s  - Simple Net get()' % cmi_debug ) 

            # lOGGING HACK - helpS logging() f-string bug to handle strings with %
            cmi_debug = __name__+"::"+self.do_simple_get.__name__+".#"+str(self.yti)+"  - "+_url
            logging.info('%s' % cmi_debug )
            cmi_debug = __name__+"::"+self.do_simple_get.__name__+".#"+str(self.yti)    # reset cmi_debug
            #########################################################
            if self.js_resp0.status_code != 200:
                    logging.error(f'{cmi_debug} - Net get() failed with error: {self.js_resp0.status_code}')
                    return 1, self.js_resp0.status_code
                    ################ FAILURE ###############################

        logging.info(f'{cmi_debug}  - Net get() success status: {self.js_resp0.status_code}')
        logging.info( f"%s  - js.render() engine... DISABLED" % cmi_debug )
        logging.info( f'%s  - Store get() resp HTML dataset' % cmi_debug )
        self.js_resp2 = self.js_resp0               # Set js_resp2 to the same response as js_resp0 for now
 
        hot_cookies = requests.utils.dict_from_cookiejar(self.js_resp0.cookies)
        logging.info( f"%s  - Swap {len(hot_cookies)} cookies into LOCAL yahoo_headers" % cmi_debug )

        self.yfn_htmldata = self.js_resp0.text      # class GLOBAL store page HTML text in memory in this class

        _uh = hashlib.sha256(_url.encode())          # hash the url
        _url_hash = _uh.hexdigest()
        logging.info( f'%s  - CREATE ml_ingest DB cache entry: [ {_url_hash} ]' % cmi_debug )
        
        #self.yfn_jsdb[aurl_hash] = self.js_resp0    
        # create jsdb CACHE entry @ key=aurl_hash, value=js_resp0 (i.e. get()::resp, not  page TEXT data)
        self.yfn_jsdb[_url_hash] = {
            'url': _url,
            'data': self.yfn_htmldata,
            'result': self.js_resp0
        }

        # Xray DEBUG
        if self.args['bool_xray'] is True:
            print ( f"========================== {self.yti} / HTML get() session cookies ================================" )
            logging.info( f'%s  - resp0 type: {type(self.js_resp0)}' % cmi_debug )
            for _i in self.js_resp0.cookies.items():
                print ( f"{_i}" )

        return 0, _url_hash

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

# hack this to test crawl4 ai <li.p.text>

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
                    aurl_hash = auh.hexdigest()                     # this cache entry is dept0 @ finaince.yahoo.com
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
    def list_news_candidates_depth0(self, symbol, depth, scan_type, hash_state):
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
        cmi_debug = __name__+"::" + self.list_news_candidates_depth0.__name__+".#"+str(self.yti)
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
        Build a ML ingest DB dataset of articles fro fast post processing
        Dedupe hash's of articles
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
            art_publisher = article.get('Publisher', 'No_publisher • No_pub_time')  # extract craw4al element
            art_teaser = article.get('Teaser', 'ERROR_no_teaser')   # extract craw4al element
            try:
                _ap_sl = art_publisher.split('•')
                art_publisher =_ap_sl[0]
                update_time = _ap_sl[1]
            except:
                logging.info(f'%s - Error @ {cg} extract publiosher info...' % cmi_debug)
                art_publisher = "Err_no_publisher"
                update_time = "Err_no_pub_time"

            print(f"Eval cycle:    Depth 1  ({cg} / {self.articles_found}) =====================================================")
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
                print(f"News agency:   {art_publisher} - {update_time}")
                print(f"origin:        {self.url_netloc} - conf: [ t:{ml_atype} u:{uhint} h:{thint} ]")
                print(f"Full URL:      {self.article_url}")
                print(f"Short title:   {art_title}")
                print(f"Long teaser:   {art_teaser}")
                
                self.ml_brief.append(art_title)                 # WARNING: I dont knonw why this is done
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
    def interpret_page_depth2(self, item_idx, data_row):
        """
        Depth : 2 
        Page interpreter. Noit a network get() request done here
        Sets uhint, thint, durl for each article logic processing at Depth 3
        Simplified version that works with crawl4ai extracted data
        """
        cmi_debug = __name__+"::" + self.interpret_page_depth2.__name__+".#"+str(item_idx)
        
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

    # #############################################
    # WARN: Heavy network data extractor. 
    #       Does full get() request for every viable news article found
    # Reads each URL, and crawls that page, extracting key elements e.g. <p>.text
    # Trying to refactor to craw4al, but currently uses BS4
    def artdata_BS4_depth3(self, item_idx, sentiment_ai):
        """
        Depth: 3
        Extractor:  BS4 -  (engine decidcated to BS4 only)
        - Build the Text corpus for 1 (one) article only
        - Calls sentiment computation for 1 article
        
        WARN: 
        Only do this once the article has been evaluated and we know where/what article TEXT is
        -  article must have its get() resp & BS4 objects cached in yfn_jsdb{}
        - Sets the Body Data zone, the <p> TAG zone
        - Extracts all of the full article raw text from <p> tags
        - Stores it in a Database
        - Associate it to the metadata info for this article
        Its now available for the LLM to compute sentiment

        TODO: rename this function to ext_artdata_BS4
        This function is controleed from main()
        Returns:
        - total_tokens, total_words, total_scent, final_results
        - these vars come from: compute_sentiment()
        """

        cmi_debug = __name__+"::"+self.artdata_BS4_depth3.__name__+".#"+str(self.yti)
        logging.info( f'%s - IN / Work on item... [ {item_idx} ]' % cmi_debug )
        data_row = self.ml_ingest[item_idx]
        symbol = data_row['symbol']
        cached_state = data_row['urlhash']      # eval  DB[] @ item=item_idx, and pull out article urlhash
        self.sent_ai = sentiment_ai
        lmdb_dbname = "LMDB_0001"
        self.kvio_eng = lmdb_io_eng(1, lmdb_dbname, self.args)
        bs4_final_results = dict()  # ensure final_results is empty
        self.sent_ai.empty_vocab = 0

        # #########################################
        
        if 'exturl' in data_row.keys():
            durl = data_row['exturl']
            external = True                 # not a local yahoo.com hosted article
            logging.info( f'%s - Found exturl ml_ingest DB' % cmi_debug )
        else:
            durl = data_row['url']
            external = False               # this is a local yahoo.com hosted article
            logging.info( f'%s - No exturl in ml_ingest DB' % cmi_debug )

        symbol = symbol.upper()

        _ec, _ttk, _ttw, _sen_data, _fr = self.kvio_eng.kv_cache_engine(1, symbol, data_row, item_idx, self.sent_ai)
        
        match _ec:
            case 0:  # BS4 KVstore cache hit
                logging.info( f'%s - BS4 Deep cache hit / Rehydrated data from KVstore...' % cmi_debug )
                # rehydrate class sentiment count dict from Deep Cache dataset
                self.sent_ai.sentiment_count['positive'] = _fr["positive_count"]
                self.sent_ai.sentiment_count['neutral'] = _fr["neutral_count"]
                self.sent_ai.sentiment_count['negative'] = _fr["negative_count"]
                _sen_df_row = pd.DataFrame(_sen_data, columns=[ 'art', 'urlhash', 'positive', 'neutral', 'negative'] )                
                self.sen_stats_df = pd.concat([self.sen_stats_df, _sen_df_row])
                #print (f"##-debug-561: sen_stats_df:\n{self.sen_stats_df}" )
                #print (f"##-debug-562: _fr:\n{_fr}" )
                logging.info( f'%s - BS4 Rehydrated sentiment metrics from KV cache: {self.sent_ai.sentiment_count}' % cmi_debug )
                print (f"================================ BS4 End.#0 Deep Cache HIT ! / Rehydrated AI Metrics: {item_idx} ================================" )
                return _ttk, _ttw, _fr                        
            case 1:  # BS4 KVstore cache miss
                logging.info( f'%s - BS4 KVstore ERROR.#1 Deserialization failure !force Net read...' % cmi_debug )
                pass
            case 2:
                logging.info( f'%s - BS4 KVstore ERROR.#2 No URL Hash KEY found !force Net read...' % cmi_debug )
                pass
            case 3:
                logging.info( f'%s - BS4 KVstore MISS.#3 No cache entry !force Net read...' % cmi_debug )
                pass
            case 4:
                logging.info( f'%s - BS4 LMDB I/O FAILURE.#4 : Failed to open DB in RO mode !' % cmi_debug )
                pass
            case _:
                logging.info( f'%s - BS4 KVstore ERROR.#def Unknown error code: {_ec} !force Net read...' % cmi_debug )
                pass

        ######################################################
        # BS4 Network read()
        # Extract article text directly from Yahoo Finance URL
        #
        # logging fixe: f-string errors when URLs have a "%" - breaks logging module (NO KNOWN FIX)
        logging.info( f'%s  - BS4 urlhash Net cache lookup: {cached_state}' % cmi_debug )
        cmi_debug = __name__+"::"+self.artdata_BS4_depth3.__name__+".#"+str(item_idx)+" - URL: "+durl
        logging.info( f'%s' % cmi_debug )
        cmi_debug = __name__+"::"+self.artdata_BS4_depth3.__name__+".#"+str(item_idx)

        if external is True:    # page is Micro stub Fake news article
            logging.info( f'%s - BS4 Skipping : Micro Article stub... [ {item_idx} ]' % cmi_debug )
            return 0, 0, 0
            
        try:
            self.yfn_jsdb[cached_state]
            _built_bs4_entry = 2
        except KeyError:
            logging.info( f'%s - BS4 MISSING from Net Cache / Force Network page read !' % cmi_debug )
            cmi_debug = __name__+"::"+self.artdata_BS4_depth3.__name__+".#"+str(item_idx)+" - URL: "+durl
            logging.info( f'%s' % cmi_debug )     # hack fix for urls containg "%" break logging module (NO FIX
            cmi_debug = __name__+"::"+self.artdata_BS4_depth3.__name__+".#"+str(item_idx)
            self.yfqnews_url = durl
            ip_urlp = urlparse(durl)
            ip_headers = ip_urlp.path
            self.ext_req = self.init_live_session(durl)        # uses basic requests modeule. Sould use requests_html at least
            self.update_headers(ip_headers)
            
            _ec, xhash = self.do_simple_get(durl)            # xhash now == cached_state (what we were given, but faield to find in cache))
            match _ec:
                        case 1:  # BS4 KVstore cache hit
                            logging.info( f'%s - FAILED to read article / ERROR code: {xhash}' % cmi_debug )
                            logging.info( f'%s - BS4 prep simple Net get() due to KV cache miss : {self.sent_ai.sentiment_count}' % cmi_debug )
                            print (f"================================ BS4 Net req() FAILURE / Cannot read article URL: {item_idx} ================================" )
                            return 0, 0,None                  
                        case 0:  # BS4 KVstore cache miss
                            logging.info( f'%s - BS4 prep simple Net get() success ! continue forcing Net read...' % cmi_debug )
                            pass
                        case _:
                            logging.info( f'%s - BS4 prep simple Net get() unknown error: {_ec} ! Abandon Net URL read' % cmi_debug )
                            return 0, 0,None
                            ##########################################################################
            
            cy_soup = self.yfn_jsdb[xhash]              # ref the dict{} that do_simple_get() created
            logging.info( f'%s - BS4 EVAL.#1 : re-read Net-cache #1 for: {cached_state}' % cmi_debug ) 
            if self.yfn_jsdb[cached_state]:
                self.yfn_jsdata = self.yfn_jsdb[cached_state]['result']
                logging.info ( f'%s - BS4 Found entry:   {cached_state}' % cmi_debug )
                logging.info ( f'%s - BS4 working url:   {cy_soup['url']}' % cmi_debug )
                logging.info ( f'%s - BS4 Cache dict:    {type(cy_soup)}' % cmi_debug )
                logging.info ( f'%s - Bs4 Cache result:  {type(self.yfn_jsdata)}' % cmi_debug )  
                logging.info ( f'%s - Bs4 Cache dataset: {type(cy_soup['data'])}' % cmi_debug )
                #
                logging.info( f'%s - Good BS4 data:     Gracefully pre-built: {cached_state}' % cmi_debug )
                _dataset_1 = self.yfn_jsdata.text
                self.nsoup = BeautifulSoup(escape(_dataset_1), "html.parser")
                self.articles_crawled[item_idx] = self.nsoup
                self.result_engine = "yfn_jsdb.#1"
                _built_bs4_entry = 1
            else:
                logging.info( f'%s - BS4 FAILED to set data !' % cmi_debug )
                return 0, 0, 0
        else:       # try, expect, ends here !
            ###################################
            # BS4 Extract Article text data NOW
            # prepare dataset for BS4
            if _built_bs4_entry == 2:
                logging.info( f'%s - EVAL.#2 :      BS4 data entry...' % cmi_debug )
                logging.info( f'%s - Weird Net cache state: Try cached net data: {cached_state}' % cmi_debug )
                print (f"###-debug: jsdb:\n{self.yfn_jsdb[cached_state]} \nresult:\n{self.yfn_jsdb[cached_state]['result']}")
                _dataset_1 = self.yfn_jsdata.text
                self.nsoup = BeautifulSoup(escape(_dataset_1), "html.parser")
                self.articles_crawled[item_idx] = self.yfn_jsdb[cached_state]['result']  # future feat: parallel crawl4ai extraction
                self.result_engine = "yfn_jsdb.#2"
            else:
                logging.info( f'%s - EVAL.#3 :      BS4 data entry...' % cmi_debug )
                logging.info( f'%s - Bad BS4 data: Force extract now: {cached_state}' % cmi_debug ) 
                self._dataset_1 = self.yfn_jsdata.text
                self.nsoup = BeautifulSoup(escape(_dataset_1), "html.parser")        # BS4 read() <- replace with crawl4ai
                self.articles_crawled[item_idx] = self.nsoup     # NOTE USED: future feat: parallel crawl4ai extraction
                self.result_engine = "yfn_jsdb.#3"

        logging.info( f'%s - BS4 EVAL.#4:       Read Net Cahce entry...' % cmi_debug )
        logging.info( f'%s - Cached hash:       {cached_state}' % cmi_debug )
        logging.info( f'%s - Cache engine:      {self.result_engine}' % cmi_debug )
        logging.info( f'%s - Cache Dataset:     {type(_dataset_1)}' % cmi_debug )
        logging.info( f'%s - In Cache URL:      {self.yfn_jsdb[cached_state]['url']}' % cmi_debug )
        logging.info( f'%s - Sent in URL:       {durl}' % cmi_debug )
        logging.info( f'%s - Ready to exec BS4 extractor - get Article TEXT for AI NLP reader...' % cmi_debug )
    
        logging.info( f'%s - BS4 set Article data zones: [ {item_idx} ]' % cmi_debug )
        local_news = self.nsoup.find(attrs={"class": "body yf-1ir6o1g"})             # full news article - locally hosted
        local_news_meta = self.nsoup.find(attrs={"class": "main yf-cfn520"})        # comes above/before article
        local_stub_news = self.nsoup.find_all(attrs={"class": "body yf-3qln1o"})   # full news article - locally hosted
        try:
            local_stub_news_p = local_news.find_all("p")    # BS4 all <p> zones (not just 1)
        except AttributeError as _ae:
            logging.info( f'%s - BS4 Error FAILED to find_all TEXT <p_tags>: {_ae}"...' % cmi_debug )
            print (f"================================ BS4 End.#9 Net Read / KV Cache miss ! BS4 FAILURE ! {item_idx} ================================" )
            return 0, 0, None   # This is likely a YF Advertising redirect to non-Yahoo webpage
        else:
            pass
        
        ####################################################################
        ##### AI M/L Gen AI NLP starts here !!!                      #######
        ##### Heavy CPU utilization / local LLM Model & no GPU       #######
        ####################################################################
        #
        hs = cached_state    # the URL hash (passing it to sentiment_ai for us in DF)
        logging.info( f'%s - BS4 Exec NLP sent classifier pipeline.#0...' % cmi_debug )
        # WARN: trigger var for compute_sentiment(symbol, item_idx, local_stub_news_p, hs, 1)
        # 0 = Crawl4ai extractor, 1 = BS4 extractor
        self.total_tokens, self.total_words, _final_data_dict = self.sent_ai.compute_sentiment(symbol, item_idx, local_stub_news_p, hs, 1)
         
        # computre some keey Data Metrics for this article        
        bs4_p_tag_count = len(local_stub_news_p)    # rows of <p> tags found in article
        
        # compute total chars (BS4 specific, as C4 is diff data structure)
        _total_chars = 0
        for _i, _v in enumerate(local_stub_news_p):
            _total_chars += sum(len(_s) for _s in _v.text)

        # these are set @ compute_sentiment::nlp_sent_engine()
        # totals of all blockets
        sent_p = self.sent_ai.sentiment_count['positive']
        sent_z = self.sent_ai.sentiment_count['neutral']
        sent_n = self.sent_ai.sentiment_count['negative']
        
        # set up a dataframe to hold the aggregated sentiment for this article in columns.
        # This is helpful for merging the info with other dataframes later on
        self.sen_data = [[
                    item_idx,
                    hs,
                    sent_p,
                    sent_z,
                    sent_n
                    ]]

        #print (f"###-debug-734: KV-write extr JSON - {self.sen_data}" )            
        sen_df_row = pd.DataFrame(self.sen_data, columns=[ 'art', 'urlhash', 'positive', 'neutral', 'negative'] )
        self.sen_stats_df = pd.concat([self.sen_stats_df, sen_df_row])
        
        _final_data_dict.update({
            'positive_count': sent_p,
            'neutral_count': sent_z,
            'negative_count': sent_n,
            'chars_count': int(_total_chars),
            'total_words': int(self.total_words),
            'total_tokens': int(self.total_tokens),
            })
        
        logging.info( f'%s - BS4 Open LMDB in READ-WRITE mode...' % cmi_debug )
        kv_success = self.kvio_eng.open_lmdb_RW(2)
        if kv_success is not None:      # explicit reliable singleton None test
            _url_hash = data_row['urlhash']
            _key = "0001"+"."+symbol+"."+_url_hash          # we are looking at the artile here. So test for this K/V data
            bs4_kvs_key = _key.encode('utf-8')              # byte encode 
            logging.info( f'%s - BS4 WRITE sent package to KVstore: {_key}' % cmi_debug )
            with self.kvio_eng.env.begin(write=True) as _txn:
                _kvs_json_dataset = json.dumps(_final_data_dict, default=str)    # serialize to JSON
                _txn.put(bs4_kvs_key, _kvs_json_dataset.encode('utf-8'))   # write data to LMDB                
                self.kvio_eng.env.close
        else:
            logging.info( f'%s - BS4 FAILED to access KVstore / not writing cache entry !' % cmi_debug )
            pass    # Not Fatal - faield to open LMDB. Continue with manual Network Read
        # empty vocabulary pretty-printer logic for eof=""
        if self.sent_ai.empty_vocab > 0:
            print (f"\n")

        bs4_final_results.update({
            'article': item_idx,
            'urlhash': hs,
            'total_tokens': self.total_tokens,
            'chars_count': int(_total_chars),
            'total_words': self.total_words,
            'scentence': _final_data_dict.get('scentence'),
            'paragraph': _final_data_dict.get('paragraph'),
            'random': _final_data_dict.get('random'),
            'positive_count': sent_p,
            'neutral_count': sent_z,
            'negative_count': sent_n,
            'bs4_rows': bs4_p_tag_count
        })

        footer = (f"Total tokenz: {self.total_tokens} / "
                  f"Words: {self.total_words} / "
                  f"Chars: {_total_chars} / "
                  f"Postive: {sent_p} / Neutral: {sent_z} / Negative: {sent_n} / "
                  f"BS4 ptags: {bs4_p_tag_count}"
                )
        print (f"{footer}")
        print (f"================================ BS4 End.#2 Net Read / KV Cache miss ! KV created: {item_idx} ================================" )
        return self.total_tokens, self.total_words, bs4_final_results

# #####################################################################################
    # WARNING:
    # sync crawl4 implementation of artdata_BS4_depth3()
    # HEAVY network data extractor
    # Reads each URL, and crawls that page, extracting key elements
    
    def artdata_C4_depth3(self, item_idx, sentiment_ai):
        """
        Extractor:  CRAWL4AI -  (engine decidcated to BS4 only)
        - Build the Text corpus for 1 (one) article only
        - Calls sentiment computation for 1 article
        
        Depth : 3
        TODO: rename this function to ext_artdata_C4
        This function is controlled from main()
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

        cmi_debug = __name__+"::"+self.artdata_C4_depth3.__name__+".#"+str(self.yti)
        logging.info( f'%s - IN / Work on item... [ {item_idx} ]' % cmi_debug )
        data_row = self.ml_ingest[item_idx]
        symbol = data_row['symbol']
        self.sent_ai = sentiment_ai
        lmdb_dbname = "LMDB_0001"
        self.kvio_eng = lmdb_io_eng(2, lmdb_dbname, self.args)
        self.sent_ai.empty_vocab = 0
        c4_final_results = dict()  # ensure final_results is empty
        
        # #########################################
                
        if 'exturl' in data_row.keys():
            durl = data_row['exturl']
            external = True                 # not a local yahoo.com hosted article
            logging.info( f'%s - Ext url found in ml_ingest DB - skipping...' % cmi_debug )
        else:
            durl = data_row['url']
            external = False               # this is a local yahoo.com hosted article
            logging.info( f'%s - No exturl in ml_ingest DB' % cmi_debug )
            cached_state = data_row['urlhash']
        
        symbol = symbol.upper()
        _ec, _ttk, _ttw, _sen_data, _fr = self.kvio_eng.kv_cache_engine(1, symbol, data_row, item_idx, self.sent_ai)

        match _ec:
            case 0:  # BS4 KVstore cache hit
                logging.info( f'%s - C4 Deep cache hit / Rehydrated data from KVstore...' % cmi_debug )
                # rehydrate class sentiment count dict from Deep Cache dataset
                self.sent_ai.sentiment_count['positive'] = _fr["positive_count"]
                self.sent_ai.sentiment_count['neutral'] = _fr["neutral_count"]
                self.sent_ai.sentiment_count['negative'] = _fr["negative_count"]
                _sen_df_row = pd.DataFrame(_sen_data, columns=[ 'art', 'urlhash', 'positive', 'neutral', 'negative'] )                
                self.sen_stats_df = pd.concat([self.sen_stats_df, _sen_df_row])
                #print (f"##-@817: sen_stats_df:\n{self.sen_stats_df}" )
                #print (f"##-@818: _fr:\n{_fr}" )
                logging.info( f'%s - C4 Rehydrated sentiment metrics from KV cache: {self.sent_ai.sentiment_count}' % cmi_debug )
                print (f"================================ C4 End.#0 Deep Cache HIT ! / Rehydrated AI Metrics: {item_idx} ================================" )
                return _ttk, _ttw, _fr                        
            case 1:  # C4 KVstore cache miss
                logging.info( f'%s - C4 KVstore ERROR.#1 Deserialization failure !force Net read...' % cmi_debug )
                pass
            case 2:
                logging.info( f'%s - C4 KVstore ERROR.#2 No URL Hash KEY found !force Net read...' % cmi_debug )
                pass
            case 3:
                logging.info( f'%s - C4 KVstore MISS.#3 No cache entry !force Net read...' % cmi_debug )
                pass
            case 4:
                logging.info( f'%s - C4 LMDB I/O FAILURE.#4 : Failed to open DB in RO mode !' % cmi_debug )
                pass
            case _:
                logging.info( f'%s - C4 KVstore ERROR.#def Unknown error code: {_ec} ! force Net read...' % cmi_debug )
                pass

        #####################################################
        # C4
        # Network ret() read the article text
        #
        logging.info( f'%s - C4 urlhash Net cache lookup: {cached_state}' % cmi_debug )
        cmi_debug = __name__+"::"+self.artdata_C4_depth3.__name__+".#"+str(item_idx)+" - URL: "+durl
        logging.info( f'%s' % cmi_debug )     # hack fix for urls containg "%" break logging module (NO FIX
        cmi_debug = __name__+"::"+self.artdata_C4_depth3.__name__+".#"+str(item_idx)

        try:                                    # cehck for cached_state in yfn_jsdb
            self.yfn_jsdb[cached_state]         # pickup the full dict @ key: urlhash - if this doesnt error/excpe = SUCCESS !
            _built_c4_entry = 2
        except KeyError:
            logging.info( f'%s - C4 MISSING from Net Cache / Force Network page read !' % cmi_debug )
            cmi_debug = __name__+"::"+self.artdata_C4_depth3.__name__+".#"+str(item_idx)+" - URL: "+durl
            logging.info( f'%s' % cmi_debug )     # hack fix for urls containg "%" break logging module (NO FIX
            cmi_debug = __name__+"::"+self.artdata_C4_depth3.__name__+".#"+str(item_idx)
            
            result = asyncio.run(self.c4_engine_depth3(durl, item_idx))  # exec crawl4ai engine t extraction of 1 article's data
            self.articles_crawled[item_idx] = result  # NOTE USED: future feat: parallel crawl4ai extraction

            self.yfqnews_url = durl
            cy = self.yfn_c4_result[cached_state]    # pickup up result dict
            logging.info( f'%s - C4 EVAL.#1: re-read:  {cached_state}' % cmi_debug ) 
            if self.yfn_c4_result[cached_state]:
                logging.info( f'%s - C4 Found entry:   {cached_state}' % cmi_debug )
                self.yfn_c4_data = cy['result']        # store the rendered raw data
                _dataset_2 = result                    # Basic HTML engine  get()
                logging.info( f'%s - C4 result:        {type(self.yfn_c4_data)}' % cmi_debug )
                logging.info( f'%s - C4 JSON data:     {type(cy['data'])}' % cmi_debug )
                logging.info( f'%s - C4 alt2 result:   {type(_dataset_2)}' % cmi_debug )
                logging.info( f'%s - C4 URL object:    {cy['url']}' % cmi_debug )
                logging.info( f'%s - C4 Sent in URL:   {durl}' % cmi_debug )
                _built_c4_entry = 1
                # Do it this way so that...
                # - we can spawn multiple async tasks in parallel
                # - self.yfn_jsdb() is non-blocking and can handle multiple threads writing to it
            else:
                logging.info( f'%s - C4 FAILED to crawl article {item_idx}' % cmi_debug )
                return 0, 0, 0    # I think this is the correct return status
        except Exception as _c4e:
            logging.error(f'{cmi_debug} - Artcile [{item_idx} data Crawl failed: {_c4e}')
            return 0, 0, 0

                
        # extract all <p> zone TEXT from the article
        ####################################################################
        ##### AI M/L Gen AI NLP starts here !!!                      #######
        ##### Heavy CPU utilization / local LLM Model & no GPU       #######
        ####################################################################
        #
        if _built_c4_entry == 1:
            logging.info( f'%s - EVAL.#2 : C4 data entry...' % cmi_debug )
            logging.info( f'%s - Good C4 data:     Gracefully pre-built: {cached_state}' % cmi_debug ) 
            dataset_1 = self.yfn_c4_result[cached_state]['result']
            result_engine = "yfn_c4_result"
            self.articles_crawled[item_idx] = result    # crawl4ai result
        elif _built_c4_entry == 2:
            logging.info( f'%s - EVAL.#3 : C4 data entry...' % cmi_debug )
            logging.info( f'%s - Weird C4 state:   Try cached Net data: {cached_state}' % cmi_debug )
            #print (f"###-debug: jsdb:\n{self.yfn_jsdb[cached_state]} \nresult:\n{self.yfn_jsdb[cached_state]['result']}")
            dataset_1 = self.yfn_jsdb[cached_state]['result']
            result_engine = "yfn_jsdb"
            self.articles_crawled[item_idx] = self.yfn_jsdb[cached_state]['result']  # future feat: parallel crawl4ai extraction
            #self.yfn_jsdb[cached_state]['result']
        else:
            logging.info( f'%s - EVAL.#4 : C4 data entry...' % cmi_debug )
            logging.info( f'%s - Bad C4 data:      Force crawl now: {cached_state}' % cmi_debug ) 
            result = asyncio.run(self.c4_engine_depth3(durl, item_idx))  # call the crawl4ai engine to extract 1 article's data
            self.articles_crawled[item_idx] = result  # NOTE USED: future feat: parallel crawl4ai extraction
            dataset_1 = self.yfn_c4_result[cached_state]['result']
            result_engine = "yfn_c4_result"

        logging.info( f'%s - Cached hash    {cached_state}' % cmi_debug )
        logging.info( f'%s - Cache engine   {result_engine}' % cmi_debug )
        logging.info( f'%s - C4 Dataset     {type(dataset_1)}' % cmi_debug )
        logging.info( f'%s - In Cache URL   {self.yfn_c4_result[cached_state]['url']}' % cmi_debug )
        logging.info( f'%s - Sent URL in    {durl}' % cmi_debug )
        logging.info( f'%s - Ready to exec C4 extractor - get Article TEXT for AI NLP reader...' % cmi_debug )
        # Do it this way so that...
        # - we  can spawn multiple async tasks in parallel
        # - self.yfn_jsdb() is not blocking and can handle multiple threads writing to it
        # self.yfn_jsdb[aurl_hash] is set by c4_engine_depth3() and do_simple)_get()
        #
        # result is:
        #   { 'url': self.yfqnews_url,
        #     'data': self.yfn_crawl_data,
        #     'result': result  }
        #
        logging.info( f'%s - Extract Article TEXT for AI NLP reading...' % (cmi_debug) )
        if external is True:    # page is Micro stub Fake news article
            logging.info( f'%s - Skipping Micro article stub... [ {item_idx} ]' % cmi_debug )
            return 0, 0, 0
        else:
            logging.info( f'%s - Access C4 selector zones in article: [ {item_idx} ]' % cmi_debug )
            c4_dict = self.yfn_c4_result[cached_state]
            art_all_p = list()                                          # ensure temp list is empty
            for i, element in enumerate(c4_dict['data']):
                    art_all_p.append(element.get('Content'))            # extract craw4al element
                    try:
                        _total_chars = sum(len(_s) for _s in art_all_p)     # compute total len of all chars in extracted data 
                    except TypeError:   # catch None
                        _total_chars = 7    # force analysis, possible PREMIUM paywall block page
                    match _total_chars:
                        case 0:
                            print ( f"================================ C4 End.#0 No data: {item_idx} ================================")
                            return 0, 0, 0
                        case 7:
                            yfn_prem_paywall = element.get('Premium_paywall')
                            if yfn_prem_paywall.upper() == "PREMIUM":
                                _final_data_dict = dict()
                                _final_data_dict.update({
                                'article': item_idx,
                                'urlhash': "0123456789-0000000000-9876543210",
                                'total_tokens': 0,
                                'chars_count': 0,
                                'total_words': 0,
                                'scentence': 0,
                                'paragraph': 0,
                                'random': 0,
                                'positive_count': 0,
                                'neutral_count': 0,
                                'negative_count': 0
                            })
                                print ("Premium Paywalled article. Skipping...")
                                print ( f"================================ C4 End.#1 YF Premium paywall: {item_idx} ================================")
                                #return 0, 0, _final_data_dict
                                return 0, 0, None
                            else:
                                print ("Unknown article type. Skipping...")
                                print ( f"================================ C4 End.#2 Unknown type: {item_idx} ================================")
                                return 0, 0, 0
                        case _:
                            ####################################################################
                            ##### AI M/L Gen AI NLP starts here !!!                      #######
                            ##### Heavy CPU utilization / local LLM Model & no GPU       #######
                            ####################################################################
                            #
                            hs = cached_state    # the URL hash (passing it to sentiment_ai for us in DF)
                            logging.info( f"%s - C4 Exec NLP sent classifier pipeline.#0..." % cmi_debug )
                            # 0 = Crawl4ai extractor, 1 = BS4 extractor
                            self.total_tokens, self.total_words, _final_data_dict = self.sent_ai.compute_sentiment(symbol, item_idx, art_all_p, hs, 0)
                            #self.total_tokens, self.total_words, _final_data_dict = self.sent_ai.compute_sentiment(symbol, item_idx, local_stub_news_p, hs, 1)

                            self.sent_ai.cr_package.update({ 'chars_count': int(_total_chars) })
                            self.sent_ai.cr_package.update({ 'total_words': int(self.total_words) })

                            # these are set @ compute_sentiment::nlp_sent_engine()
                            # totals of all blockets
                            sent_p = self.sent_ai.sentiment_count['positive']
                            sent_z = self.sent_ai.sentiment_count['neutral']
                            sent_n = self.sent_ai.sentiment_count['negative']
                            
                            _final_data_dict.update({
                                'positive_count': sent_p,
                                'neutral_count': sent_z,
                                'negative_count': sent_n
                                })

                            self.sen_data = [[
                                item_idx,
                                hs,
                                sent_p,
                                sent_z,
                                sent_n
                                ]]  

                            #print (f"##-@1088: KV-write extr JSON - {self.sen_data}" ) 
                            sen_df_row = pd.DataFrame(self.sen_data, columns=[ 'art', 'urlhash', 'positive', 'neutral', 'negative'] )
                            self.sen_stats_df = pd.concat([self.sen_stats_df, sen_df_row])

                            _final_data_dict.update({
                                'positive_count': sent_p,
                                'neutral_count': sent_z,
                                'negative_count': sent_n,
                                'chars_count': int(_total_chars),
                                'total_words': int(self.total_words),
                                'total_tokens': int(self.total_tokens),
                                })

                            # Create LMBD KV cache entry
                            logging.info( f'%s - C4 Open LMDB in READ-WRITE mode...' % cmi_debug )
                            kv_success = self.kvio_eng.open_lmdb_RW(3)
                            if kv_success is not None:
                                _url_hash = data_row['urlhash']
                                _key = "0001"+"."+symbol+"."+_url_hash     # we are looking at the artile here. So test for this K/V data
                                c4_kvs_key = _key.encode('utf-8')          # byte encode 
                                logging.info( f'%s - C4 WRITE sent package to KVstore: {_key}' % cmi_debug )
                                with self.kvio_eng.env.begin(write=True) as _txn:
                                    _kvs_json_dataset = json.dumps(_final_data_dict, default=str)
                                    _txn.put(c4_kvs_key, _kvs_json_dataset.encode('utf-8'))     # write data to LMDB
                                    self.kvio_eng.env.close
                            else:
                                logging.info( f'%s - C4 FAILED to access KVstore / not writing cache entry !' % cmi_debug )
                                pass        # Not Fatal - faield to open LMDB. Continue with manual Network Read
                            # empty vocabulary pretty-printer logic for eof=""
                            if self.sent_ai.empty_vocab > 0:
                                print (f"\n")

                            c4_final_results.update({
                                'article': item_idx,
                                'urlhash': hs,
                                'total_tokens': self.total_tokens,
                                'chars_count': int(_total_chars),
                                'total_words': self.total_words,
                                'scentence': _final_data_dict.get('scentence'),
                                'paragraph': _final_data_dict.get('paragraph'),
                                'random': _final_data_dict.get('random'),
                                'positive_count': sent_p,
                                'neutral_count': sent_z,
                                'negative_count': sent_n
                            })


                            footer = (f"Total tokenz: {self.total_tokens} / "
                                    f"Words: {self.total_words} / "
                                    f"Chars: {_total_chars} / "
                                    f"Postive: {sent_p} / Neutral: {sent_z} / Negative: {sent_n} / "
                                    )
                            print (f"{footer}")
                            print (f"================================ C4 End.#2 Net Read / KV Cache miss ! KV created: {item_idx} ================================" )
                            return self.total_tokens, self.total_words, c4_final_results

        print (f"##-@1141: Unknown state!" )
        return 0, 0, None
    
    # ################ 7
    async def c4_engine_depth3(self, durl, item_idx):
        """
        Helper function for artdata_C4_depth3() ONLY - not a public API
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
                extraction_strategy = JsonCssExtractionStrategy(schema, verbose=True)
                js_cmds = [
                    "window.scrollTo(0, document.body.scrollHeight);",
                    "await new Promise(resolve => setTimeout(resolve, 2000));"
                    ]
                
                # scan_full_page=True,
                # js_code = js_cmds
                config = CrawlerRunConfig(
                    extraction_strategy=extraction_strategy,
                    cache_mode=CacheMode.BYPASS  # Bypass cache for fresh data
                    )
        else:
            logging.error(f'%s - FAILED to load schema file: [ {self.YF_sym_article_schema} ]' % cmi_debug)
            return None

        logging.info(f'%s  - Crawl article [ {item_idx} ] NOW...' % cmi_debug)
        try:
            async with AsyncWebCrawler() as crawler:
                result = await crawler.arun(durl, config=config)        # exec the craw HERE !!!!
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
                    logging.info(f'%s  - Create C4 crawl cache entry: {aurl_hash}' % cmi_debug)
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
            pass
            #print (f"Index: {k:03}\n{d}")
            '''
            print(f"{k:03} {d['symbol']:.5} / {d['urlhash']} Hints: [t:{d['type']} u:{d['uhint']} h:{d['thint']}]")
            if 'exturl' in d.keys():
                print(f"          Local:     {d['url']}")
                print(f"          External:  {d['exturl']}")
            else:
                print(f"          Local:     {d['url']}")
            '''
        
        return
