#! python3
import os
import json
import logging
import hashlib
import pandas as pd
import numpy as np
import argparse
import time
import asyncio
from pathlib import Path
from typing import List
from datetime import datetime, date
from urllib.parse import urlparse
from rich import print
from rich.markup import escape

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, CrawlResult
from crawl4ai import JsonCssExtractionStrategy

# logging setup
logging.basicConfig(level=logging.INFO)

#####################################################

class yfnews_reader:
    """
    Read Yahoo Finance news reader using crawl4ai, Word Vectorizer, Positive/Negative sentiment analyzer
    Converted from BeautifulSoup to crawl4ai implementation
    """

    # global accessors
    a_urlp = None
    args = []               # class dict to hold global args being passed in from main() methods
    cur_dir = None
    cycle = 0               # class thread loop counter
    extracted_articles = None  # crawl4ai extracted articles
    get_counter = 0         # count of get() requests
    json_schema_file = None
    li_superclass = None    # all possible News articles
    ml_brief = []           # ML TXT matrix for Naive Bayes Classifier pre Count Vectorizer
    ml_ingest = {}          # ML ingested NLP candidate articles
    ml_sent = None
    nlp_x = 0
    sen_stats_df = None     # Aggregated sentiment stats for this 1 article
    symbol = None           # Unique company symbol
    url_netloc = None
    yfn_all_data = None     # JSON dataset contains ALL data
    yfn_crawl_data = None   # Crawl4ai extracted data
    yfn_jsdb = {}           # database to hold response handles from multiple crawl operations
    yfn_uh = None           # global url hinter class
    yfqnews_url = None      # SET by form_endpoint - the URL that is being worked on
    yti = 0                 # Unique instance identifier
    
    article_url = "https://www.defaul_instance_url.com"
    this_article_url = "https://www.default_interpret_page_url.com"

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
        self.json_schema_file = f"{self.cur_dir}/YAHOO_FINANCE_crawl4ai_schema.json"
        
        return

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

    def share_hinter(self, hinst):
        cmi_debug = __name__+"::" + self.share_hinter.__name__+".#"+str(self.yti)
        logging.info(f'%s - IN {type(hinst)}' % cmi_debug)
        self.yfn_uh = hinst
        return

# ##########################################################################################################
    async def crawl4ai_extract_news(self, idx_x):
        """
        Use crawl4ai to extract news data from Yahoo Finance using JSON schema
        """
        cmi_debug = __name__+"::" + self.crawl4ai_extract_news.__name__+".#"+str(self.yti)+"."+str(idx_x)
        if not self.yfqnews_url or not isinstance(self.yfqnews_url, str):
            logging.error(f'{cmi_debug} - Invalid URL: {self.yfqnews_url}')
            return None
            
        logging.info(f'ml_yahoofinews_crawl4ai::crawl4ai_extract_news.#{self.yti}.{idx_x} - %s', self.yfqnews_url)
        
        # Load JSON schema
        logging.info(f'%s - JSON crawl4ai schema file: [ {self.json_schema_file} ]...' % cmi_debug)
        schema_file_path = f"{self.json_schema_file}"
        
        if os.path.exists(schema_file_path):
            with open(schema_file_path, "r") as f:
                schema = json.load(f)
            logging.info(f'%s - crawl4ai JSON schema loaded' % cmi_debug)
        else:
            logging.error(f'%s - FAILED to load crawl4ai schema: [ {self.json_schema_file} ]...' % cmi_debug)
            return None

        # Setup crawl4ai extraction strategy
        logging.info(f'%s - INIT crawl4ai extraction strategy...' % cmi_debug)
        extraction_strategy = JsonCssExtractionStrategy(schema)
        js_cmds = [
            "window.scrollTo(0, document.body.scrollHeight);",
            "await new Promise(resolve => setTimeout(resolve, 1500));"
        ]
        
        config = CrawlerRunConfig(
            extraction_strategy=extraction_strategy,
            scan_full_page=True,
            js_code=js_cmds,
            verbose=True,
            cache_mode=CacheMode.BYPASS  # Bypass cache for fresh data
        )

        #    headers=self.yahoo_headers,
        # Execute crawl4ai
        dedupe_set = set()
        try:
            async with AsyncWebCrawler() as crawler:
                logging.info(f'%s - doing async webcrawl NOW...' % cmi_debug)
                result = await crawler.arun(self.yfqnews_url, config=config)
                if result.success:
                    logging.info(f'%s - crawl4ai extraction successful' % cmi_debug)
                    self.yfn_crawl_data = json.loads(result.extracted_content)
                    auh = hashlib.sha256(self.yfqnews_url.encode())
                    aurl_hash = auh.hexdigest()
                    
                    if aurl_hash not in dedupe_set:             # dedupe membership test
                        dedupe_set.add(aurl_hash)               # add ihash to dupe_set for next membership test
                        self.yfn_jsdb[aurl_hash] = {            # build dict data row
                            'url': self.yfqnews_url,
                            'data': self.yfn_crawl_data,
                            'result': result
                        }
                    else:
                        logging.info(f"%s - # Duplicate data: Skipping..." % cmi_debug )
                        pass
                    logging.info(f'%s  - CREATED URL cache entry: [ {aurl_hash} ]' % cmi_debug)
                    return aurl_hash
                else:
                    logging.error(f'%s - crawl4ai extraction failed: {result.error}' % cmi_debug)
                    return None
                    
        except Exception as e:
            logging.error(f'{cmi_debug} - Exception during crawl4ai extraction: {e}')
            return None

# ##########################################################################################################
    def scan_news_feed(self, symbol, depth, scan_type, bs4_obj_idx, hash_state):
        """
        Symbol : Stock symbol NEWS FEED for articles (e.g. https://finance.yahoo.com/quote/OTLY/news?p=OTLY )
        Depth 0 : Surface scan of all news articles in the news section for a stock ticker
        Scan_type:  0 = html | 1 = crawl4ai extraction
        Share class accessors of where the New Articles live
        """
        cmi_debug = __name__+"::" + self.scan_news_feed.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug)
        symbol = symbol.upper()
        depth = int(depth)
        
        logging.info(f'%s - Scan news: {symbol} @ {self.yfqnews_url}' % cmi_debug)
        logging.info(f"%s - URL Hinter cycle: {self.yfn_uh.hcycle} " % cmi_debug)
        
        if scan_type == 1:  # crawl4ai extraction
            logging.info(f'%s - Check urlhash cache state: {hash_state}' % cmi_debug)
            
            try:
                cached_data = self.yfn_jsdb[hash_state]
                logging.info(f'%s - URL EXISTS in cache: {hash_state}' % cmi_debug)
                
                # Extract articles from crawl4ai data
                self.extracted_articles = cached_data['data']
                logging.info(f'%s - set crawl4ai data objects' % cmi_debug)
                
                # Count articles found
                if isinstance(self.extracted_articles, list):
                    article_count = len(self.extracted_articles)
                    logging.info(f'%s - Depth: 0 / Found News Articles: {article_count}' % cmi_debug)
                    
                    # Debug output
                    if self.args.get('bool_xray', False):
                        print(f" ")
                        print(f"=============== Articles found: {article_count} ====================")
                        for i, article in enumerate(self.extracted_articles):
                            if article.get('Title'):
                                print(f"Item: {i+1}: {article.get('Title', 'No title')[:70]} / (News article)")
                            else:
                                print(f"Item: {i+1}: Empty no article data")
                else:
                    logging.warning(f'%s - No articles found in extraction' % cmi_debug)
                    
            except KeyError:
                logging.error(f'%s - MISSING in cache: {hash_state}' % cmi_debug)
                return None
        
        return

# ##########################################################################################################
    def eval_news_feed_stories(self, symbol):
        """
        Depth 1 - scanning news feed stories and some metadata (depth 1)
        Convert BeautifulSoup logic to work with crawl4ai extracted data
        """
        cmi_debug = __name__+"::" + self.eval_news_feed_stories.__name__+".#"+str(self.yti)
        logging.info('%s - IN \\n' % cmi_debug)
        time_now = time.strftime("%H:%M:%S", time.localtime())
        symbol = symbol.upper()
        
        if not self.extracted_articles:
            logging.error(f'%s - No extracted articles available' % cmi_debug)
            return
        
        cg = 1
        hcycle = 1
        
        logging.info(f'%s - Article Zone scanning / ml_ingest populating...' % cmi_debug)
        
        for article in self.extracted_articles:
            self.nlp_x += 1
            
            # Extract article data from crawl4ai results
            article_title = article.get('Title', 'ERROR_no_title')
            article_url = article.get('Ext_url', '')
            publisher = article.get('Publisher', 'ERROR_no_publisher')
            print (f"##### DEBUG:\nTITLE: {article_title}\nURL: {article_url}\nPUBLISH: {publisher}" ) 

            # Clean up publisher info (remove bullet points)
            if '•' in publisher:
                publisher = publisher.split('•')[0].strip()
            
            print(f"=== Article: [ {cg} ] ===========================================")
            
            # URL processing
            if article_url:
                if article_url.startswith('http'):
                    self.article_url = article_url
                    self.a_urlp = urlparse(self.article_url)
                    pure_url = 1
                else:
                    self.article_url = f"https://finance.yahoo.com{article_url}"
                    self.a_urlp = urlparse(self.article_url)
                    pure_url = 0
                
                # URL hinting
                uhint, uhdescr = self.yfn_uh.uhinter(hcycle, self.article_url)
                logging.info(f'%s - Source url [{self.a_urlp.netloc}] / u:{uhint} / {uhdescr}' % cmi_debug)
                
                # Set thint based on uhint
                if uhint == 0: thint = 0.0      # real news / local page
                elif uhint == 1: thint = 1.0    # fake news / remote-stub @ YFN stub
                elif uhint == 2: thint = 4.0    # video
                elif uhint == 3: thint = 1.1    # remote article
                elif uhint == 4: thint = 7.0    # research report
                elif uhint == 5: thint = 6.0    # bulk yahoo premium service
                else: thint = 9.9               # unknown
                
                inf_type = self.yfn_uh.confidence_lvl(thint)
                self.url_netloc = self.a_urlp.netloc
                ml_atype = uhint
                
                print(f"New article:      {symbol} / [ {cg} ] / Depth 1")
                print(f"News item:        {inf_type[0]} / Origin confidence: [ t:{ml_atype} u:{uhint} h:{thint} ]")
                print(f"News agency:      {publisher}")
                print(f"News origin:      {self.url_netloc}")
                print(f"Article URL:      {self.article_url}")
                print(f"Article title:    {article_title}")
                
                # Add to ML brief for analysis
                self.ml_brief.append(article_title)
                
                # Generate URL hash
                auh = hashlib.sha256(self.article_url.encode())
                aurl_hash = auh.hexdigest()
                print(f"Unique url hash:  {aurl_hash}")
                print(f" ")
                
                # Build NLP candidate dict
                nd = {
                    "symbol": symbol,
                    "urlhash": aurl_hash,
                    "type": ml_atype,
                    "thint": thint,
                    "uhint": uhint,
                    "url": self.article_url,
                    "teaser": article_title,
                    "publisher": publisher,
                    "title": article_title
                }
                
                logging.info(f'%s - Add to ML Ingest DB: [ {cg} ]' % cmi_debug)
                self.ml_ingest.update({self.nlp_x: nd})
                
                cg += 1
                hcycle += 1
            else:
                logging.warning(f'%s - No URL found for article: {article_title[:45]}' % cmi_debug)
        
        return

# ##########################################################################################################
    def interpret_page(self, item_idx, data_row):
        """
        Depth 2 Page interpreter
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
            logging.info(f"%s - Depth: 2.3 / Injected add link / [ u: {uhint} h: {thint} ]" % cmi_debug)
            data_row.update({"viable": 0})
            self.ml_ingest[item_idx] = data_row
            return uhint, thint, durl
            
        elif uhint == 4:  # Video story
            logging.info(f"%s - Depth: 2.4 / Video story / [ u: {uhint} h: {thint} ]" % cmi_debug)
            data_row.update({"viable": 0})
            self.ml_ingest[item_idx] = data_row
            return uhint, thint, durl
            
        else:
            logging.info(f"%s - Depth: 2.x / Unknown type / [ u: {uhint} h: {thint} ]" % cmi_debug)
            data_row.update({"viable": 0})
            self.ml_ingest[item_idx] = data_row
            return uhint, 9.9, durl

# ##########################################################################################################
    def extract_article_data(self, item_idx, sentiment_ai):
        """
        Depth 3: Extract article data for sentiment analysis
        Simplified version for crawl4ai - uses title/teaser for sentiment analysis
        """
        cmi_debug = __name__+"::" + self.extract_article_data.__name__+".#"+str(self.yti)
        logging.info(f'%s - IN / Work on item... [ {item_idx} ]' % cmi_debug)
        
        data_row = self.ml_ingest[item_idx]
        symbol = data_row['symbol']
        
        # For crawl4ai version, we'll use the title/teaser for sentiment analysis
        # since full article extraction would require additional crawl4ai calls
        
        article_text = data_row.get('teaser', '')
        if not article_text:
            article_text = data_row.get('title', '')
        
        if not article_text:
            logging.warning(f'%s - No text available for sentiment analysis' % cmi_debug)
            return 0, 0, 0
        
        # Create mock paragraph structure for sentiment analysis
        class MockParagraph:
            def __init__(self, text):
                self.text = text
        
        mock_paragraphs = [MockParagraph(article_text)]
        
        # Run sentiment analysis
        hs = data_row['urlhash']
        logging.info(f'%s - Init M/L NLP Tokenizer sentiment-analyzer pipeline...' % cmi_debug)
        
        try:
            total_tokens, total_words, total_scent = sentiment_ai.compute_sentiment(
                symbol, item_idx, mock_paragraphs, hs
            )
            
            print(f"Total tokens generated: {total_tokens} / Neutral: {sentiment_ai.sentiment_count['neutral']} / Positive: {sentiment_ai.sentiment_count['positive']} / Negative: {sentiment_ai.sentiment_count['negative']}")
            
            # Set up dataframe for aggregated sentiment
            self.sen_data = [[
                item_idx,
                hs,
                sentiment_ai.sentiment_count['positive'],
                sentiment_ai.sentiment_count['neutral'],
                sentiment_ai.sentiment_count['negative']
            ]]
            
            sen_df_row = pd.DataFrame(self.sen_data, columns=['art', 'urlhash', 'positive', 'neutral', 'negative'])
            self.sen_stats_df = pd.concat([self.sen_stats_df, sen_df_row])
            
            print(f"======================================== End: {item_idx} ===============================================")
            
            return total_tokens, total_words, total_scent
            
        except Exception as e:
            logging.error(f'%s - Error in sentiment analysis: {e}' % cmi_debug)
            return 0, 0, 0
# ##########################################################################################################
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

    # Async wrapper methods to maintain compatibility
    async def async_scan_news_feed(self, symbol, depth, scan_type, bs4_obj_idx):
        """
        Async version of scan_news_feed that uses crawl4ai
        """
        hash_state = await self.crawl4ai_extract_news(bs4_obj_idx)
        if hash_state:
            self.scan_news_feed(symbol, depth, scan_type, bs4_obj_idx, hash_state)
        return hash_state

    def run_async_scan(self, symbol, depth, scan_type, bs4_obj_idx):
        """
        Sync wrapper for async crawl4ai operations
        """
        return asyncio.run(self.async_scan_news_feed(symbol, depth, scan_type, bs4_obj_idx))
