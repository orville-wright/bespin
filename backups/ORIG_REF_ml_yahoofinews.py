#! python3
from requests_html import HTMLSession
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from datetime import datetime, date
import hashlib
import re
import logging
import pandas as pd
#import modin.pandas as pd
import numpy as np
import argparse
import time
from rich import print
from rich.markup import escape
from playwright.sync_api import sync_playwright

# logging setup
logging.basicConfig(level=logging.INFO)

#####################################################

class yfnews_reader:
    """
    Read Yahoo Finance news reader, Word Vectorizer, Positive/Negative sentiment analyzer
    """

    # global accessors
    a_urlp = None
    args = []               # class dict to hold global args being passed in from main() methods
    cycle = 0               # class thread loop counter
    ext_req = ""            # HTMLSession request handle
    get_counter = 0         # count of get() requests
    js_session = None       # SET by this class during __init__ - main requests session
    js_resp0 = None         # HTML session get() - response handle
    js_resp2 = None         # JAVAScript session get() - response handle
    li_superclass = None    # all possible News articles
    ml_brief = []           # ML TXT matrix for Naieve Bayes Classifier pre Count Vectorizer
    ml_ingest = {}          # ML ingested NLP candidate articles
    ml_sent = None
    nlp_x = 0
    nsoup = None            # BS4 shared handle between UP & DOWN (1 URL, 2 embeded data sets in HTML doc)
    sen_stats_df = None     # Aggregated sentiment stats for this 1 article
    symbol = None           # Unique company symbol
    ul_tag_dataset = None   # BS4 handle of the <tr> extracted data
    url_netloc = None
    yfn_uh = None           # global url hinter class
    yfqnews_url = None      # SET by form_endpoint - the URL that is being worked on
    yfn_all_data = None     # JSON dataset contains ALL data
    yfn_htmldata = None     # Page in HTML
    yfn_jsdata = None       # Page in JavaScript-HTML
    yfn_jsdb = {}           # database to hold response handles from multiple js.session_get() ops
    yti = 0                 # Unique instance identifier

    article_url = "https://www.defaul_instance_url.com"
    dummy_url = "https://finance.yahoo.com/screener/predefined/day_losers"
    this_article_url = "https://www.default_interpret_page_url.com"
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

    def __init__(self, yti, symbol, global_args):
        self.yti = yti
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s - Instantiate.#{yti}' % cmi_debug )
        # init empty DataFrame with preset colum names
        self.args = global_args
        self.symbol = symbol
        self.nlp_x = 0
        self.cycle = 1
        self.sent_df0 = pd.DataFrame(columns=[ 'Row', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time'] )
        
        return

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
        self.yfqnews_url = 'https://finance.yahoo.com/quote/' + symbol + '/news/'    # use global accessor (so all paths are consistent)
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

        self.yfn_htmldata = self.js_resp0.text      # store entire page HTML text in memory in this class
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

###################################### 9 ###########################################

    def scan_news_feed(self, symbol, depth, scan_type, bs4_obj_idx, hash_state):
        """
        Symbol : Stock symbol NEWS FEED for articles (e.g. https://finance.yahoo.com/quote/OTLY/news?p=OTLY )
        Depth 0 : Surface scan of all news articles in the news section for a stock ticker
        Scan_type:  0 = html | 1 = Javascript render engine
        Share class accessors of where the New Articles live i.e. the <li> section
        """
        cmi_debug = __name__+"::"+self.scan_news_feed.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        symbol = symbol.upper()
        depth = int(depth)
        
        logging.info( f'%s - Scan news: {symbol} @ {self.yfqnews_url}' % cmi_debug )
        logging.info( f"%s - URL Hinter cycle: {self.yfn_uh.hcycle} " % cmi_debug )
        if scan_type == 0:    # Simple HTML BS4 scraper
            logging.info( f'%s - Check urlhash cache state: {hash_state}' % cmi_debug )
        try:
            self.yfn_jsdb[hash_state]       # check if the URL hash is in the cache
            logging.info( f'%s - URL EXISTS in cache: {hash_state}' % cmi_debug )
            cx_soup = self.yfn_jsdb[hash_state]
            self.nsoup = BeautifulSoup(cx_soup.html.html, "html.parser")   # !!!! this was soup = but I have no idea where "soup" gets set
            logging.info( f'%s - set BS4 data objects' % cmi_debug )
            
            # Major area that gets BROKEN by Yahoo Finance changes
            self.ul_tag_dataset = self.nsoup.find_all("section", class_="yf-1ce4p3e")        # produces : list iterator
            # self.ul_tag_dataset.div.div.div.div.ul.find_all()  # find the first article in the list}

            container_node = self.ul_tag_dataset[0]
            self.li_superclass = container_node.find_all('li')

        except KeyError as error:
            logging.info( f'%s - MISSING in cache: Must read JS page' % cmi_debug )
            logging.info( f'%s - Force read news url: {self.yfqnews_url}' % cmi_debug )
            hx = self.ext_pw_js_get(bs4_obj_idx)
            logging.info( f'%s - FRESH JS page in use: [ {bs4_obj_idx} ]' % cmi_debug )
            nsoup = BeautifulSoup(self.yfn_jsdata.text, "html.parser")    # store gloabl. dont use cache object 
            logging.info( f'%s - set BS4 data objects' % cmi_debug )

            self.ul_tag_dataset = self.nsoup.find(attrs={"class": "container yf-1ce4p3e"} )        # produces : list iterator
            #print ( f"########################### 3 ######################################" )
            #print ( f"### DEBUG: {self.ul_tag_dataset[0]}" )
            #print ( f"########################### 4 ######################################" )
            #self.li_superclass = self.ul_tag_dataset.find_all(attrs={"stream-item story-item yf-1usaaz9"} )
            xxx = self.ul_tag_dataset[0]
            self.li_superclass = xxx.find_all('li')

  
        logging.info( f'%s - Depth: 0 / Found News containers: {len(self.ul_tag_dataset[0])}' % cmi_debug )
        logging.info( f'%s - Depth: 0 / Found Sub cotainers:   {len(list(self.ul_tag_dataset[0].children))} / Tags: {len(list(self.ul_tag_dataset[0].descendants))}' % cmi_debug )
        logging.info( f'%s - Depth: 0 / Found News Articles:   {len(self.li_superclass)}' % cmi_debug)
 
        # >>Xray DEBUG<<
        if self.args['bool_xray'] is True:
            print ( f" " )
            x = y = 1
            print ( f"=============== <li> zone : {x} children+descendants ====================" )
            for child in self.li_superclass:
                if child.name is not None:
                    try:
                        print ( f"Item: {y}: {child.h3.text} / (potential News article)" )
                        y += 1
                        """
                        for element in child.descendants:
                            print ( f"{y}: {element.name} ", end="" )
                            y += 1
                            """
                    except AttributeError as error:
                        x += 1
                        print ( f"==================== End <li> zone : {x} =========================" )
                else:
                    print ( f"Item: {y}: Empty no article data" )
                    print ( f"\n==================== End <li> zone : {x} =========================" )
                    x += 1
    
        return

###################################### 10 ###########################################

    def eval_news_feed_stories(self, symbol):
        """
        Depth 1 - scanning news feed stories and some metatdata (depth 1)
        INFO: we are NOT looking deeply inside each metat data article (depth 1) yet
        NOTE: assumes connection was previously setup & html data structures are pre-loaded
              leverages default JS session/request handle
              Depth 1 -Iinterrogate items within the main [News Feed page]
        1. cycle though the top-level NEWS FEED page for this stock
        2. Scan each article found
        3. For each article, extract KEY news elements (i.e. Headline, Brief, local URL, remote UIRL)
        4. leverage the URL hinter. Make a decision on TYPE & HINTER results
        5. Decide worthness of REAL news articles & insert into ml_ingest{} NLP candidate list
        6. Exreact some key info
        7. Create a Candidate list of articles in [ml_ingest] array 
        """

        cmi_debug = __name__+"::"+self.eval_news_feed_stories.__name__+".#"+str(self.yti)
        logging.info('%s - IN \n' % cmi_debug )
        time_now = time.strftime("%H:%M:%S", time.localtime() )
        symbol = symbol.upper()
        li_superclass_all = self.ul_tag_dataset[0].find_all(attrs={"class": "js-stream-content Pos(r)"} )
        mini_headline_all = self.ul_tag_dataset[0].div.find_all(attrs={'class': 'C(#959595)'})
        li_subset_all = self.ul_tag_dataset[0].find_all('li')

        h3_counter = a_counter = 0
        x = 1
        y = 0
        hcycle = 1
        pure_url = 9                                         # saftey preset
        uhint = 9                                            # saftey preset
        thint = 99.9                                         # saftey preset
        self.article_teaser ="ERROR_default_data_0"
        ml_atype = 0

        ## GENERATOR: Scan & find critical tags
        def atag_gen():                         #  extract <h3>, agency, author, publish date
            a_counter = 0
            for li_tag in self.li_superclass:   # BS4 object set from scan_news_feed()
                self.nlp_x += 1
                for element in li_tag.descendants:
                    if element.name == "a":
                        a_counter += 1
                        if element.h3 is not None:
                            yield ( f"{a_counter}")
                            yield ( f"{element.h3.text}" )
                            if element.has_attr('href') is True:
                                #yield ( f'ZONE : {a_counter} : H3URL : {element.get("href")}' )
                                yield ( f'{element.get("href")}' )
                                news_ag = li_tag.find(attrs={'class': 'publishing yf-1weyqlp'}) 
                                if news_ag is not None:
                                    news_ag = news_ag.text.split("•")
                                    yield ( f"{news_ag[0]}")
                                else:
                                    yield ( f"Failed to extract News Agency" )

        ########## end Generatior

        scan_a_zone = atag_gen()
        try:
            cg = 1
            news_agency ="ERROR_default_data_1"
            logging.info( f'%s - Article Zone scanning / ml_ingest populating...' % cmi_debug )
            while True:
                li_a_zone = next(scan_a_zone)
                self.article_teaser = next(scan_a_zone)
                print ( f"================== Article: [ {cg} ] / A-Zone: [ {li_a_zone} ] ==================" )
                self.article_url = next(scan_a_zone)
                self.a_urlp = urlparse(self.article_url)
                news_agency = next(scan_a_zone)
                inf_type = "Undefined"

                for safety_cycle in range(1):    # ABUSE for/loop BREAK as logic control exit (poor mans switch/case)
                    if self.a_urlp.scheme == "https" or self.a_urlp.scheme == "http":    # check URL scheme specifier
                        uhint, uhdescr = self.yfn_uh.uhinter(hcycle, self.article_url)       # raw url string
                        logging.info( f'%s - Source url [{self.a_urlp.netloc}] / u:{uhint} / {uhdescr}' % (cmi_debug) )
                        pure_url = 1                    # explicit pure URL to remote entity
                        if uhint == 0: thint = 0.0      # Fake news / remote-stub @ YFN stub
                        if uhint == 1: thint = 1.0      # real news / local page
                        if uhint == 2: thint = 4.0      # video (currently / FOR NOW, assume all videos are locally hosted on finanice.yahoo.com
                        if uhint == 3: thint = 1.1      # shoudl never trigger here - see abive... <Pure-Abs url>
                        if uhint == 4: thint = 7.0      # research report / FOR NOW, assume all research reports are locally hosted on finanice.yahoo.com
                        if uhint == 5: thint = 6.0      # Bulk Yahoo Premium Service add
                        inf_type = self.yfn_uh.confidence_lvl(thint)  # my private look-up / returns a tuple
                        self.url_netloc = self.a_urlp.netloc      # get FQDN netloc
                        ml_atype = uhint
                        hcycle += 1
                        break
                    else:
                        self.a_url = f"https://finance.yahoo.com{self.article_url}"
                        self.a_urlp = urlparse(self.a_url)
                        self.url_netloc = self.a_urlp.netloc      # get FQDN netloc
                        logging.info( f'%s - Source url: {self.a_urlp.netloc}' % (cmi_debug) )
                        uhint, uhdescr = self.yfn_uh.uhinter(hcycle, self.a_urlp)          # urlparse named tuple
                        if uhint == 0: thint = 0.0      # real news / remote-stub @ YFN stub
                        if uhint == 1: thint = 1.0      # real news / local page
                        if uhint == 2: thint = 4.0      # video (currently / FOR NOW, assume all videos are locally hosted on finanice.yahoo.com
                        if uhint == 3: thint = 1.1      # shoudl never trigger here - see abive... <Pure-Abs url>
                        if uhint == 4: thint = 7.0      # research report / FOR NOW, assume all research reports are locally hosted on finanice.yahoo.com
                        if uhint == 5: thint = 6.0      # Bulk Yahoo Premium Service add                      
                        pure_url = 0                    # locally hosted entity
                        ml_atype = uhint                    # Real news
                        inf_type = self.yfn_uh.confidence_lvl(thint)                # return var is tuple
                        hcycle += 1
                        break       # ...need 1 more level of analysis analysis to get headline & teaser text

                print ( f"New article:      {symbol} / [ {cg} ] / Depth 1" )
                print ( f"News item:        {inf_type[0]} / Origin confidence: [ t:{ml_atype} u:{uhint} h:{thint} ]" )
                print ( f"News agency:      {news_agency}" )
                print ( f"News origin:      {self.url_netloc}" )
                print ( f"Article URL:      {self.article_url}" )
                #print ( f"Article headline: {article_headline}" )
                print ( f"Article teaser:   {self.article_teaser}" )

                self.ml_brief.append(self.article_teaser)           # add Article teaser long TXT into ML pre count vectorizer matrix
                auh = hashlib.sha256(self.article_url.encode())     # hash the url
                aurl_hash = auh.hexdigest()
                print ( f"Unique url hash:  {aurl_hash}" )
                print ( f" " )

                # build NLP candidate dict for deeper pre-NLP article analysis in Level 1
                # ONLY insert type 0, 1 articles as NLP candidates !!
                # WARN: after interpret_page() this DICT may contain new fields i.e. 'exturl:'
                nd = {
                    "symbol" : symbol,
                    "urlhash" : aurl_hash,
                    "type" : ml_atype,
                    "thint" : thint,
                    "uhint" : uhint,
                    "url" : self.a_urlp.scheme+"://"+self.a_urlp.netloc+self.a_urlp.path,
                    "teaser" : self.article_teaser
                }
                logging.info( f'%s - Add to ML Ingest DB: [ {cg} ]' % (cmi_debug) )
                self.ml_ingest.update({self.nlp_x : nd})
                cg += 1

        except StopIteration:
            pass

        return

###################################### 11 ###########################################
# method 11
    def interpret_page(self, item_idx, data_row):
        """
        Depth 2 Page interpreter
        Interrogate this news page and translate HTML zomes/tags using Beautiful Soup
        Test for known news page types (e.g. good news articles, fake news, video news etc)
        Return info that allows us to definatively know what we're looking at and how/where to AI NLP read the text of the news artcile.
        NOTE: Complex analysis of URLs becasue they point to many different types of pages in differnt locations. 
        The stub/page can have miltple personas, so this translater is where the magic happens...
        """

        cmi_debug = __name__+"::"+self.interpret_page.__name__+".#"+str(item_idx)
        right_now = date.today()
        idx = item_idx
        #data_row = data_row
        symbol = data_row['symbol']
        ttype = data_row['type']
        thint = data_row['thint']
        uhint = data_row['uhint']
        durl = data_row['url']
        cached_state = data_row['urlhash']

        self.this_article_url = data_row['url']
        symbol = symbol.upper()

        logging.info( f'%s - urlhash cache lookup: {cached_state}' % cmi_debug )
        cmi_debug = __name__+"::"+self.interpret_page.__name__+".#"+str(item_idx)+" - "+durl
        logging.info( f'%s' % cmi_debug )     # hack fix for urls containg "%" break logging module (NO FIX
        cmi_debug = __name__+"::"+self.interpret_page.__name__+".#"+str(item_idx)

        try:
            self.yfn_jsdb[cached_state]
            cx_soup = self.yfn_jsdb[cached_state]
            logging.info( f'%s - Cached object FOUND: {cached_state}' % cmi_debug )
            dataset_1 = self.yfn_jsdata     # 1
            self.nsoup = BeautifulSoup(escape(dataset_1), "html.parser")
            logging.info( f'%s - Cache BS4 object:   {type(cx_soup)}' % cmi_debug )
            logging.info( f'%s - Dataset object    : {type(dataset_1)}' % cmi_debug )
            logging.info( f'%s - Cache URL object  : {type(durl)}' % cmi_debug )
        except KeyError:
            logging.info( f'%s - Not in Cache / must read page' % cmi_debug )
            logging.info( f'%s - Cache type : {type(durl)}' % cmi_debug )

            # HACK to help logging() f-string bug to handle strings with %
            cmi_debug = __name__+"::"+self.interpret_page.__name__+".#"+str(item_idx)+" - "+durl
            logging.info( f'%s' % cmi_debug )     # hack fix for urls containg "%" break logging module (NO FIX
            cmi_debug = __name__+"::"+self.interpret_page.__name__+".#"+str(item_idx)
            #logging.info( f'%s - Cache URL : {escape(durl)}' % cmi_debug )
            
            # Must read the page, since its not in the cache
            self.yfqnews_url = durl
            ip_urlp = urlparse(durl)
            ip_headers = ip_urlp.path           # remove https:// from url
            self.init_live_session(durl)        # WARN: not sure this does much. Suspect cookie refresh
            self.update_headers(ip_headers)     # update coooike object - path: ip_headers

            #xhash = self.ext_do_js_get(idx)     # JS Render mode : loads data into self.yfn_jsdata
            xhash = self.do_simple_get(durl)     # basic HTML mode: non JS Basic HTML get()
            self.yfqnews_url = durl
            logging.info( f'%s - REPEAT cache lookup: {cached_state}' % cmi_debug )
            # print (f"####### Dump Cache #####\n{self.yfn_jsdb.keys()}")
            
            try:
                self.yfn_jsdb[cached_state]
                logging.info( f'%s - Cached  object  FOUND: {cached_state}' % cmi_debug )
                logging.info( f'%s - Hash from forced READ: {xhash}' % cmi_debug )
                cy_soup = self.yfn_jsdb[cached_state]    # get() response 

                #dataset_2 = self.yfn_jsdata
                dataset_2 = self.yfn_htmldata           # for testing non JS Basic HTML get()
                logging.info ( f'%s - cache url:     {type(durl)}' % cmi_debug )
                logging.info ( f'%s - cache request: {type(cy_soup)}' % cmi_debug )
                logging.info ( f'%s - Cache dataset: {type(self.yfn_jsdata)}' % cmi_debug )
                self.nsoup = BeautifulSoup(escape(dataset_2), "html.parser")
            except KeyError:
                logging.info( f'%s - CORRUPT cache state' % cmi_debug )
                logging.info( f'%s - Cache URL object  : {type(durl)}' % cmi_debug )
                logging.info( f'%s - FAILED to read JS page and set BS4 obejcts' % cmi_debug )
                return 10, 10.0, "ERROR_unknown_state!"
        
        logging.info( f'%s - set BS4 data zones for Article: [ {idx} ]' % cmi_debug )
        local_news = self.nsoup.find(attrs={"class": "body yf-1ir6o1g"})   # full news article - locally hosted
        #local_news = self.nsoup.find(attrs={"class": "body yf-tsvcyu"})   # full news article - locally hosted
        # local_news_meta = self.nsoup.find(attrs={"class": "main yf-cfn520"})   # comes above/before article
        local_news_meta = self.nsoup.find(attrs={"class": "byline yf-1k5w6kz"})   # comes above/before article
        local_stub_news = self.nsoup.find_all(attrs={"class": "article yf-l7apfj"})
        local_story = self.nsoup.find(attrs={"class": "body yf-tsvcyu"})  # Op-Ed article - locally hosted
        local_video = self.nsoup.find(attrs={"class": "body yf-tsvcyu"})  # Video story (minimal supporting text) stub - locally hosted
        full_page = self.nsoup()  # full news article - locally hosted
        #rem_news = nsoup.find(attrs={"class": "caas-readmore"} )           # stub news article - remotely hosted


        # Depth 2.0 :Local news article / Hosted in YFN
        if uhint == 0:
                logging.info ( f"%s - Depth: 2.0 / Local Full artice / [ u: {uhint} h: {thint} ]" % cmi_debug )
                logging.info ( f'%s - Depth: 2.0 / BS4 processed doc length: {len(self.nsoup)}' % cmi_debug )
                logging.info ( f'%s - Depth: 2.0 / nsoup type is: {type(self.nsoup)}' % cmi_debug )
                
                author_zone = local_news_meta.find("div", attrs={"class": "byline-attr-author yf-1k5w6kz"} )                    
                pubdate_zone = local_news_meta.find("div", attrs={"class": "byline-attr-time-style"} )
                try:
                    author = author_zone.a.string
                except AttributeError:
                    logging.info ( f"%s - Depth: 2.0 / Author zone error:  No <A> zone - trying basic..." % cmi_debug )
                    try:
                        author = author_zone.string
                    except AttributeError:
                        logging.info ( f"%s - Depth: 2.0 / Author zone error:  No <A> zone - trying basic..." % cmi_debug )
                        author = "ERROR_author_zone"

                pubdate = pubdate_zone.time.string

                print( f"Publish INFO:  [ Author: {author} / Published: {pubdate} ]" )
                if local_news.find_all("p" ) is not None:
                #if article_zone is not None:
                    #article = article_zone
                    logging.info ( f"%s - Depth: 2.0 / GOOD <p> zone / Local full TEXT article" % cmi_debug )
                    logging.info ( f"%s - Depth: 2.0 / NLP candidate is ready" % cmi_debug )
                    #print ( f"############################ rem news #############################" )
                    #print ( f"### DEBUG:{full_page.prettify()}" )
                    #print ( f"### DEBUG:{self.nsoup.text}" )
                    #print ( f"############################ rem news #############################" )
                data_row.update({"viable": 1})                       # cab not extra text data from this article
                self.ml_ingest[idx] = data_row                       # now PERMENTALY update the ml_ingest record @ index = id
                return uhint, thint, durl

        # Depth 2.1 : Fake local news stub / Micro article links out to externally hosted article
        if uhint == 1:
            logging.info ( f"%s - Depth: 2.1 / Fake Local news stub / [ u: {uhint} h: {thint} ]" % cmi_debug )
            logging.info ( f'%s - Depth: 2.1 / BS4 processed doc length: {len(self.nsoup)}' % cmi_debug )
            logging.info ( f'%s - Depth: 2.1 / nsoup type is: {type(self.nsoup)}' % cmi_debug )

            local_news_meta = self.nsoup.find("head")
            local_news_body = self.nsoup.find("body")
            local_news_bmain = local_news_body.find("main")
            local_news_bmart = local_news_bmain.find("article")
            local_news_bmart_divs = local_news_bmart.find_all("div")

            local_news_meta_desc = self.nsoup.find("meta", attrs={"name": "description"})

            #local_news_bmart_cap = local_news_bmart.find("div", attrs={"class": "caas-title-wrapper"})
            local_news_bmart_cap = local_news_bmart.find("h1", attrs={"class": "cover-title yf-1rjrr1"})
            #local_news_bmart_cap = local_news_bmart.find("div", attrs={"class": "atoms-wrapper"})
            caption_pct_cl = re.sub(r'[\%]', "PCT", local_news_bmart_cap.text)  # cant have % in text. Problematic !!
            logging.info ( f'%s - COVER CAPTION: {caption_pct_cl}' % cmi_debug )

            #local_news_bmart_ath = local_news_bmart.find("div", attrs={"class": "caas-attr-item-author"})
            #local_news_bmart_dte = local_news_bmart.find("div", attrs={"class": "caas-attr-time-style"})

            local_news_bmart_ath = local_news_bmart.find("div", attrs={"class": "byline-attr-author yf-1k5w6kz"})
            logging.info ( f'%s - AUTHOR: {local_news_bmart_ath.text}' % cmi_debug )

            local_news_bmart_dte = local_news_bmart.find("time", attrs={"class": "byline-attr-meta-time"})
            logging.info ( f'%s - PUB TIME: {local_news_bmart_dte.text}' % cmi_debug )

            local_news_bmain_azone = local_news_bmain.find("a")

            """
            print ( f"### DEBUG - Meta tle: {local_news_meta.title.string}" )
            print ( f"### DEBUG - Meta:     {local_news_meta_desc['content']}" )
            print ( f"### DEBUG - Caption:  {local_news_bmart_cap.h1.text}" )
            print ( f"### DEBUG - Authour:  {local_news_bmart_ath.text}" )
            print ( f"### DEBUG - Date:     {local_news_bmart_dte.text}" )
            print ( f"### DEBUG - Ext link: {local_news_bmain_azone['href']}" )
            print ( f"### DEBUG - Article:  {local_news_bmain_azone.text}" )

            for i in range(0, len(local_news_bmart_divs)):
                try:
                    print ( f"zone: {i}: {local_news_bmart_divs[i]['class']}")
                    #for j in range(0, len(local_news_meta[i]['class'])):
                    #    try:
                    #        print ( f"class: {local_news_meta[i]['class'][j]}")
                    #    except KeyError:
                    #        print ( f"class: no class")
                except KeyError:
                    print ( f"zone: {i}: no zone")
                pass
            """

            author = local_news_bmart_ath.text
            pubdate = local_news_bmart_dte.text

            # f-string cannot handle % sign in strings to expand + print
            #  cmi_debug = __name__+"::"+self.interpret_page.__name__+".#"+str(item_idx)
            cmi_debug = __name__+"::"+self.interpret_page.__name__+".#"+str(item_idx)+" - Depth: 2.1 / Caption: "+caption_pct_cl
            #logging.info ( f"%s - Depth: 2.1 / Caption: {local_news_bmart_cap.h1.text}" % cmi_debug )
            logging.info ( f"%s" % cmi_debug )
            cmi_debug = __name__+"::"+self.interpret_page.__name__+".#"+str(item_idx)
            logging.info ( f"%s - Depth: 2.1 / Author: {author} / Published: {pubdate}" % cmi_debug )
            logging.info ( f"%s - Depth: 2.1 / External link: {local_news_bmain_azone['href']}" % cmi_debug )

            thint = 1.1
            if local_news_bmart is not None:                         # article has some content
                logging.info ( f"%s - Depth: 2.1 / Good article stub / External location @: {local_news_bmain_azone['href']}" % cmi_debug )
                ext_url_item =  local_news_bmain_azone['href']       # build a new dict entry (external; absolute url)
                logging.info ( f"%s - Depth: 2.1 / Insert url into ml_ingest: [{ext_url_item}] " % cmi_debug )
                # !! this is wrong - need to add new field exturl to the data_row dict
                #data_row.update(url = ext_url_item)                 # insert new dict entry into ml_ingest via an AUGMENTED data_row
                data_row.update({"exturl": ext_url_item})            # insert new dict entry into ml_ingest via an AUGMENTED data_row
                data_row.update({"viable": 0})                       # cab not extra text data from this article
                self.ml_ingest[idx] = data_row                       # now PERMENTALY update the ml_ingest record @ index = id
                logging.info ( f"%s - Depth: 2.1 / NLP candidate is ready [ u: {uhint} h: {thint} ]" % cmi_debug )
                return uhint, thint, ext_url_item
            elif local_stub_news.text == "Story continues":          # local articles have a [story continues...] button
                logging.info ( f"%s - Depth: 2.1 / GOOD [story continues...] stub" % cmi_debug )
                logging.info ( f"%s - Depth: 2.1 / confidence level / u: {uhint} h: {thint}" % cmi_debug )
                data_row.update({"viable": 0})                       # cab not extra text data from this article
                self.ml_ingest[idx] = data_row                       # now PERMENTALY update the ml_ingest record @ index = id
                return uhint, thint, self.this_article_url           # REAL local news
            elif local_story.button.text == "Read full article":     # test to make 100% sure its a low quality story
                logging.info ( f"%s - Depth: 2.1 / GOOD [Read full article] stub" % cmi_debug )
                logging.info ( f"%s - Depth: 2.1 / confidence level / u: {uhint} h: {thint}" % cmi_debug )
                data_row.update({"viable": 0})                       # cab not extra text data from this article
                self.ml_ingest[idx] = data_row                       # now PERMENTALY update the ml_ingest record @ index = id
                return uhint, thint, self.this_article_url              # Curated Report
            else:
                logging.info ( f"%s - Depth: 2.1 / NO local page interpreter available / u: {uhint} t: {thint}" % cmi_debug )
                data_row.update({"viable": 0})                       # cab not extra text data from this article
                self.ml_ingest[idx] = data_row                       # now PERMENTALY update the ml_ingest record @ index = id
                return uhint, 9.9, self.this_article_url
        
        if uhint == 2:
            if local_video.find('p'):          # video page only has a small <p> zone. NOT much TEXT (all the news is in the video)
                logging.info ( f'%s - Depth: 2.2 / BS4 processed doc length: {len(self.nsoup)}' % cmi_debug )
                logging.info ( f"%s - Depth: 2.2 / GOOD [Video report] minimal text" % cmi_debug )
                logging.info ( f"%s - Depth: 2.2 / confidence level / u: {uhint} h: {thint}" % cmi_debug )
                data_row.update({"viable": 0})                       # cab not extra text data from this article
                self.ml_ingest[idx] = data_row                       # now PERMENTALY update the ml_ingest record @ index = id
                return uhint, thint, self.this_article_url                   # VIDEO story with Minimal text article
            else:
                logging.info ( f"%s - Depth: 2.2 / ERROR failed to interpret [Video report] page" % cmi_debug )
                logging.info ( f"%s - Depth: 2.2 / confidence level / u: {uhint} h: {thint}" % cmi_debug )
                data_row.update({"viable": 0})                       # cab not extra text data from this article
                self.ml_ingest[idx] = data_row                       # now PERMENTALY update the ml_ingest record @ index = id
                return uhint, 9.9, self.this_article_url                   # VIDEO story with Minimal text article

        if uhint == 3:
            logging.info ( f"%s - Depth: 2.2 / External publication - CANT interpret remote article @ [Depth 2]" % cmi_debug )
            logging.info ( f"%s - Depth: 2.2 / confidence level / u: {uhint} h: {thint}" % cmi_debug )
            data_row.update({"viable": 0})                       # cab no extra text data from this article
            self.ml_ingest[idx] = data_row                       # now PERMENTALY update the ml_ingest record @ index = id
            return uhint, thint, self.this_article_url           # Explicit remote article - can't interpret off-site article

        if uhint == 4:
            logging.info ( f"%s - Depth: 2.2 / POSSIBLE Research report " % cmi_debug )
            logging.info ( f"%s - Depth: 2.2 / confidence level / u: {uhint} h: {thint}" % cmi_debug )
            data_row.update({"viable": 0})                       # cab not extra text data from this article
            self.ml_ingest[idx] = data_row                       # now PERMENTALY update the ml_ingest record @ index = id
            return uhint, thint, self.this_article_url                      # Explicit remote article - can't see into this off-site article

        logging.info ( f"%s - Depth: 2.10 / ERROR NO page interpreter logic triggered" % cmi_debug )
        data_row.update({"viable": 0})                       # cab not extra text data from this article
        self.ml_ingest[idx] = data_row                       # now PERMENTALY update the ml_ingest record @ index = id
        return 10, 10.0, "ERROR_unknown_state!"              # error unknown state

###################################### 12 ###########################################

    def extract_article_data(self, item_idx, sentiment_ai):
        """
        Depth 3:
        Only do this once the article has been evaluated and we know exactly where/what each article is
        Any article we read, should have its resp & BS4 objects cached in yfn_jsdb{}
        Set the Body Data zone, the <p> TAG zone
        Extract all of the full article raw text
        Store it in a Database
        Associate it to themetadata info for this article
        Its now available for the LLM to read and process
        """

        cmi_debug = __name__+"::"+self.extract_article_data.__name__+".#"+str(self.yti)
        logging.info( f'%s - IN / Work on item... [ {item_idx} ]' % cmi_debug )

        
        data_row = self.ml_ingest[item_idx]
        symbol = data_row['symbol']
        cached_state = data_row['urlhash']
        if 'exturl' in data_row.keys():
            durl = data_row['exturl']
            external = True     # not a local yahoo.com hosted article
        else:
            durl = data_row['url']
            external = False

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
            self.yfn_jsdb[cached_state]
            cx_soup = self.yfn_jsdb[cached_state]
            logging.info( f'%s - Found cahce entry / Render data from cache...' % cmi_debug )
            #cx_soup.html.render()           # since we dont cache the raw data, we need to render the page again
            self.yfn_jsdata = cx_soup.text   # store the rendered raw data
            dataset_1 = self.yfn_jsdata
            logging.info( f'%s - Cached object    : {cached_state}' % cmi_debug )
            logging.info( f'%s - Cache req/get    : {type(cx_soup)}' % cmi_debug )
            logging.info( f'%s - Cahce Dataset    : {type(dataset_1)}' % cmi_debug )
            logging.info( f'%s - Cache URL object : {cx_soup.url}' % cmi_debug )
            self.nsoup = BeautifulSoup(escape(dataset_1), "html.parser")
        except KeyError:
            logging.info( f'%s - MISSING from cache / must read page' % cmi_debug )
            logging.info( f'%s - Cache URL object  : {type(durl)}' % cmi_debug )
 
            cmi_debug = __name__+"::"+self.extract_article_data.__name__+".#"+str(item_idx)+" - "+durl
            logging.info( f'%s' % cmi_debug )     # hack fix for urls containg "%" break logging module (NO FIX
            cmi_debug = __name__+"::"+self.extract_article_data.__name__+".#"+str(item_idx)
 
            self.yfqnews_url = durl
            ip_urlp = urlparse(durl)
            ip_headers = ip_urlp.path
            self.init_dummy_session(durl)
            self.update_headers(ip_headers)

            #xhash = self.do_js_get(item_idx)
            cy_soup = self.yfn_jsdb[cached_state]     # get() response 
            xhash = self.do_simple_get(durl)          # non JS Basic HTML get()
            #self.yfqnews_url = url                   # ""   ""

            logging.info( f'%s - Retry cache lookup for: {cached_state}' % cmi_debug )
            if self.yfn_jsdb[cached_state]:
                logging.info( f'%s - Found cache entry: {cached_state}' % cmi_debug )
                #cy_soup.html.render()                  # disbale JS render()
                self.yfn_jsdata = cy_soup.text
                #dataset_2 = self.yfn_jsdata            # Javascript engine render()...slow
                dataset_2 = self.yfn_htmldata           # Basic HTML engine  get()
                logging.info ( f'%s - Cache url:     {cy_soup.url}' % cmi_debug )
                logging.info ( f'%s - Cache req/get: {type(cy_soup)}' % cmi_debug )
                logging.info ( f'%s - Cache Dataset: {type(self.yfn_jsdata)}' % cmi_debug )
                self.nsoup = BeautifulSoup(escape(dataset_2), "html.parser")
            else:
                logging.info( f'%s - FAIL to set BS4 data !' % cmi_debug )
                return 10, 10.0, "ERROR_unknown_state!"

        logging.info( f'%s - Extract ML TEXT dataset: {durl}' % (cmi_debug) )
        if external is True:    # page is Micro stub Fake news article
            logging.info( f'%s - Skipping Micro article stub... [ {item_idx} ]' % cmi_debug )
            return
            # Do not do deep data extraction
            # just use the CAPTION Teaser text from the YFN local url
            # we extracted that in interpret_page()
        else:
            logging.info( f'%s - set BS4 data zones for article: [ {item_idx} ]' % cmi_debug )
            #local_news = self.nsoup.find(attrs={"class": "body yf-tsvcyu"})             # full news article - locally hosted
            local_news = self.nsoup.find(attrs={"class": "body yf-1ir6o1g"})             # full news article - locally hosted
            local_news_meta = self.nsoup.find(attrs={"class": "main yf-cfn520"})        # comes above/before article
            # local_stub_news = self.nsoup.find_all(attrs={"class": "article yf-l7apfj"})
            local_stub_news = self.nsoup.find_all(attrs={"class": "body yf-3qln1o"})   # full news article - locally hosted
            local_stub_news_p = local_news.find_all("p")    # BS4 all <p> zones (not just 1)

            ####################################################################
            ##### M/L Gen AI NLP starts here !!!                         #######
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

###################################### 13 ###########################################

    def dump_ml_ingest(self):        # >>Xray DEBUG<<
        """
        Dump the contents of ml_ingest{}, which holds the NLP candidates
        """
        cmi_debug = __name__+"::"+self.dump_ml_ingest.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        print ( "================= ML Ingested Depth 1 NLP candidates ==================" )
        for k, d in self.ml_ingest.items():
            print ( f"{k:03} {d['symbol']:.5} / {d['urlhash']} Hints: t:{d['type']} u:{d['uhint']} h:{d['thint']}]" )
            if 'exturl' in d.keys():
                print ( f"          Local:    {d['url']}" )
                print ( f"          External: {d['exturl']}" )
            else:
                print ( f"          Local:    {d['url']}" )

        return
