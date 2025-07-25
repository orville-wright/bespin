#! python3
from requests_html import HTMLSession
from urllib.parse import urlparse
from datetime import datetime, date
import logging
import argparse
from rich import print

from ml_yahoofinews import yfnews_reader
from ml_urlhinter import url_hinter
from y_topgainers import y_topgainers
from y_cookiemonster import y_cookiemonster

# ML / NLP section #############################################################
class ml_nlpreader:
    """
    Class to identify, rank, classify stocks NEWS articles
    """

    # global accessors
    args = []               # class dict to hold global args being passed in from main() methods
    ml_yfn_dataset = None   # Yahoo Finance News reader instance
    yfn_uh = None           # URL Hinter instance for the YFN reader
    yti = 0
    cycle = 0               # class thread loop counter

    def __init__(self, yti, global_args):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s   - Instantiate.#{yti}' % cmi_debug )
        self.args = global_args                                    # Only set once per INIT. all methods are set globally
        self.yti = yti
        self.yfn = yfnews_reader(1, "DUMMY0", global_args )        # instantiate a class of fyn with dummy info
        return

########################################## 1 #############################################
# method 1
    def nlp_read_all(self, global_args):
        """
        The machine will read now!
        Read finance.yahoo.com / News 'Brief headlines' (i.e. short text docs) 
        Reads new for ALL stock symbols in the Top Gainers DF table.
        """

        self.args = global_args
        if self.args['bool_news'] is True:                   # read ALL news for top 10 gainers
            cmi_debug = __name__+"::"+self.nlp_read_all.__name__
            logging.info( f'%s   - Instantiate.#{self.yti}' % cmi_debug )
            print ( " " )
            print ( "========================= ML (NLP) / Yahoo Finance News Sentiment AI =========================" )
            print ( f"Build NLP test dataset / for Top Gainers..." )
            newsai_test_dataset = y_topgainers(2)       # instantiate class
            newsai_test_dataset.get_topg_data()         # extract data from finance.Yahoo.com
            nx = newsai_test_dataset.build_tg_df0()     # build entire dataframe
            newsai_test_dataset.build_top10()           # build top 10 gainers
            print ( " " )
            yfn = yfnews_reader(1, "DUMMY1", self.args )   # dummy symbol just for instantiation
            yfn.init_dummy_session('https://www.finance.yahoo.com')
            uh = url_hinter(2, self.args)               # anyone needs to be able to get hints on a URL from anywhere
            yfn.share_hinter(uh)                        # share the url hinter available
            print ( "============================== Prepare bulk NLP candidate list =================================" )
            print ( f"ML/NLP candidates: {newsai_test_dataset.tg_df1['Symbol'].tolist()}" )
            for nlp_target in newsai_test_dataset.tg_df1['Symbol'].tolist():
                yfn.update_headers(nlp_target)
                yfn.form_endpoint(nlp_target)
                yfn.do_simple_get()
                yfn.scan_news_feed(nlp_target, 0, 1, 0)    #depth = 0, redner = Javascript render engine
                yfn.eval_article_tags(nlp_target)          # ml_ingest{} is built
                print ( "============================== NLP candidates are ready =================================" )

            self.nlp_summary(2)
            print ( f" " )
            print ( " " )
            print ( "========================= Tech Events performance Sentiment =========================" )

        return

########################################## 2 #############################################
# method #2
    def nlp_read_one(self, news_symbol, global_args):
        """
        The machine will now read !
        Read finance.yahoo.com / News 'Brief headlines' (i.e. short text docs) 
        Reads ALL news artivles for only ONE stock symbol.
        """
        print ( " " )
        print ( f"ML (NLP) / News Sentiment for 1 symbol [ {news_symbol} ]" )
        self.args = global_args
        cmi_debug = __name__+"::"+self.nlp_read_one.__name__
        logging.info( f'%s   - IN.#{self.yti}' % cmi_debug )
        news_symbol = str(self.args['newsai_sent'])       # symbol provided on CMDLine
        
        ml_yfn_reader = y_cookiemonster(3)
        ml_yfn_dataset = yfnews_reader(1, news_symbol, self.args )      # yfnews_reader is class in ml_yahoofinnews.py
        ml_yfn_dataset.init_dummy_session()       # setup cookie jar and headers
        #self.yfn = yfnews_reader(1, news_symbol, self.args )  # create instance of YFN News reader
        #self.yfn.init_dummy_session('finance.yahoo.com/markets/stocks/most-active/')
        
        # old : hpath = '/quote/' + news_symbol + '/news?p=' + news_symbol
        ml_yfn_dataset.form_endpoint(news_symbol)
        hpath = 'finance.yahoo.com/quote/' + news_symbol + '/news/'
        ml_yfn_dataset.ext_req = ml_yfn_reader.get_js_data(hpath)   # returns HTMLSession() resp object        
        logging.info( f"%s - ext_req: {type(ml_yfn_dataset.ext_req)} : {ml_yfn_dataset.ext_req.url}" % cmi_debug )
        
        #self.yfn.update_headers(hpath)
        #self.yfn.update_cookies()
        self.ml_yfn_dataset = ml_yfn_dataset                # set global access to the YFN News reader instance
        #
        # 2 options to render JS page data
        # performance testing between pyppeteer and playwright
        #
        hash_state = ml_yfn_dataset.ext_do_js_get(0)        # get() & process the page JS data using pyppeteer
        #hash_state = ml_yfn_dataset.ext_pw_js_get(0)        # get() & process the page JS data using plasywright
                
        logging.info( f"%s - globalize url_hinter: [ 1 ]" % cmi_debug )
        self.yfn_uh = url_hinter(1, self.args)              # create instance of urh hinter
        ml_yfn_dataset.yfn_uh = self.yfn_uh                 # share the url hinter with the YFN reader instance
        
               
        # args: symbol | Depth | html/JS | data_page_index | page cache hash_id
        ml_yfn_dataset.scan_news_feed(news_symbol, 0, 1, 0, hash_state)   
        ml_yfn_dataset.eval_news_feed_stories(news_symbol) # ml_ingest{} get built here
        print ( f" " )

        return

####################################### 3 ##########################################
# method 3
    def nlp_summary(self, yti, ml_idx):
        """
        **CRTIICAL: Assumes ml_ingest has already been pre-populated
        Reads 1 item from the ml_ingest{} and processes it...
        """

        self.yti = yti
        cmi_debug = __name__+"::"+self.nlp_summary.__name__+".#"+str(self.yti)
        logging.info( f'%s - IN.#{yti}' % cmi_debug )
        logging.info('%s - ext get request pre-processed by cookiemonster...' % cmi_debug )
        locality_code = {
                    0: 'Local 0',
                    1: 'Local 1',
                    2: 'Local 2',
                    3: 'Remote',
                    9: 'Unknown locality'
                    }

        print ( " ")

        sn_row = self.ml_yfn_dataset.ml_ingest[ml_idx]          # select the row from the ml_ingest{} dict
        if sn_row['type'] == 0:                                 # REAL valid news article, inferred from Depth 0
            print( f"{sn_row['symbol']} / Valid News article: {ml_idx}" )
            t_url = urlparse(sn_row['url'])                                 # WARN: a rlparse() url_named_tupple (NOT the raw url)
            uhint, uhdescr = self.yfn_uh.uhinter(0, t_url)
            thint = (sn_row['thint'])                                       # the hint we guessed at while interrogating page <tags>
            logging.info ( f"%s       - Logic.#0 Hints for url: [ t:0 / u:{uhint} / h: {thint} ] / {uhdescr}" % cmi_debug )

            # WARNING : Do deep M/L AI analysis on the page
            # Heavy compute
            r_uhint, r_thint, r_xturl = self.ml_yfn_dataset.interpret_page(ml_idx, sn_row)    # go deep, with everything we knonw about this item
            logging.info ( f"%s       - Inferr conf: {r_xturl}" % cmi_debug )
            p_r_xturl = urlparse(r_xturl)
            inf_type = self.yfn_uh.confidence_lvl(thint)                  # returned var is a tupple - (descr, locality code)
            #
            print ( f"Article type:  [ 0 / {sn_row['url']} ]" )                # all type 0 are assumed to be REAL news
            print ( f"Origin URL:    [ {t_url.netloc} ] / {uhdescr} / {inf_type[0]} / ", end="" )
            print ( f"{locality_code.get(inf_type[1])}" )
            uhint, uhdescr = self.yfn_uh.uhinter(21, p_r_xturl)
            print ( f"Target URL:    [ {p_r_xturl.netloc} ] / {uhdescr} / ", end="" )
            print ( f"{locality_code.get(uhint)} [ u:{uhint} ]" )
            return thint    # what this artuicle actuall;y is

        # Fake New Micro-Ad, but could possibly be news...
        elif sn_row['type'] == 1:
            print( f"{sn_row['symbol']} / Fake News article - Micro-add: {ml_idx} - AI will not eval sentiment" )
            t_url = urlparse(sn_row['url'])
            uhint, uhdescr = self.yfn_uh.uhinter(1, t_url)      # hint on ORIGIN url
            thint = (sn_row['thint'])                   # the hint we guess at while interrogating page <tags>
            logging.info ( f"%s       - Logic.#1 hint origin url: t:1 / u:{uhint} / h: {thint} {uhdescr}" % cmi_debug )

            # WARN: Do deep M/L AI page analysis, with everything we know about this item
            r_uhint, r_thint, r_xturl = self.ml_yfn_dataset.interpret_page(ml_idx, sn_row)    
            logging.info ( f"%s       - Logic.#1 hint ext url: {r_xturl}" % cmi_debug )
            p_r_xturl = urlparse(r_xturl)
            inf_type = self.yfn_uh.confidence_lvl(thint)

            # summary report...
            print ( f"Article type:  [ 1 / {sn_row['url']} ]" )                # all type 0 are assumed to be REAL news
            print ( f"Origin:  [ {t_url.netloc} ] / {uhdescr} / {inf_type[0]} / ", end="" )
            print ( f"{locality_code.get(inf_type[1], 'in flux')}" )
            uhint, uhdescr = self.yfn_uh.uhinter(31, p_r_xturl)      # hint on TARGET url
            print ( f"Hints:   {uhdescr} / ", end="" )
            print ( f"{locality_code.get(uhint, 'in flux')} [ u:{uhint} ]" )
            logging.info ( f"%s - skipping..." % cmi_debug )
            return thint
        
        # Video story. Prob no real news text
        elif sn_row['type'] == 2:
            print( f"{sn_row['symbol']} / Video article - Not readable: {ml_idx} - AI will not eval sentiment" )
            t_url = urlparse(sn_row['url'])
            thint = (sn_row['thint'])
            inf_type = self.yfn_uh.confidence_lvl(thint)
            print ( f"Article: {ml_idx} - {inf_type[0]}: 2 - NOT an NLP candidate" )
            print ( f"URL:     {sn_row['url']}" )
            print ( f"Origin:  [ {t_url.netloc} ] / {inf_type[0]} / ", end="" )
            print ( f"{locality_code.get(inf_type[1], 'in flux')}" )
            logging.info ( f"%s - skipping..." % cmi_debug )
            return thint
        
        elif sn_row['type'] == 5:                     # possibly not news, Yahoo Premium subscription add
            thint = (sn_row['thint'])
            inf_type = self.yfn_uh.uhinter.confidence_lvl(thint)
            print ( f"Article: {ml_idx} - {inf_type[0]}: 5 - NOT an NLP candidate" )
            logging.info ( f"%s - skipping..." % cmi_debug )
            thint = (sn_row['thint'])
            return thint
        
        elif sn_row['type'] == 9:                     # possibly not news? (Micro Ad)
            print ( f"Article: {ml_idx} - Type 9 - NOT yet defined - NOT an NLP candidate" )
            logging.info ( f"%s - skipping..." % cmi_debug )
            thint = (sn_row['thint'])
            return thint
        
        else:
            print ( f"Article: {ml_idx} - ERROR unknown article type" )
            logging.info ( f"%s - #? skipping..." % cmi_debug )
            thint = (sn_row['thint'])

        return thint
