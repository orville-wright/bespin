#! python3

import argparse
import asyncio
from datetime import datetime, date
import logging
from ml_yf_news_c4 import yfnews_reader
from ml_urlhinter import url_hinter
from ml_sentiment import ml_sentiment
from urllib.parse import urlparse
from rich import print

# logging setup
logging.basicConfig(level=logging.INFO)

# ML / NLP section #############################################################
class ml_nlpreader:
    """
    Class to identify, rank, classify stocks NEWS articles
    Updated to work with crawl4ai version of Yahoo Finance scraper
    """

    # global accessors
    args = []               # class dict to hold global args being passed in from main() methods
    cycle = 0               # class thread loop counter
    ml_yfn_dataset = None   # Yahoo Finance News reader instance
    yfn = None              # class of @ml_yahoofinews_crawl4ai.py/yfnews_reader
    yfn_uh = None           # URL Hinter instance for the YFN reader
    yti = 0

    def __init__(self, yti, global_args):
        cmi_debug = __name__+"::" + self.__init__.__name__
        logging.info(f'%s   - Instantiate.#{yti}' % cmi_debug)
        self.yfn = yfnews_reader(1, "DUMMY0", global_args )        # instantiate a class of fyn frpm @ ml_yahoofinews_craw4ai
        self.args = global_args
        self.yti = yti
        return

    # ##############################################################################
    async def nlp_read_one(self, news_symbol, global_args):
        """
        DEPTH -> 0 and -> 1
        Main controler for depth 0 and 0 data extraction

        Async version of nlp_read_one that uses crawl4ai
        The machine will now read!
        Read finance.yahoo.com / News 'Brief headlines' using crawl4ai
        Reads ALL news articles for only ONE stock symbol (why this method is title "read_one").
        """
        print(" ")
        print(f"ML (NLP) / News Sentiment for 1 symbol [ {news_symbol} ]")
        self.args = global_args
        cmi_debug = __name__+"::" + self.nlp_read_one.__name__
        logging.info(f'%s   - IN.#{self.yti}' % cmi_debug)
        news_symbol = str(news_symbol).upper()
        
        ml_yfn_dataset = yfnews_reader(1, news_symbol, self.args)       # Instantiate
        ml_yfn_dataset.form_endpoint(news_symbol)                       # extablish the exct news url endpoint
        logging.info(f"%s - globalize url_hinter @ #1" % cmi_debug)
        self.yfn_uh = url_hinter(1, self.args)                          # instantiate URL hinter 
        ml_yfn_dataset.yfn_uh = self.yfn_uh

        # 3 Main steps execuete here - Depth -> 0 + Depth -> 1
        # print a report of the Depth 0 Top Level skim run
        hash_state = await ml_yfn_dataset.yahoofin_news_depth0(0)   # scrape NOW @ Depth 0 yahoofin_news_depth0()

        if hash_state:												# Depth: 0
            articles_found = ml_yfn_dataset.list_newsfeed_candidates(news_symbol, 0, 1, hash_state)
            ml_yfn_dataset.eval_news_feed_stories(news_symbol)		# Depth: 1            
            self.ml_yfn_dataset = ml_yfn_dataset                    # set global dataset -> ml_yfn_dataset            
            print(f" ")
            print(f"Successfully skimmed: {articles_found} / Evaled: {len(ml_yfn_dataset.ml_ingest)} articles @ Depth: 0")
            if self.args.get('bool_xray', False):                   # DEBUG: xray
                ml_yfn_dataset.dump_ml_ingest()
        else:
            logging.error(f"%s - No Top lvel artciels were found !!" % cmi_debug)
        
        return articles_found

    # ##############################################################################
    def nlp_summary_report(self, yti, ml_idx):
        """
        CRITICAL: Assumes ml_ingest has already been pre-populated
        NOTE: Reads 1 (ONE) article ONLY from the ml_ingest{} DB and processes it...
              Executes Dept 2 analysis via ml_yfn_dataset::interpret_page()   - no get() or BS4
        
        Needs updating to crawl4ai data extraction (currenlt BS4)
        """
        self.yti = yti
        cmi_debug = __name__+"::" + self.nlp_summary_report.__name__+".#"+str(self.yti)
        logging.info(f'%s - IN.#{yti}' % cmi_debug)
        
        locality_code = {
            0: 'Local 0',
            1: 'Local 1', 
            2: 'Local 2',
            3: 'Remote',
            9: 'Unknown locality'
        }

        print(" ")

        if not self.ml_yfn_dataset or ml_idx not in self.ml_yfn_dataset.ml_ingest:
            logging.error(f'%s - No data found for index: {ml_idx}' % cmi_debug)
            return 9.9

        sn_row = self.ml_yfn_dataset.ml_ingest[ml_idx]
        
        # ################# 1: Real valid news article
        if sn_row['type'] == 0:  # REAL valid news article
            print(f"{sn_row['symbol']} / Valid News article: {ml_idx}")
            t_url = urlparse(sn_row['url'])
            uhint, uhdescr = self.yfn_uh.uhinter(0, t_url)
            thint = sn_row['thint']
            logging.info(f"%s       - Logic.#0 Hints for url: [ t:0 / u:{uhint} / h: {thint} ] / {uhdescr}" % cmi_debug)
            
            # Do deep analysis on the page @ Depth 2
            r_uhint, r_thint, r_xturl = self.ml_yfn_dataset.interpret_page(ml_idx, sn_row)
            logging.info(f"%s       - Inferred conf: {r_xturl}" % cmi_debug)
            p_r_xturl = urlparse(r_xturl)
            inf_type = self.yfn_uh.confidence_lvl(thint)
            print(f"Article type:  [ +{uhint} ] / {sn_row['url']}")
            print(f"Origin URL:    [ {t_url.netloc} ] / {inf_type[0]} / ", end="")
            print(f"{locality_code.get(inf_type[1])}")
            uhint, uhdescr = self.yfn_uh.uhinter(10, p_r_xturl)
            print(f"Target URL:    [ {p_r_xturl.netloc} ] / {uhdescr} / ", end="")
            print(f"{locality_code.get(uhint)} [ u:{uhint} ]")
            return thint
        
        # ################# 1: Fake news article - Micro-ad
        elif sn_row['type'] == 1:
            print(f"{sn_row['symbol']} / Fake News article - Micro-ad: {ml_idx} - AI will not eval sentiment")
            t_url = urlparse(sn_row['url'])
            uhint, uhdescr = self.yfn_uh.uhinter(11, t_url)
            thint = sn_row['thint']
            logging.info(f"%s       - Logic.#1 hint origin url: t:1 / u:{uhint} / h: {thint} {uhdescr}" % cmi_debug)
            
            r_uhint, r_thint, r_xturl = self.ml_yfn_dataset.interpret_page(ml_idx, sn_row) # Depth 2 analysis of page
            
            try:
                url_test = len(r_xturl)
                logging.info(f"%s       - Logic.#1 hint ext url: {r_xturl}" % cmi_debug)
                p_r_xturl = urlparse(r_xturl)
                inf_type = self.yfn_uh.confidence_lvl(thint)
                print(f"Article type:  [ +{uhint} ] / {sn_row['url']}")
                print(f"Origin:        [ {t_url.netloc} ] / {inf_type[0]} / {uhdescr} /", end="")
                print(f"{locality_code.get(inf_type[1], 'in flux')}")
                uhint, uhdescr = self.yfn_uh.uhinter(111, p_r_xturl)
                print(f"Hints:         {uhdescr} / ", end="")
                print(f"{locality_code.get(uhint, 'in flux')} [ u:{uhint} ]")
                logging.info(f"%s - skipping..." % cmi_debug)
                return thint
            except Exception as e:
                logging.info(f"%s       - BAD artile URL {url_test} : {e}" % cmi_debug)
                return thint

        # ################# 2: Video story
        elif sn_row['type'] == 2:
            print(f"{sn_row['symbol']} / Video article - Not readable: {ml_idx} - AI will not eval sentiment")
            t_url = urlparse(sn_row['url'])
            thint = sn_row['thint']
            inf_type = self.yfn_uh.confidence_lvl(thint)
            uhint, uhdescr = self.yfn_uh.uhinter(12, t_url)
            print(f"Article type:  [ +{uhint} ] / Video stream cannot be processed by AI model")
            print(f"URL:           {sn_row['url']}")
            print(f"Origin:        [ {t_url.netloc} ] / {inf_type[0]} / ", end="")
            print(f"{locality_code.get(inf_type[1], 'in flux')}")
            logging.info(f"%s - skipping..." % cmi_debug)
            return thint
        
        # ################# 3: External publication
        elif sn_row['type'] == 3:
            print(f"{sn_row['symbol']} / Random Filler item - Not readable: {ml_idx} - AI will not eval sentiment")
            t_url = urlparse(sn_row['url'])
            thint = sn_row['thint']
            inf_type = self.yfn_uh.confidence_lvl(thint)
            uhint, uhdescr = self.yfn_uh.uhinter(13, t_url)
            print(f"Article type:  [ +{uhint} ] / Unreliable external article data")
            print(f"URL:           {sn_row['url']}")
            print(f"Origin:        [ {t_url.netloc} ] / {inf_type[0]} / ", end="")
            print(f"{locality_code.get(inf_type[1], 'in flux')}")
            logging.info(f"%s - skipping..." % cmi_debug)
            return thint

        # ################# 5: Yahoo Premium subscription ad
        elif sn_row['type'] == 5:
            t_url = urlparse(sn_row['url'])
            thint = sn_row['thint']
            inf_type = self.yfn_uh.confidence_lvl(thint)
            uhint, uhdescr = self.yfn_uh.uhinter(13, t_url)
            print(f"Article: {ml_idx} - {inf_type[0]}: 5 - NOT an NLP candidate")
            logging.info(f"%s - skipping..." % cmi_debug)
            return thint
        
        # ################# 9: Placeholder - Not yet defined
        elif sn_row['type'] == 9:
            print(f"Article: {ml_idx} - Type 9 - NOT yet defined - NOT an NLP candidate")
            logging.info(f"%s - skipping..." % cmi_debug)
            thint = sn_row['thint']
            return thint
        
        # ################# + : catchall for Bad data
        else:
            print(f"Article: {ml_idx} - ERROR BAD Data | unknown article type: {sn_row['type']}")
            logging.info(f"%s - #? skipping..." % cmi_debug)
            thint = sn_row['thint']
            return thint

