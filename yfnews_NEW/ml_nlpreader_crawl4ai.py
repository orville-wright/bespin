#! python3
import asyncio
import logging
import argparse
from urllib.parse import urlparse
from datetime import datetime, date
from rich import print

from ml_yahoofinews_crawl4ai import yfnews_reader
from ml_urlhinter import url_hinter
from ml_sentiment import ml_sentiment

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
    ml_yfn_dataset = None   # Yahoo Finance News reader instance
    yfn_uh = None           # URL Hinter instance for the YFN reader
    yti = 0
    cycle = 0               # class thread loop counter

    def __init__(self, yti, global_args):
        cmi_debug = __name__+"::" + self.__init__.__name__
        logging.info(f'%s   - Instantiate.#{yti}' % cmi_debug)
        self.args = global_args
        self.yti = yti
        return

    # ##############################################################################
    async def async_nlp_read_one(self, news_symbol, global_args):
        """
        Async version of nlp_read_one that uses crawl4ai
        The machine will now read!
        Read finance.yahoo.com / News 'Brief headlines' using crawl4ai
        Reads ALL news articles for only ONE stock symbol.
        """
        print(" ")
        print(f"ML (NLP) / News Sentiment for 1 symbol [ {news_symbol} ]")
        self.args = global_args
        cmi_debug = __name__+"::" + self.async_nlp_read_one.__name__
        logging.info(f'%s   - IN.#{self.yti}' % cmi_debug)
        
        news_symbol = str(news_symbol).upper()
        ml_yfn_dataset = yfnews_reader(1, news_symbol, self.args)
        ml_yfn_dataset.form_endpoint(news_symbol)
        logging.info(f"%s - globalize url_hinter: [ 1 ]" % cmi_debug)
        self.yfn_uh = url_hinter(1, self.args)
        ml_yfn_dataset.yfn_uh = self.yfn_uh
        hash_state = await ml_yfn_dataset.crawl4ai_extract_news(0)
        
        if hash_state:
            # Process the extracted news data
            ml_yfn_dataset.scan_news_feed(news_symbol, 0, 1, 0, hash_state)
            ml_yfn_dataset.eval_news_feed_stories(news_symbol)
            
            # Store the dataset for further processing
            self.ml_yfn_dataset = ml_yfn_dataset
            
            print(f" ")
            print(f"Successfully processed {len(ml_yfn_dataset.ml_ingest)} articles")
            
            # Debug: dump ML ingest data
            if self.args.get('bool_xray', False):
                ml_yfn_dataset.dump_ml_ingest()
        else:
            logging.error(f"%s - Failed to extract news data" % cmi_debug)
        
        return


    # ##############################################################################
    def nlp_read_one(self, news_symbol, global_args):
        """
        Sync wrapper for async nlp_read_one
        """
        return asyncio.run(self.async_nlp_read_one(news_symbol, global_args))

    # ##############################################################################
    def nlp_summary(self, yti, ml_idx):
        """
        **CRITICAL: Assumes ml_ingest has already been pre-populated
        Reads 1 item from the ml_ingest{} and processes it...
        Updated to work with crawl4ai extracted data
        """
        self.yti = yti
        cmi_debug = __name__+"::" + self.nlp_summary.__name__+".#"+str(self.yti)
        logging.info(f'%s - IN.#{yti}' % cmi_debug)
        
        locality_code = {
            0: 'Local 0',
            1: 'Local 1', 
            2: 'Local 2',
            3: 'Remote url 3',
            4: 'Local video 4',
            9: 'Unknown locality'
        }

        print(" ")

        if not self.ml_yfn_dataset or ml_idx not in self.ml_yfn_dataset.ml_ingest:
            logging.error(f'%s - No data found for index: {ml_idx}' % cmi_debug)
            return 9.9

        sn_row = self.ml_yfn_dataset.ml_ingest[ml_idx]
        
        if sn_row['type'] == 0:  # REAL valid news article
            t_url = urlparse(sn_row['url'])
            uhint, uhdescr = self.yfn_uh.uhinter(0, t_url)
            thint = sn_row['thint']
            print(f"{sn_row['symbol']} / Valid News article: {ml_idx}")
            logging.info(f"%s       - Logic.#0 Hints for url: [ t:0 / u:{uhint} / h: {thint} ] / {uhdescr}" % cmi_debug)
            
            # Do deep analysis on the page
            r_uhint, r_thint, r_xturl = self.ml_yfn_dataset.interpret_page(ml_idx, sn_row)
            logging.info(f"%s       - Inferred conf: {r_xturl}" % cmi_debug)
            p_r_xturl = urlparse(r_xturl)
            inf_type = self.yfn_uh.confidence_lvl(thint)
            print(f"Article type:  [ 0 / {sn_row['url']} ]")
            print(f"Origin URL:    [ {t_url.netloc} ] / {uhdescr} / {inf_type[0]} / ", end="")
            print(f"{locality_code.get(inf_type[1])}")
            uhint, uhdescr = self.yfn_uh.uhinter(21, p_r_xturl)
            print(f"Target URL:    [ {p_r_xturl.netloc} ] / {uhdescr} / ", end="")
            print(f"{locality_code.get(uhint)} [ u:{uhint} ]")
            return thint
            
        elif sn_row['type'] == 1:  # Fake News Micro-Ad
            t_url = urlparse(sn_row['url'])
            uhint, uhdescr = self.yfn_uh.uhinter(1, t_url)
            thint = sn_row['thint']
            print(f"{sn_row['symbol']} / Fake News article - Micro-ad: {ml_idx} - AI will not Read sentiment")
            logging.info(f"%s       - Logic.#1 hint origin url: t:1 / u:{uhint} / h: {thint} {uhdescr}" % cmi_debug)
            
            r_uhint, r_thint, r_xturl = self.ml_yfn_dataset.interpret_page(ml_idx, sn_row)
            try:
                url_test = len(r_xturl)
                p_r_xturl = urlparse(r_xturl)
                inf_type = self.yfn_uh.confidence_lvl(thint)
                logging.info(f"%s       - Logic.#1 hint ext url: {r_xturl}" % cmi_debug)
                print(f"Article type:  [ +1 / {sn_row['url']} ]")
                print(f"Origin:        [ {t_url.netloc} ] / {uhdescr} / {inf_type[0]} / ", end="")
                print(f"{locality_code.get(inf_type[1], 'in flux')}")
                uhint, uhdescr = self.yfn_uh.uhinter(31, p_r_xturl)
                print(f"Hints:         [ {uhdescr} / ", end="")
                print(f"{locality_code.get(uhint, 'in flux')} [ u:{uhint} ]")
                logging.info(f"%s - skipping..." % cmi_debug)
                return thint
            except Exception as e:
                logging.info(f"%s       - BAD artile URL {url_test} : {e}" % cmi_debug)
                return thint

        elif sn_row['type'] == 2:  # Video story - this is an initial guess. It culd be wring. 
            t_url = urlparse(sn_row['url'])
            thint = sn_row['thint']
            inf_type = self.yfn_uh.confidence_lvl(thint)
            print(f"{sn_row['symbol']} / {inf_type[0]}: {ml_idx} - Not readable AI will not eval sentiment")
            print(f"Article:   [ +{inf_type[1]} / {inf_type[0]}  - NOT an NLP candidate")
            print(f"URL:       [ {sn_row['url']}")
            print(f"Origin:    [ {t_url.netloc} ] / {inf_type[0]} / ", end="")
            print(f"{locality_code.get(inf_type[1], 'in flux')}")
            logging.info(f"%s - skipping..." % cmi_debug)
            return thint
            
        elif sn_row['type'] == 3:  # Injected add link
            t_url = urlparse(sn_row['url'])
            thint = sn_row['thint']
            inf_type = self.yfn_uh.confidence_lvl(thint)
            print(f"{sn_row['symbol']} / {inf_type[0]}: {ml_idx} - AI will not Read sentiment")
            print(f"Article:   [ +{inf_type[1]} / {inf_type[0]} - AI will not Read sentiment")
            print(f"URL:       [ {sn_row['url']}")
            print(f"Origin:    [ {t_url.netloc} ] / {inf_type[0]} / ", end="")
            print(f"{locality_code.get(inf_type[1], 'in flux')}")
            logging.info(f"%s - skipping..." % cmi_debug)
            return thint

        elif sn_row['type'] == 4:  # Video story
            t_url = urlparse(sn_row['url'])
            thint = sn_row['thint']
            inf_type = self.yfn_uh.confidence_lvl(thint)
            print(f"{sn_row['symbol']} / Video 4 article - Not readable: {ml_idx} - AI will not eval sentiment")
            print(f"Article: {ml_idx} - {inf_type[0]}: 2 - NOT an NLP candidate")
            print(f"URL:     {sn_row['url']}")
            print(f"Origin:  [ {t_url.netloc} ] / {inf_type[0]} / ", end="")
            print(f"{locality_code.get(inf_type[1], 'in flux')}")
            logging.info(f"%s - skipping..." % cmi_debug)
            return thint

        elif sn_row['type'] == 5:  # Yahoo Premium subscription ad
            thint = sn_row['thint']
            inf_type = self.yfn_uh.confidence_lvl(thint)
            print(f"Article: {ml_idx} - {inf_type[0]}: 5 - NOT an NLP candidate")
            logging.info(f"%s - skipping..." % cmi_debug)
            return thint
            
        elif sn_row['type'] == 9:  # Not yet defined
            print(f"Article: {ml_idx} - Type 9 - NOT yet defined - NOT an NLP candidate")
            logging.info(f"%s - skipping..." % cmi_debug)
            thint = sn_row['thint']
            return thint
            
        else:
            print(f"Article: {ml_idx} - ERROR BAD Data | unknown article type: {sn_row['type']}")
            logging.info(f"%s - #? skipping..." % cmi_debug)
            thint = sn_row['thint']
            return thint

    # ####################################################################################

    def process_sentiment_analysis(self, target_symbols=None):
        """
        Process sentiment analysis for articles in ml_ingest
        """
        cmi_debug = __name__+"::" + self.process_sentiment_analysis.__name__
        logging.info(f'%s - Processing sentiment analysis' % cmi_debug)
        
        if not self.ml_yfn_dataset or not self.ml_yfn_dataset.ml_ingest:
            logging.error(f'%s - No ML ingest data available' % cmi_debug)
            return
        
        #################################################################`
        # AI M/L NLP reader
        # Initialize sentiment analyzer
        sentiment_ai = ml_sentiment(self.yti, self.args)
        
        results = {}
        
        for item_idx, data_row in self.ml_yfn_dataset.ml_ingest.items():
            if data_row.get('viable', 0) == 1:  # Only process viable articles
                symbol = data_row['symbol']
                if target_symbols and symbol not in target_symbols:               # WTF is this for?
                    continue
                
                logging.info(f'%s - Compute sentiment for: {item_idx} / {symbol}' % cmi_debug)
                try:
                    tokens, words, scent = self.ml_yfn_dataset.extract_article_data(item_idx, sentiment_ai)
                    if symbol not in results:
                        results[symbol] = {
                            'articles_processed': 0,
                            'total_tokens': 0,
                            'total_words': 0,
                            'sentiment_counts': {'positive': 0, 'negative': 0, 'neutral': 0}
                        }
                    
                    results[symbol]['articles_processed'] += 1
                    results[symbol]['total_tokens'] += tokens
                    results[symbol]['total_words'] += words
                    
                    # Aggregate sentiment counts
                    for sentiment_type in ['positive', 'negative', 'neutral']:
                        results[symbol]['sentiment_counts'][sentiment_type] += sentiment_ai.sentiment_count[sentiment_type]
                        
                except Exception as e:
                    logging.error(f'%s - Error processing article {item_idx}: {e}' % cmi_debug)
                    continue
        
        # Print results summary
        print("\n" + "="*80)
        print("SENTIMENT ANALYSIS RESULTS")
        print("="*80)
        
        for symbol, data in results.items():
            print(f"\nSymbol: {symbol}")
            print(f"Articles processed: {data['articles_processed']}")
            print(f"Total tokens: {data['total_tokens']}")
            print(f"Total words: {data['total_words']}")
            print(f"Sentiment distribution:")
            print(f"  Positive: {data['sentiment_counts']['positive']}")
            print(f"  Negative: {data['sentiment_counts']['negative']}")
            print(f"  Neutral: {data['sentiment_counts']['neutral']}")
            
            total_sentiment = sum(data['sentiment_counts'].values())
            if total_sentiment > 0:
                pos_pct = (data['sentiment_counts']['positive'] / total_sentiment) * 100
                neg_pct = (data['sentiment_counts']['negative'] / total_sentiment) * 100
                neu_pct = (data['sentiment_counts']['neutral'] / total_sentiment) * 100
                
                print(f"  Positive: {pos_pct:.1f}%")
                print(f"  Negative: {neg_pct:.1f}%")
                print(f"  Neutral: {neu_pct:.1f}%")
                
        return results

    # #####################################################################################
    def run_full_analysis(self, symbol):
        """
        Run complete analysis workflow for a symbol
        """
        cmi_debug = __name__+"::" + self.run_full_analysis.__name__
        logging.info(f'%s - Running full analysis for: {symbol}' % cmi_debug)
        
        # Step 1: Extract news data
        self.nlp_read_one(symbol, self.args)
        
        # Step 2: Process sentiment analysis
        results = self.process_sentiment_analysis([symbol])
        
        # Step 3: Generate summary
        if self.ml_yfn_dataset and self.ml_yfn_dataset.ml_ingest:
            print(f"\n" + "="*60)
            print(f"DETAILED ARTICLE ANALYSIS FOR {symbol}")
            print("="*60)
            
            for idx in self.ml_yfn_dataset.ml_ingest.keys():
                self.nlp_summary(self.yti, idx)
                print("-" * 40)
        
        return results
