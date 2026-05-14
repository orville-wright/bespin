#!/usr/bin/env python3

import asyncio
import random
import pandas as pd
#import modin.pandas as pd
import logging
import argparse
import time
import threading
import re
from urllib.parse import urlparse
from rich import print
import pprint


# my private classes & methods
from data_engines_fundamentals.alpaca_md import alpaca_md
from bigcharts_md import bc_quote
from ml_yf_nlp_orchestrator import ml_nlpreader
from ml_sentiment import ml_sentiment
from ml_urlhinter import url_hinter
from nasdaq_uvoljs import un_volumes
from nasdaq_wrangler import nq_wrangler
from nasdaq_quotes import nquote
from shallow_logic import combo_logic
from y_cookiemonster import y_cookiemonster
from y_daylosers import y_daylosers
from y_smallcaps import smallcap_screen
from y_techevents import y_techevents
from y_topgainers import y_topgainers
from datastore_eng_LMDB import lmdb_io_eng

from data_engines_fundamentals.alphavantage_md import alphavantage_md
from neo4j_graphdb import neo4j_auradb
from data_engines_fundamentals.fred_md import fred_md
from data_engines_fundamentals.eodhistoricaldata_md import eodhistoricaldata_md
from data_engines_fundamentals.financialmodelingprep_md import financialmodelingprep_md
from data_engines_fundamentals.finnhub_md import finnhub_md
from data_engines_fundamentals.marketstack_md import marketstack_md
from data_engines_fundamentals.sec_md import sec_md
from data_engines_fundamentals.stockdata_md import stockdata_md
from data_engines_fundamentals.stooq_md import stooq_md
from data_engines_fundamentals.tiingo_md import tiingo_md
from data_engines_fundamentals.twelvedata_md import twelvedata_md

# Data Extractor engines
from data_engines_fundamentals.polygon_md import polygon_md
from data_engines_news.barrons_news import barrons_news
from data_engines_news.benzinga_news import benzinga_news
from data_engines_news.forbes_news import forbes_news
from data_engines_news.fxstreet_news import fxstreet_news
from data_engines_news.investing_news import investing_news
from data_engines_news.hedgeweek_news import hedgeweek_news
from data_engines_news.gurufocus_news import gurufocus_news

# Main() Global attributes
global args
global parser

args = {}
articles_found = 0          # number of articles found by the AI news reader for 1 synble scan run
lmdb_env = {}               # global LMDB KV database (cross classes accessor)  
logging.basicConfig(level=logging.INFO)
uh = url_hinter(1, args)    # everyone needs to be able to get hints on a URL from anywhere
work_inst = 0
yti = 1


parser = argparse.ArgumentParser(prog="Aop", description="Entropy apperture engine")
parser.add_argument('-a','--allnews', help='ML/NLP News sentiment AI for all stocks', action='store_true', dest='bool_news', required=False, default=False)
#
#parser.add_argument('-n','--newsai-sent', help='AI NLP News sentiment AI for 1 stock', action='store', dest='newsai_sent', required=False, default=False)
parser.add_argument('-c','--news-cycle', help='Full news cycle sentiment for evey data engine', action='store_true', dest='news_cycle', required=False, default=False)
parser.add_argument('-n','--newsai-sent', help='AI NLP News sentiment for 1 stock [ticker] [num-of-articles]', nargs="*", dest='newsai_sent', required=False, default=False)
#
parser.add_argument('-p','--perf', help='Tech event performance sentiment', action='store_true', dest='bool_te', required=False, default=False)
parser.add_argument('-q','--quote', help='Get ticker price action quote', action='store', dest='qsymbol', required=False, default=False)
parser.add_argument('-s','--screen', help='Small cap screener logic', action='store_true', dest='bool_scr', required=False, default=False)
parser.add_argument('-t','--tops', help='show top ganers/losers', action='store_true', dest='bool_tops', required=False, default=False)
#
parser.add_argument('-u','--unusual', help='unusual up & down volume', action='store_true', dest='bool_uvol', required=False, default=False)
parser.add_argument('-v','--verbose', help='verbose error logging', action='store_true', dest='bool_verbose', required=False, default=False)
parser.add_argument('-x','--xray', help='dump detailed debug data structures', action='store_true', dest='bool_xray', required=False, default=False)
#

############################# main() ##################################

def main():
    cmi_debug = "aop::"+__name__+"::main()"
    global args
    args = vars(parser.parse_args())        # args as a dict []
    print ( " " )
    print ( "#################### I n i t a l i z i n g ####################" )
    print ( " " )
    #print ( "CMDLine args:", parser.parse_args() )
    if args['bool_verbose'] is True:        # Logging level
        print ( "Enabeling verbose info logging..." )
        logging.disable(0)                  # Log level = OFF
    else:
        logging.disable(20)                 # Log lvel = INFO

    print ( " " )
    recommended = {}        # dict of recomendations

########### 1 - TOP GAINERS ################
    if args['bool_tops'] is True:
        print ( "========== Large Cap / Top Mover by % change ===============================" )
        ## new JS data extractor
        topgainer_reader = y_cookiemonster(1)         # instantiate class of cookiemonster
        mlx_top_dataset = y_topgainers(1)             # instantiate class
        mlx_top_dataset.init_dummy_session()          # setup cookie jar and headers
 
        mlx_top_dataset.ext_req = topgainer_reader.get_js_data('finance.yahoo.com/markets/stocks/most-active/')
        mlx_top_dataset.ext_get_data(1)

        x = mlx_top_dataset.build_tg_df0()     # build full dataframe
        mlx_top_dataset.build_top10()          # show top 10
        mlx_top_dataset.print_top10()          # print it
        print ( " " )

########### 2 - TOP LOSERS ################
        print ( "========== Large Cap / Top Looser by -% change  ================================" )
        ## new JS data extractor
        toploser_reader = y_cookiemonster(2)         # instantiate class of cookiemonster
        mlx_loser_dataset = y_daylosers(1)           # instantiate class
        mlx_loser_dataset.init_dummy_session()       # setup cookie jar and headers
 
        mlx_loser_dataset.ext_req = toploser_reader.get_js_data('finance.yahoo.com/markets/stocks/losers/')
        mlx_loser_dataset.ext_get_data(1)

        x = mlx_loser_dataset.build_tl_df0()     # build full dataframe
        mlx_loser_dataset.build_top10()          # show top 10
        mlx_loser_dataset.print_top10()          # print it
        print ( " " )

########### 3 Generla News Reader ################
# DEV: Adding and testing all the new Market Data enbgines / extractors here
# Notes for: AI coding assistance @claude

    if args['news_cycle'] is True:
        #'''
        ext_count = 0
        barrons_news_reader = barrons_news(1)
        ext_count += asyncio.run(barrons_news_reader.craw4ai_str_schema_extr())
        benzinga_news_reader = benzinga_news(1)
        ext_count += asyncio.run(benzinga_news_reader.craw4ai_str_schema_extr())
        forbes_news_reader = forbes_news(1)
        ext_count += asyncio.run(forbes_news_reader.craw4ai_str_schema_extr())
        fxstreet_news_reader = fxstreet_news(1)
        ext_count += asyncio.run(fxstreet_news_reader.craw4ai_str_schema_extr())
        investing_news_reader = investing_news(1)
        ext_count += asyncio.run(investing_news_reader.craw4ai_str_schema_extr())
        hedgeweek_news_reader = hedgeweek_news(1)
        ext_count += asyncio.run(hedgeweek_news_reader.craw4ai_str_schema_extr())
        #'''
            
        #gurufocus_news_reader = gurufocus_news(1)
        #asyncio.run(gurufocus_news_reader.craw4ai_str_schema_extr())
        
        print (f"Total News articles extracted: {ext_count}" )
        print ( " " )

########### Small Cap gainers & loosers ################
# small caps are isolated outside the regular dataset by yahoo.com
    if args['bool_scr'] is True:
        print ( "========== Small Cap / Top Gainers / +5% with Mkt-cap > $299M ==========" )
        scap_reader = y_cookiemonster(2)             # instantiate class of cookiemonster
        small_cap_dataset = smallcap_screen(1)       # instantiate class of a Small Scap Screener
        small_cap_dataset.init_dummy_session()       # setup cookie jar and headers
 
        small_cap_dataset.ext_req = scap_reader.get_js_data('finance.yahoo.com/research-hub/screener/small_cap_gainers/')
        small_cap_dataset.ext_get_data(1)
        
        x = small_cap_dataset.build_df0()         # build full dataframe
        small_cap_dataset.build_top10()           # show top 10
        small_cap_dataset.print_top10()           # print it

        recommended.update(small_cap_dataset.screener_logic())
        print ( " ")

# process Nasdaq.com unusual_vol ################
    if args['bool_uvol'] is True:
        print ( "========== Unusually high Volume / Up =======================================================" )
        un_vol_activity = un_volumes(1, args)       # instantiate NEW nasdaq data class, args = global var
        un_vol_activity.get_un_vol_data()           # extract JSON data (Up & DOWN) from api.nasdaq.com

        # should test success of extract before attempting DF population
        un_vol_activity.build_df(0)           # 0 = UP Unusual volume
        un_vol_activity.build_df(1)           # 1 = DOWN unusual volume

        # find lowest price stock in unusuall UP volume list
        up_unvols = un_vol_activity.up_unvol_listall()      # temp DF, nicely ordered & indexed of unusual UP vol activity
        ulp = up_unvols['Cur_price'].min()                  # find lowest price row in DF
        uminv = up_unvols['Cur_price'].idxmin()             # get index ID of lowest price row
        u_got_it = up_unvols.loc[uminv]

        ulsym = u_got_it.at['Symbol']              # get symbol of lowest price item @ index_id
        ulname = u_got_it.at['Co_name']            # get name of lowest price item @ index_id
        upct = u_got_it.at['Pct_change']           # get %change of lowest price item @ index_id

        print ( f"Best low-buy OPPTY: #{uminv} - {ulname.rstrip()} ({ulsym.rstrip()}) @ ${ulp} / {upct}% gain" )
        print ( " " )
        print ( f"{un_vol_activity.up_unvol_listall()} " )
        print ( " ")
        print ( "========== Unusually high Volume / Down =====================================================" )
        print ( f"{un_vol_activity.down_unvol_listall()} " )
        print ( " ")
        # Add unusual vol into recommendations list []
        #recommended['2'] = ('Unusual vol:', ulsym.rstrip(), '$'+str(ulp), ulname.rstrip(), '+%'+str(un_vol_activity.up_df0.loc[uminv, ['Pct_change']][0]) )
        recommended['2'] = ('Unusual vol:', ulsym.rstrip(), '$'+str(ulp), ulname.rstrip(), '+%'+str(upct) )

# ##################################################################################
# ##### NP NLP News Reader for Sentiment Analysis
# ##### Currently read all news or ONE stock
# ###################################################################################

    if args['newsai_sent'] is not False:
            news_symbol = (args['newsai_sent'][0]).upper()
            arg_cycle = int(args['newsai_sent'][1])     # for testing & debug. Limit new scraping system to 20 runs.
            cmi_debug = __name__+"::newsai_sent.#1"
            final_sent_df = pd.DataFrame()              # reset DataFrame for each article
            print ( " " )
            print ( f"AI news reader sentimennt analysis for Stock [ {news_symbol} ]" )
            news_ai = ml_nlpreader(1, args, caller="news_ai")
            sent_ai = ml_sentiment(1, args)
            logging.info(f'%s - Open global LMBD KV cache engine...' % cmi_debug)
            lmdb_dbname = "LMDB_0001"
            lmdb_env = lmdb_io_eng("GLOBAL", lmdb_dbname, args)  # create instance of LMDB
            
            logging.info(f'%s - Execute nlp_read_one AI news sentiment LOOP...' % cmi_debug)
            articles_found = asyncio.run(news_ai.nlp_read_one(news_symbol, args))  # scan_news_feed() + eval_news_feed_stories()
            
            # kgraphdb = db_graph(1, args)                # inst a class 
            # kgraphdb.con_aopkgdb(1)                     # connect to neo4j db

            _atc = 0     # article specific stats : tokenz count
            _acc = 0     # article specific stats : chars count
            _awc = 0     # article specific stats :  words count
            _asc = 0     # article scentences
            _apc = 0     # article paragraphs
            _arc = 0     # article random non-scents/parags
 
            _ttcz = 0    # Cumulative : Total Tokens genertaed
            _tccz = 0    # Cumulative : Total chars read
            _twcz = 0    # Cumulative : Total words read 
            _tscz = 0    # Cumulative : Total scentences read
            _tpcz = 0    # Cumulative : Total paragraphs read
            _trcz = 0    # Cumulative : Total rands read
            
            ai_sent_start_time = time.perf_counter()  # Mark the start time
            antibot_load_balancer = 0
            ai_nlp_cycle = int(0)
            for sn_idx, sn_row in news_ai.yfn.ml_ingest.items():    # all pages extrated in ml_ingest
                aggmean_sent_df = pd.DataFrame()                    # reset DataFrame for each article
                thint = news_ai.nlp_summary_report(3, sn_idx)       # TESTING: News article TYPE in ml_ingest to look for      
                if thint == 0.0:    # only compute type 0.0 prepared and validated new articles in ML_ingest
                    # scraper loadbalancer, Anti-bot avoidance & performance balancing
                    # WARN:  executes sentiment_ai.compute_sentiment()
                    if antibot_load_balancer == 0:                          # randomize  craw4ai / BS4 scrapers
                        _atc, _awc, final_results = news_ai.yfn.artdata_C4_depth3(sn_idx, sent_ai, lmdb_env)    # craw4ai engine
                    else:
                        _atc, _awc, final_results = news_ai.yfn.artdata_BS4_depth3(sn_idx, sent_ai, lmdb_env)   # BS4 engine 
                    _rnd_loadb = random.randint(1, 100)             # randomize load balancer decison
                    if _rnd_loadb % 2 == 0:
                        antibot_load_balancer = 0                           # choose CRAW4AI scraper (+ unified BS4/C4 chunker)
                    else:
                        antibot_load_balancer = 1                           # choose BS4 scraper (+ unified BS4/C4 chunker)
                    if _atc == 0 and _awc == 0 and final_results is None:  # error state (extract FAILURE)
                        continue

                    '''
                    FINAL RESULTS DICT KEYS:
                        'article': item_idx,
                        'urlhash': hs,
                        'total_tokens': _final_data_dict.get('total_tokens'),
                        'total_chars': _final_data_dict.get('chars_count'),
                        'total_words': _final_data_dict.get('total_words'),
                        'scentence': _final_data_dict.get('scentence'),
                        'paragraph': _final_data_dict.get('paragraph'),
                        'random': _final_data_dict.get('random'),
                        'positive_count': sent_p,
                        'neutral_count': sent_z,
                        'negative_count': sent_n,
                        'bs4_rows': bs4_p_tag_count
                    '''

                    #print (f"##-@540: fr: {final_results}" )
                    _atc = final_results['total_tokens']
                    _acc = final_results['chars_count']
                    _awc = final_results['total_words'] 
                    _asc = final_results['scentence']
                    _apc = final_results['paragraph']
                    _arc = final_results['random']
                    this_urlhash = sent_ai.active_urlhash
                    
                    # compute cumulative metrics across ALL ARTICLES
                    _ttcz += _atc
                    _tccz += _acc
                    _twcz += _awc
                    _tscz += _asc
                    _tpcz += _apc
                    _trcz += _arc
                    
                    pd.set_option('display.max_rows', None)
                    pd.set_option('max_colwidth', 40)
                    aggregate_mean = sent_ai.sen_df0.loc[sent_ai.sen_df0['urlhash'] == this_urlhash].groupby('snt')['rnk'].mean()

                    # aggregate_mean DF keys are only set if the sentiment analysis computes a pos/net/neu sentiment for the article.
                    # If the article has no matching sentiment, the keys are not set in the DF.
                    # Check if the keys exists, and create a default = 0.0 if not
                    nx, px, zx = 0.0, 0.0, 0.0
                    try:
                        px = aggregate_mean.loc['positive']
                    except KeyError:
                        logging.info( f'%s - Positive sentiment DF key missing / Create + Set: 0.0' % cmi_debug )
                        aggregate_mean.loc['positive'] = 0.0

                    try:
                        nx = aggregate_mean.loc['negative']
                    except KeyError:
                        aggregate_mean.loc['negative'] = 0.0
                        logging.info( f'%s - Negative sentiment DF key missing / Create + Set: 0.0' % cmi_debug )

                    try:
                        zx = aggregate_mean.loc['neutral']
                    except KeyError:
                        logging.info( f'%s - Neutral sentiment DF key missing / Create + Set: 0.0' % cmi_debug )
                        aggregate_mean.loc['neutral'] = 0.0

                    #print ( f"\n\n### DEBUG: Article Dataframe 3 ####" )
                    data_payload = [[ \
                            sn_idx, \
                            this_urlhash, \
                            px, \
                            nx, \
                            zx ]]

                    # build the Sentiment DF that shows interesting computed sentiment for each article
                    sent_df_row = pd.DataFrame(data_payload, columns=['art', 'urlhash', 'psnt', 'nsnt', 'zsnt'] )
                    aggmean_sent_df = pd.concat([aggmean_sent_df, sent_df_row])
                    merge_row = pd.merge(news_ai.yfn.sen_stats_df, aggmean_sent_df, on=['art', 'urlhash'])
                    final_sent_df = pd.concat([final_sent_df, merge_row], ignore_index=True)
                    ai_nlp_cycle += 1
                    if ai_nlp_cycle < arg_cycle:        # only counting real articles, not junk, fake, adds etc
                        pass
                    else:
                        ##-debug print (f"Exiting AI NLP cycle @ article: {ai_nlp_cycle}...")
                        break                    
                else:
                    print (f"Skipping:      [ UNREADABLE / Article not valid for AI NLP Sentiment analysis] {ai_nlp_cycle}")
                    print (f"================ End.0 Skipping / No action taken ! {ai_nlp_cycle} ================" )

            ################################################################
            # END  AI AI NLP article processing data scraping loop
            ################################################################
            # DONE
            # - cycling through all articles for this stock symbol
            # - computing sentiment for all articles found
            # - Display final stats and results next

            # DEBUG
            if args['bool_verbose'] is True:        # Logging level
                news_ai.yfn.dump_ml_ingest()
                #print (f"{sent_ai.sen_df0}")
 
            #sent_ai.sen_df1 = sent_ai.sen_df0.groupby('snt').agg(['count'])
            pd.set_option("expand_frame_repr", False)
            aggregation_functions = { \
                'art': 'nunique', \
                'urlhash': 'nunique', \
                'positive': 'sum', \
                'neutral': 'sum', \
                'negative': 'sum', \
                'psnt': 'mean', \
                'nsnt': 'mean', \
                'zsnt': 'mean'
                }

            # Calculate the totals row
            totals_row = final_sent_df.agg(aggregation_functions)
            totals_df = pd.DataFrame(totals_row).T
            totals_df.index = ['Totals']
            final_sent_df['art'] = final_sent_df['art'].astype(object)
            totals_df['urlhash'] = ''       # Or np.nan if preferred for a numerical representation
            
            df_final = pd.concat([final_sent_df, totals_df])
            # print ( f"{df_final}")
            print (f"\n")

            aggr_sw_factor = 1.55       # aggregate stop words fatcor (TODO: can actually compute this!)
            h_read_wpm = 175            # how many words avg human can read per/min
            
            positive_t = df_final.iloc[-1]['psnt']
            negative_t = df_final.iloc[-1]['nsnt']
            neutral_t = df_final.iloc[-1]['zsnt']
            positive_c = df_final.iloc[-1]['positive']
            negative_c = df_final.iloc[-1]['negative']
            neutral_c = df_final.iloc[-1]['neutral']
            
            arts_read = df_final.iloc[-1]['art']
            row_count = len(df_final)
            hpt_mins = ((_twcz * aggr_sw_factor) + (_tscz + _tpcz + _trcz)) / h_read_wpm
            hpt_hours =  hpt_mins / 60
            analyst_time = (hpt_hours * 1.3) * 1.15     # extra time to compute sentiment, extra time to buld report
            analyst_rate = 500                          # hourly rate for a Wall St. Data Scientist + Fin Analyst ($/hour)
            analyst_cost = analyst_time * analyst_rate
            
            ai_sent_end_time = time.perf_counter()                          # Mark the end time
            ai_sent_time = ai_sent_end_time - ai_sent_start_time            # compute total time

            print ( f"========================= Final Sentiment Analysis for: {news_symbol.upper()} ================================" )       
            precise_results = sent_ai.sentiment_metrics(
                news_symbol.upper(), df_final, positive_c, negative_c, positive_t, negative_t, neutral_t
            )
            
            print (f"\n=================== AI NLP Sentiment processing metrics: {news_symbol.upper()} ==================================" )
            print (f"LLM Vec Tokenz:  {_ttcz} - Chars: {_tccz} / Words: {_twcz} / scent/paras: {(_tscz + _tpcz + _trcz)} | AI read time: {(ai_sent_time / 60):.2f} mins" )
            print (f"Human read time: {(hpt_mins):.1f} mins ({(hpt_hours):.1f} hours)  | Human analyst time: {analyst_time:.1f} hours" )
            print (f"AI performance:  {round((hpt_mins * 60) / (ai_sent_time / 60))} Faster than a Human  |   Analyst cost: ${round(analyst_cost):,}" )
            print (f" ")
            
            pd.set_option('display.max_rows', None)
            pd.set_option('display.max_columns', None)
            
            #################################################################
            # Neo4j DATBASE FUNCTIONS
            # KGdb stats
            # this code is buggy. Needs to be updated and optomozied.
            skip_kg_build = True    # disable code for now!
            
            if skip_kg_build is True:
                pass
            else:
                try:
                    found_sym = kgraphdb.check_node_exists(1, news_symbol)
                except TypeError:
                    # Type:class 'NoneType' is discovered here...
                    kg_node_id = kgraphdb.create_sym_node(news_symbol, sentiment_df=sent_ai.sen_df3)
                    print ( f"Error: Symbol node does NOT exist - creating ! fst:{type(kg_node_id)} / fs:{kg_node_id}" )
                    #kg_node_id = kgraphdb.create_sym_node(news_symbol)
                    # create a neo4j nodes Relationships, Properties and Types for each article thats associated with this symbol
                    kgraphdb.create_article_nodes(df_final, news_symbol)
                    kgraphdb.create_sym_art_rels(news_symbol, df_final,agency="Unknown", author="Unknown", published="Unknown", article_teaser="Unknown")
                    kgraphdb.news_agency()
                    print ( f"Error: Created Article nodes, Relationships, New Agency also !" )
                    created = True
                else:
                    match found_sym:
                        # FIX: add unknown elments later (need to gather them from elsewhere first)
                        # Article must be created first, then related to their parent symbol node
                        case None:
                            kg_node_id = kgraphdb.create_sym_node(news_symbol, sentiment_df=sent_ai.sen_df3)
                            print ( f"Error: Symbol node does NOT exist - creating ! fst:{type(kg_node_id)} / fs:{kg_node_id}" )
                            _kgec = kgraphdb.create_article_nodes(df_final, news_symbol)
                            kgraphdb.create_sym_art_rels(news_symbol, df_final, agency="Unknown", author="Unknown", published="Unknown", article_teaser="Unknown")
                            created = True
                            #if args['bool_verbose'] is True:
                            print (f" ")
                            print ( f"None: Symbol does NOT exists / status check: fst:{type(found_sym)} / fs:{found_sym}" )
                            print ( f"Created new KG nodes: {_kgec}" )
                        case True:
                            #if args['bool_verbose'] is True:
                            print (f" ")
                            print ( f"True: Symbol node  exist - Graph Node NOT created ! fst:{type(found_sym)} / fs:{found_sym}" )
                            created = False
                        case False:
                            _kgec = kgraphdb.create_article_nodes(df_final, news_symbol)
                            kgraphdb.create_sym_art_rels(news_symbol, df_final, agency="Unknown", author="Unknown", published="Unknown", article_teaser="Unknown")
                            created = True
                            #if args['bool_verbose'] is True:
                            print (f" ")
                            print ( f"Flase: Symbol node exists status check: fst:{type(found_sym)} / fs:{found_sym}" )
                            print ( f"Created new KG node_id: {_kgec}" )
                        case _:
                            print (f"Weird return code during GraphDB node exist check!" )  
                            print ( f"KG node exists status check: fst:{type(found_sym)} / fs:{found_sym}" )              
                            res = kgraphdb.dump_symbols(1)
                            kgraphdb.close_neo4j_kgdb(1, kgraphdb.driver)


#################################################################################

if __name__ == '__main__':
    main()
