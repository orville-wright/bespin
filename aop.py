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
from ml_yf_nlp_reader_c4 import ml_nlpreader
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

from data_engines_fundamentals.alphavantage_md import alphavantage_md
from db_graph import db_graph
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
from y_generalnews import y_generalnews

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
logging.basicConfig(level=logging.INFO)
work_inst = 0
global args
args = {}
global parser
articles_found = 0              # number of articles found by the AI news reader for 1 synble scan run
yti = 1
uh = url_hinter(1, args)        # everyone needs to be able to get hints on a URL from anywhere


parser = argparse.ArgumentParser(prog="Aop", description="Entropy apperture engine")
parser.add_argument('-a','--allnews', help='ML/NLP News sentiment AI for all stocks', action='store_true', dest='bool_news', required=False, default=False)
parser.add_argument('-d','--deep', help='Deep converged multi data list', action='store_true', dest='bool_deep', required=False, default=False)
#
#parser.add_argument('-n','--newsai-sent', help='AI NLP News sentiment AI for 1 stock', action='store', dest='newsai_sent', required=False, default=False)
parser.add_argument('-n','--newsai-sent', help='AI NLP News sentiment AI for 1 stock', nargs="*", dest='newsai_sent', required=False, default=False)
#
parser.add_argument('--news-cycle', help='Full news cycle extarct from eveny data engine', action='store_true', dest='news_cycle', required=False, default=False)
parser.add_argument('-p','--perf', help='Tech event performance sentiment', action='store_true', dest='bool_te', required=False, default=False)
parser.add_argument('-q','--quote', help='Get ticker price action quote', action='store', dest='qsymbol', required=False, default=False)
parser.add_argument('-s','--screen', help='Small cap screener logic', action='store_true', dest='bool_scr', required=False, default=False)
parser.add_argument('-t','--tops', help='show top ganers/losers', action='store_true', dest='bool_tops', required=False, default=False)
#
parser.add_argument('-u','--unusual', help='unusual up & down volume', action='store_true', dest='bool_uvol', required=False, default=False)
parser.add_argument('-v','--verbose', help='verbose error logging', action='store_true', dest='bool_verbose', required=False, default=False)
parser.add_argument('-x','--xray', help='dump detailed debug data structures', action='store_true', dest='bool_xray', required=False, default=False)
#
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
parser.add_argument('--alphavantage-news', help='Get Alpha Vantage market news (optionally filter by symbol)', action='store', dest='alphavantage_news_symbol', required=False, default=False)
parser.add_argument('--finnhub', help='Get Finnhub quote and data for symbol', action='store', dest='finnhub_symbol', required=False, default=False)
parser.add_argument('--finnhub-news', help='Get Finnhub financial news for symbol', action='store', dest='finnhub_news_symbol', required=False, default=False)
parser.add_argument('--marketstack', help='Get Marketstack EOD and intraday data for symbol', action='store', dest='marketstack_symbol', required=False, default=False)
parser.add_argument('--stockdata', help='Get StockData.org quote and data for symbol', action='store', dest='stockdata_symbol', required=False, default=False)
parser.add_argument('--twelvedata', help='Get Twelve Data comprehensive data for symbol', action='store', dest='twelvedata_symbol', required=False, default=False)
parser.add_argument('--eodhistoricaldata', help='Get EOD Historical Data for symbol', action='store', dest='eodhistoricaldata_symbol', required=False, default=False)
parser.add_argument('--financialmodelingprep', help='Get FinancialModelingPrep data for symbol', action='store', dest='financialmodelingprep_symbol', required=False, default=False)
parser.add_argument('--stooq', help='Get Stooq historical data for symbol', action='store', dest='stooq_symbol', required=False, default=False)

#######################################################################
# Global method for __main__
# thread function #1
# DEPRECATED

extract_done = threading.Event()

def do_nice_wait(topg_inst):
    """Threaded wait that does work to build out the 10x10x60 DataFrame"""
    logging.info('y_topgainers:: IN Thread - do_nice_wait()' )
    logging.info('y_topgainers::do_nice_wait() -> inst: %s' % topg_inst.yti )
    for r in range(6):
        logging.info('do_nice_wait() cycle: %s' % topg_inst.cycle )
        time.sleep(5)    # wait immediatley to let remote update
        topg_inst.get_topg_data()       # extract data from finance.Yahoo.com
        topg_inst.build_tg_df0()
        topg_inst.build_top10()
        topg_inst.build_tenten60(r)     # pass along current cycle
        print ( ".", end="", flush=True )
        topg_inst.cycle += 1            # adv loop cycle

        if topg_inst.cycle == 6:
            logging.info('do_nice_wait() - EMIT exit trigger' )
            extract_done.set()

    logging.info('do_nice_wait() - Cycle: %s' % topg_inst.cycle )
    logging.info('do_nice_wait() - EXIT thread inst: %s' % topg_inst.yti )

    return      # dont know if this this requireed or good semantics?

def bkgrnd_worker():
    """Threaded wait that does work to build out the 10x10x60 DataFrame"""
    global work_inst
    logging.info('main::bkgrnd_worker() IN Thread - bkgrnd_worker()' )
    logging.info('main::bkgrnd_worker() Ref -> inst #: %s' % work_inst.yti )
    for r in range(4):
        logging.info('main::bkgrnd_worker():: Loop: %s' % r )
        time.sleep(30)    # wait immediatley to let remote update
        work_inst.build_tg_df0()
        work_inst.build_top10()
        work_inst.build_tenten60(r)

    logging.info('main::bkgrnd_worker() EMIT exit trigger' )
    extract_done.set()
    logging.info('main::bkgrnd_worker() EXIT thread inst #: %s' % work_inst.yti )
    return      # dont know if this this requireed or good semantics?


############################# main() ##################################

def main():
    cmi_debug = "aop::"+__name__+"::main()"
    global args
    args = vars(parser.parse_args())        # args as a dict []
    print ( " " )
    print ( "#################### I n i t a l i z i n g ####################" )
    print ( " " )
    print ( "CMDLine args:", parser.parse_args() )
    if args['bool_verbose'] is True:        # Logging level
        print ( "Enabeling verbose info logging..." )
        logging.disable(0)                  # Log level = OFF
    else:
        logging.disable(20)                 # Log lvel = INFO

    print ( " " )
    recommended = {}        # dict of recomendations

########### 1 - TOP GAINERS ################
    if args['bool_tops'] is True:
        print ( "========== Large Cap / Top Gainers ===============================" )
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
        print ( "========== Large Cap / Top Loosers ================================" )
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
        
        ############## hacking on general news
        # this work is trying to use craw4ai to scrap finaince.yahoo.com under severe anti-bot guardrails
        #genews_reader = y_cookiemonster(3)
        #genews_dataset = y_generalnews(3)
        #genews_dataset.init_dummy_session()
        #genews_dataset.ext_req = genews_dataset.do_simple_get('https://finance.yahoo.com')
        #genews_dataset.ext_req = genews_reader.get_js_data('barrons.com/real-time/2')
        #genews_dataset.ext_get_data(3)
        #gx = genews_dataset.build_df0()
        print (f"Total News artciels extarcted: {ext_count}" )
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

################################################################################
# generate FINAL combo list 
# combine all the findings into 1 place - single source of truth
    """
    DEEP amalysis means - try to understand & inferr plain language reasons as to why these stock are
    appearing in the final 'Single Source of Truth' combo_df. Having a big list of top mover/highly active
    stocks isn't meaningful unless you can understand (quickly in real-time) whats going on with each one.
    From here, you can plan to do something... otherwise, this is just a meaningless list.
    NOTE: Most of this logic prepares/cleans/wrangles data into a perfect combo_df 'Single Source of Truth'.
    """
    if args['bool_deep'] is True and args['bool_scr'] is True and args['bool_uvol'] is True:
        x = combo_logic(1, mlx_top_dataset, small_cap_dataset, un_vol_activity, args )
        x.polish_combo_df(1)
        x.tag_dupes()
        x.tag_uniques()
        x.rank_hot()       # currently disabled b/c it errors. pandas statment needs to be simplifed and split
        #x.find_hottest()
        x.rank_unvol()     # ditto
        x.rank_caps()      # ditto
        x.combo_df.sort_values(by=['Symbol'])         # sort by sumbol name (so dupes are linearly grouped)
        x.reindex_combo_df()                          # re-order a new index (PERMENANT write)

# Summarize combo list key findings ##################################################################
        # Curious Outliers
        # temp_1 = x.combo_df.sort_values(by=['Pct_change'], ascending=False)
        # temp_1 = x.combo_df.sort_values(by=['Symbol'])                        # sort by sumbol name (so dupes are linearly grouped)
        # temp_1.reset_index(inplace=True, drop=True)                           # reset index

        x.find_hottest()

        print ( f"========== Hot stock anomolies ===================================================" )
        if x.combo_dupes_only_listall(1).empty:
            print ( f"NONE found at moment" )
        else:
            print ( f"{x.combo_dupes_only_listall(1)}" )

        print ( " " )
        print ( f"========== Full System of Truth  ===================================================" )
        print ( f"\n{x.combo_df}" )    # sort by %
        print ( " " )

        print ( "========== ** OUTLIERS ** : Unusual UP volume + Top Gainers by +5% ================================" )
        print ( " " )
        temp_1 = x.combo_df.sort_values(by=['Pct_change'], ascending=False) 
        print ( f"{temp_1}" )       # DUPLES in the DF = a curious outlier
        # print ( f"{temp_1[temp_1.duplicated(['Symbol'], keep='first')]}" )    # DUPLES in the DF = a curious outlier
        #print ( f"{temp_1[temp_1.duplicated(['Symbol'], keep='last')]}" )       # DUPLES in the DF = a curious outlier
        print ( " " )
        print ( f"================= >>COMBO<< Full list of intersting market observations ==================" )
        #print ( f"{x.combo_listall_nodupes()}" )
        temp_2 = x.combo_listall_nodupes()                                      # dupes by SYMBOL only
        print ( f"{temp_2.sort_values(by=['Pct_change'], ascending=False)}" )

        if len(x.rx) == 0:      # rx=[] holds hottest stock with lowest price overall
            print ( " " )       # empty list[] = no stock found yet (prob very early in trading morning)
            print ( f"No **hot** stock for >>LOW<< buy-in recommendations list yet" )
        else:
            hotidx = x.rx[0]
            hotsym = x.rx[1]
            hotp = x.combo_df.at[hotidx, 'Cur_price']
            #hotp = x.combo_df.loc[[x.combo_df['Symbol'] == hotsym], ['Cur_price']]
            hotname = x.combo_df.at[hotidx, 'Co_name']
            hotpct = x.combo_df.at[hotidx, 'Pct_change']
            #hotname = x.combo_df.loc[hotidx, ['Co_name']][0]
            print ( " " )       # empty list[] = no stock found yet (prob very early in trading morning)

            #row_index = x.combo_df.loc[x.combo_df['Symbol'] == hotsym.rstrip()].index[0]

            #recommended['3'] = ('Hottest:', hotsym.rstrip(), '$'+str(hotp), hotname.rstrip(), '+%'+str(x.combo_df.loc[hotidx, ['Pct_change']][0]) )
            recommended['3'] = ('Hottest:', hotsym.rstrip(), '$'+str(hotp), hotname.rstrip(), '+%'+str(x.combo_df.at[hotidx, 'Pct_change']) )
            print ( f"==============================================================================================" )
            print ( f"Best low-buy highest %gain **Hot** OPPTY: {hotsym.rstrip()} - {hotname.rstrip()} / {'$'+str(hotp)} / {'+%'+str(hotpct)} gain" )
            print ( " " )
            print ( " " )

        # lowest priced stock
        clp = x.combo_df['Cur_price'].min()
        cminv = x.combo_df['Cur_price'].idxmin()
        i_got_min = x.combo_df.loc[cminv]

        clsym = i_got_min.at['Symbol']                # get symbol of lowest price item @ index_id
        clname = i_got_min.at['Co_name']              # get name of lowest price item @ index_id
        clupct = i_got_min.at['Pct_change']           # get %change of lowest price item @ index_id

        #clsym = x.combo_df.loc[cminv, ['Symbol']][0]
        #clname = x.combo_df.loc[cminv, ['Co_name']][0]    
        #recommended['4'] = ('Large cap:', clsym.rstrip(), '$'+str(clp), clname.rstrip(), '+%'+str(x.combo_df.loc[cminv, ['Pct_change']][0]) )

        recommended['4'] = ('Large cap:', clsym.rstrip(), '$'+str(clp), clname.rstrip(), '+%'+str(clupct) )

        # Biggest % gainer stock
        cmax = x.combo_df['Pct_change'].idxmax()
        clp = x.combo_df.loc[cmax, 'Cur_price']
        i_got_max = x.combo_df.loc[cmax]

        clsym = i_got_max.at['Symbol']                # get symbol of lowest price item @ index_id
        clname = i_got_max.at['Co_name']              # get name of lowest price item @ index_id
        clupct = i_got_max.at['Pct_change']           # get %change of lowest price item @ index_id
        #recommended['5'] = ('Top % gainer:', clsym.rstrip(), '$'+str(clp), clname.rstrip(), '+%'+str(x.combo_df.loc[cmax, ['Pct_change']][0]) )

        recommended['5'] = ('Top % gainer:', clsym.rstrip(), '$'+str(clp), clname.rstrip(), '+%'+str(clupct) )
        

# Recommendeds ###############################################################
        #  key    recomendation data     - (example output shown)
        # =====================================================================
        #   1:    Small cap % gainer: TXMD $0.818 TherapeuticsMD, Inc. +%7.12
        #   2:    Unusual vol: SPRT $11.17 support.com, Inc. +%26.79
        #   3:    Hottest: AUPH $17.93 Aurinia Pharmaceuticals I +%9.06
        #   4:    Large cap: PHJMF $0.07 PT Hanjaya Mandala Sampoe +%9.2
        #   5:    Top % gainer: SPRT $19.7 support.com, Inc. +%41.12
        # todo: we should do a linear regression on the price curve for each item

        print ( " " )
        print ( f"============ recommendations >>Lowest buy price<< stocks with greatest % gain  =============" )
        print ( " " )
        for k, v in recommended.items():
            print ( f"{k:3}: {v[0]:21} {v[1]:6} {v[3]:28} {v[2]:8} /  {v[4]}" )
            print ( "--------------------------------------------------------------------------------------------" )

# Summary ############### AVERGAES and computed info ##########################
        print ( " " )
        print ( "============== Market activity overview, inisghts & stats =================" )
        avgs_prc = x.combo_grouped(2).round(2)       # insights
        avgs_pct = x.combo_grouped(1).round(2)       # insights

        print ( f"Price average over all stock movemnts" )
        print ( f"{avgs_prc}" )
        print ( " " )
        print ( f"Percent  % average over all stock movemnts" )
        print ( f"{avgs_pct}" )

        #print ( f"Current day average $ gain: ${averages.iloc[-1]['Prc_change'].round(2)}" )
        #print ( f"Current day percent gain:   %{averages.iloc[-1]['Pct_change'].round(2)}" )

################################################################################
# WARN: Deprecated
# Finaince.Yahoo.com moved these indicators int PREMIUM account owners only
# Get the Bull/Bear Technical performance Sentiment for all stocks in combo DF
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
            print ( f"AI news reader sentimennt analysis for Stock [ {news_symbol} ] =========================" )
            news_ai = ml_nlpreader(1, args)
            sent_ai = ml_sentiment(1, args)
            articles_found = asyncio.run(news_ai.nlp_read_one(news_symbol, args))  # scan_news_feed() + eval_news_feed_stories()
            
            kgraphdb = db_graph(1, args)                # inst a class 
            kgraphdb.con_aopkgdb(1)                     # connect to neo4j db

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
            load_balancer = 0
            ai_nlp_cycle = int(0)
            for sn_idx, sn_row in news_ai.yfn.ml_ingest.items():    # all pages extrated in ml_ingest
                aggmean_sent_df = pd.DataFrame()                    # reset DataFrame for each article
                thint = news_ai.nlp_summary_report(3, sn_idx)       # TESTING: News article TYPE in ml_ingest to look for      
                if thint == 0.0:    # only compute type 0.0 prepared and validated new articles in ML_ingest
                    # scraper loadbalancer, Anti-bot avoidance & performance balancing
                    # WARN:  executes sentiment_ai.compute_sentiment()
                    load_balancer = 1

                    if load_balancer == 0:                          # balance between craw4ai / BS4 scrapers+chunkers
                        _atc, _awc, final_results = news_ai.yfn.extr_artdata_depth3(sn_idx, sent_ai)    # craw4ai engine
                    else:
                        _atc, _awc, final_results = news_ai.yfn.BS4_artdata_depth3(sn_idx, sent_ai)   # BS4 engine 
                    rnd_loadbr = random.randint(1, 100)             # randomize load balancer decison
                    if rnd_loadbr % 2 == 0:
                        load_balancer = 0                           # choose CRAW4AI scraper/chunker
                    else:
                        load_balancer = 1                           # choose BS4 scraper/chunker
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
                        print (f"Exiting AI NLP cycle @ article: {ai_nlp_cycle}...")
                        break                    
                else:
                    print (f"Skipping: {ai_nlp_cycle} / Article type not valid for AI NLP Sentiment analysis...")

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
            try:
                found_sym = kgraphdb.check_node_exists(1, news_symbol)
                if found_sym['present'] is True:    # True = symbol already exists
                    # FIX: add unknown elments later (need to gather them from elsewhere first)
                    # Article must be created first, then related to their parent symbol node
                    kgraphdb.create_article_nodes(df_final, news_symbol)
                    kgraphdb.create_sym_art_rels(news_symbol, df_final, agency="Unknown", author="Unknown", published="Unknown", article_teaser="Unknown")
                    created = False
                    pass    # do nothing is Ticker Symbol exists
            except TypeError:
                # Type:class 'NoneType' is discovered here...
                kg_node_id = kgraphdb.create_sym_node(news_symbol, sentiment_df=sent_ai.sen_df3)
                #kg_node_id = kgraphdb.create_sym_node(news_symbol)
                # create a neo4j nodes Relationships, Properties and Types for each article thats associated with this symbol
                kgraphdb.create_article_nodes(df_final, news_symbol)
                kgraphdb.create_sym_art_rels(news_symbol, df_final,agency="Unknown", author="Unknown", published="Unknown", article_teaser="Unknown")
                kgraphdb.news_agency()
                created = True
                
            if args['bool_verbose'] is True:
                print (f" ")
                if created is True:    # True = symbol already exists
                    print ( f"Created new KG node_id: {kg_node_id}" )
                else:
                    print ( f"Symbol allready exist - New node NOT created !" )
                res = kgraphdb.dump_symbols(1)
                kgraphdb.close_aopkgdb(1, kgraphdb.driver)


#################################################################################
##############              MARKET DATA EXTRACTORS                     ##########
#################################################################################


#################################################################################
###############                    QUOTES                            ############
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
        cmi_debug = __name__+"::"+"Fred_econ_data"+".#1"
        try:
            fred = fred_md(1, args)
            
            # Get economic snapshot
            snapshot = fred.get_economic_snapshot()
            #print (f"\n{snapshot}")
            if snapshot:
                print("Key Economic Snapshot Major Indicators:")               
                for indicator, data in snapshot.items():
                    print(f"  {indicator.replace('_', ' ').title()}: {data['value']} ({data['rt_sdate']} - {data['rt_edate']}) | ({data['date']} ({data['series_id']})")

            # Get get_economic trends
            snapshot = fred.get_economic_trends()
            #print (f"\n{snapshot}")
            if snapshot:
                print("\nKey Economic Trends:")
                for indicator, data in snapshot.items():
                    print(f"  {indicator.replace('_', ' ').title()}: {data['current']} ({data['start_period']}) ({data['change']}) ({data['pct_change']}) ({data['period_days']})" )
                

            # Get yield curve
            yield_curve = fred.get_yield_curve()
            if yield_curve:
                print(f"\nTreasury Yield Curve:")
                for maturity, rate in yield_curve.items():
                    print(f"  {maturity.replace('_', ' ')}: {rate}%")

                #for maturity, rate in yield_curve.items():
                #    print(f"  {maturity.replace('_', ' ')}: {rate}%")
                                        
        except Exception as e:
            print(f"Error fetching FRED data: {e}")
            logging.error(f"FRED data error: {e}")
        
        print(" ")

#####################################################################
##### Polygon.io integration
#####
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
            # This is not a PREMIUM service. (not free). It will fail with a Free levle API key.
            quote = polygon.get_last_quote(polygon_symbol)
            if quote["status"] != "NOT_AUTHORIZED":
                if quote:
                    print(f"Last Quote:")
                    print(f"  Bid: ${quote.get('bid', 'N/A')} x {quote.get('bid_size', 'N/A')}")
                    print(f"  Ask: ${quote.get('ask', 'N/A')} x {quote.get('ask_size', 'N/A')}")
                    if quote.get('spread'):
                        print(f"  Spread: ${quote['spread']:.4f}")
            else:
                print (f"Last Quote data not available: {quote["reason"]}" )
            
            # Get ticker details
            details = polygon.get_company_info(polygon_symbol)
            if details:
                print(f"\nCompany Details:")
                print(f"  Name: {details.get('name', 'N/A')}")
                print(f"  Market: {details.get('market', 'N/A')}")
                print(f"  Exchange: {details.get('primary_exchange', 'N/A')}")
                if details.get('market_cap'):
                    print(f"  Market Cap: ${details['market_cap']:,}")
            
            # Get recent daily bars
            # NOTE: The class fucntion get_aggregates() has multiple posible data outputs that could be retruned...
            # 1. an old JSON payload
            # 2. an new customer LIST[] payload
            # 3. a pure Datafame
            # 4. and below... all that info is manually reformetted in this custom output 
            bars = polygon.get_aggregates(polygon_symbol, timespan='day', limit=5)
            if not bars.empty:
                idx = 1
                print(f"\nRecent Daily Bars:")
                for idx, bar in bars.iterrows():
                    print(f"  {idx:03d} - {bar['symbol']} - {bar['time'].strftime('%Y-%m-%d')}: O:{bar['open']:.2f} H:{bar['high']:.2f} L:{bar['low']:.2f} C:{bar['close']:.2f} V:{bar['vol']:,}")
                    idx += 1
        except Exception as e:
            print(f"Error fetching Polygon data: {e}")
            logging.error(f"Polygon data error extractor for: {polygon_symbol} - {e}")
        
        print(" ")

#####################################################################
##### Tiingo comprehensive data integration
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
            cmi_debug = "aop.main()"+"::"+"AV_top-gainer-losers"
            logging.info( f"%s - IN.#{yti}" % cmi_debug )
            gainers_losers = av.get_top_gainers_losers()
            if gainers_losers:
                logging.info( f"%s - get DICT metadata / validate: {type(gainers_losers)}" % cmi_debug )
                metadata = gainers_losers.get('metadata', {})
                last_updated = gainers_losers.get('last_updated', {})
                print(f"Market data as of: {last_updated}")
                
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

    # Alpha Vantage market news integration
    if args['alphavantage_news_symbol'] is not False:
        print("========== Alpha Vantage Market News ==========")
        
        try:
            av_news = alphavantage_md(5, args)
            
            # Check if a specific symbol was provided or get general market news
            if args['alphavantage_news_symbol'].upper() != 'GENERAL':
                news_symbol = args['alphavantage_news_symbol'].upper()
                print(f"Getting market news for: {news_symbol}")
                news_data = av_news.market_news(tickers=news_symbol, limit=10)
            else:
                print("Getting general market news...")
                news_data = av_news.market_news(limit=15)
            
            if news_data and 'feed' in news_data:
                articles = news_data['feed']
                print(f"Found {len(articles)} news articles")
                
                if news_data.get('sentiment_score_definition'):
                    print(f"\nSentiment Score Definition: {news_data['sentiment_score_definition']}")
                
                print("\nRecent Market News:")
                print("-" * 80)
                
                for idx, article in enumerate(articles[:20], 1):  # Show top 20 articles
                    print(f"\n{idx}. {article.get('title', 'N/A')}")
                    print(f"   Source: {article.get('source', 'N/A')} | Published: {article.get('time_published', 'N/A')}")
                    print(f"   url: {article.get('url', 'N/A')}")
                    # This is where we insert the URL for this stock tick into the LMDB KV Datastore
                    # LMDB POI-news KV might look like this: (urlhash, [ticker, url, publisher])
                    
                    # Show sentiment analysis
                    sentiment_score = article.get('overall_sentiment_score', 0)
                    sentiment_label = article.get('overall_sentiment_label', 'N/A')
                    print(f"   Sentiment: {sentiment_label} (Score: {sentiment_score:.3f})")
                    
                    # Show topics if available
                    topics = article.get('topics', [])
                    if topics:
                        topic_list = [topic.get('topic', 'N/A') for topic in topics[:3]]  # Show first 3 topics
                        print(f"   Topics: {', '.join(topic_list)}")
                    
                    # Show ticker sentiment for specific symbols
                    ticker_sentiment = article.get('ticker_sentiment', [])
                    if ticker_sentiment:
                        for ticker_data in ticker_sentiment[:3]:  # Show first 3 tickers
                            ticker = ticker_data.get('ticker', 'N/A')
                            relevance = ticker_data.get('relevance_score', 'N/A')
                            ticker_sent_score = ticker_data.get('ticker_sentiment_score', 'N/A')
                            ticker_sent_label = ticker_data.get('ticker_sentiment_label', 'N/A')
                            print(f"   {ticker}: Relevance {relevance}, Sentiment {ticker_sent_label} ({ticker_sent_score})")
                    
                    # Show summary if available
                    summary = article.get('summary', '')
                    if summary:
                        # Truncate summary to 100 characters
                        summary_truncated = summary[:100] + "..." if len(summary) > 100 else summary
                        print(f"   Summary: {summary_truncated}")
                    
                    print("   " + "-" * 78)
                
                # Show aggregate sentiment statistics
                if articles:
                    total_articles = len(articles)
                    positive_articles = len([a for a in articles if a.get('overall_sentiment_label') == 'Bullish'])
                    negative_articles = len([a for a in articles if a.get('overall_sentiment_label') == 'Bearish'])
                    neutral_articles = len([a for a in articles if a.get('overall_sentiment_label') == 'Neutral'])
                    
                    avg_sentiment = sum([float(a.get('overall_sentiment_score', 0)) for a in articles]) / total_articles
                    
                    print(f"\nAggregate News Sentiment Analysis:")
                    print(f"  Total Articles: {total_articles}")
                    print(f"  Bullish: {positive_articles} ({positive_articles/total_articles*100:.1f}%)")
                    print(f"  Bearish: {negative_articles} ({negative_articles/total_articles*100:.1f}%)")
                    print(f"  Neutral: {neutral_articles} ({neutral_articles/total_articles*100:.1f}%)")
                    print(f"  Average Sentiment Score: {avg_sentiment:.3f}")
                    
                    if avg_sentiment > 0.1:
                        overall_sentiment = "Bullish"
                    elif avg_sentiment < -0.1:
                        overall_sentiment = "Bearish"  
                    else:
                        overall_sentiment = "Neutral"
                    
                    print(f"  Overall Market Sentiment: {overall_sentiment}")
                    
            else:
                print("No news articles available")
                
        except Exception as e:
            print(f"Error fetching Alpha Vantage market news: {e}")
            logging.error(f"Alpha Vantage market news error: {e}")
        
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
