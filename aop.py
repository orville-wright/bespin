#! python3

import websockets.client  # Force-loads the client module into the websockets namespace
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
from ml_yf_nlp_orchestrator import ml_nlpreader, NewsAgeResolver
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
from y_unvol import y_unvol
from y_unvoljs import yf_unvoljs
from datastore_eng_LMDB import lmdb_io_eng

from neo4j_graphdb import neo4j_auradb

"""
Disbaled for now
TODO: Move out to xop.py
from data_engines_fundamentals.alphavantage_md import alphavantage_md
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
"""

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

"""
parser.add_argument('--alpaca', help='Get Alpaca live quotes for symbol', action='store', dest='alpaca_symbol', required=False, default=False)
parser.add_argument('--alpaca-bars', help='Get Alpaca OHLCV bars for symbol', action='store', dest='alpaca_bars', required=False, default=False)
parser.add_argument('--alpaca-feed', help='Alpaca data feed: iex, sip, delayed_sip, boats, overnight, or otc', action='store', dest='alpaca_feed', required=False, default=None)
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
"""

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


########## 0 Basic Quotes #################3
    if args['qsymbol'] is not False:
        quoute_examples()

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

########### 4 Generla News Reader ################
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
        scap_reader = y_cookiemonster(1)             # instantiate class of cookiemonster
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
        
        # UNUSUAL Volumes directly from NASDAQ.com for NASDAQ stocks only !
        #
        print ( "========== NASDAQ only Unusually high Volume / Up =============================================" )
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
        print ( "========== NASDAQ only  Unusually high Volume / Down ===========================================" )
        print ( f"{un_vol_activity.down_unvol_listall()} " )
        print ( " ")
        # Add unusual vol into recommendations list []
        #recommended['2'] = ('Unusual vol:', ulsym.rstrip(), '$'+str(ulp), ulname.rstrip(), '+%'+str(un_vol_activity.up_df0.loc[uminv, ['Pct_change']][0]) )
        recommended['2'] = ('Unusual vol:', ulsym.rstrip(), '$'+str(ulp), ulname.rstrip(), '+%'+str(upct) )

        """
        ########### YAHOO FINANCE UNUSUAL VOLUME  ################
        # UNUSUAL Volumes directly from Finaince Yahoo.com
        # Stocks originate from many/any/all exchanges that Yahoo.com is tracking. So list is random.
        # Could also include NASDAQ stocks from Nasdaq explcit list

        print ( "========== Finaince Yahoo.com Unusual Volume movers / Broad Spectrum  view ===================" )
        y_unvol_reader = y_cookiemonster(2)        # instantiate class of cookiemonster

        y_unvol_dataset = y_unvol(1)               # instantiate class
        y_unvol_dataset.init_dummy_session()       # setup cookie jar and headers
 
        y_unvol_dataset.ext_req = y_unvol_reader.get_js_data('finance.yahoo.com/markets/stocks/unusual-volume-stocks/?start=0&count=20')
        y_unvol_dataset.ext_get_data(1)

        x = y_unvol_dataset.build_uv_df0()     # build full dataframe
        y_unvol_dataset.build_top10()          # show top 10
        y_unvol_dataset.print_top10()          # print it
        print ( " " )
        """

########### Testing new JSON YF Unusual VOLUME extractor ################
        print ( "========== YF JSON mode Unusually high Volume ========================================" )
        yf_un_vol_activity = yf_unvoljs(1, args)       # instantiate NEW nasdaq data class, args = global var
        yf_un_vol_activity.get_un_vol_data()           # extract JSON data (Up & DOWN) from api.nasdaq.com

        # should test success of extract before attempting DF population
        yf_un_vol_activity.build_df(0)           # 0 = UP Unusual volume

        # find lowest price stock in unusuall UP volume list
        _up_yf_unvols = yf_un_vol_activity.up_unvol_listall()      # temp DF, nicely ordered & indexed of unusual UP vol activity

        _ulp = _up_yf_unvols['Cur_price'].min()                  # find lowest price row in DF
        _uminv = _up_yf_unvols['Cur_price'].idxmin()             # get index ID of lowest price row
        _u_got_it = _up_yf_unvols.loc[uminv]

        _ulsym = u_got_it.at['Symbol']              # get symbol of lowest price item @ index_id
        _ulname = u_got_it.at['Co_name']            # get name of lowest price item @ index_id
        _upct = u_got_it.at['Pct_change']           # get %change of lowest price item @ index_id

        print ( f"Best low-buy OPPTY: #{_uminv} - {_ulname.rstrip()} ({_ulsym.rstrip()}) @ ${_ulp} / {_upct}% gain" )
        print ( " " )
        print ( f"{_up_yf_unvols}" )
        
################################################################################
# DELETE ME
# THis was the original logic to combine all the findings into a single source of truth
# it will now be done by AI
# 
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
        print ( "========== Hottest stocks Bullish status =============" )
        print ( f"{te.te_df0[['Symbol', 'Today', 'Short', 'Mid', 'Long', 'Bullcount', 'Senti']].sort_values(by=['Bullcount', 'Senti'], ascending=False)}" )
        print ( "------------------------------------------------------" )
        #
        # HACKING : show uniques from COMBO def
        print ( "***** Hacking ***** " )
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
            try:
                arg_cycle = int(args['newsai_sent'][1])     # for testing & debug. Limit scraping to nn articles.
            except  IndexError:
                arg_cycle = 50          # default articels to get if users fails to provide a number on cmdline
                print ( f"No user constrained scan limit provided! - Scanning for {arg_cycle} articles...")

            if arg_cycle == 999:
                print ( "Scanning for maximum number of News articles: 999... ")
            else:
                print ( f"User constrained news scan limit to: {arg_cycle} articles...")

            cmi_debug = __name__+"::newsai_sent.#1"
            ai_nlp_cycle = int(1)
            bad_articles = int(1)

            final_sent_df = pd.DataFrame()              # reset DataFrame for each article
            
            # Threaded optimization pre-loader : Phase 1
            ml_sentiment.preload_classifier()
            # create a Thread to background to preload the heavy HF classifier pipeline

            print ( " " )
            print ( f"AI news reader sentimennt analysis for Stock [ {news_symbol} ]" )
            news_ai = ml_nlpreader(1, args, caller="news_ai")
            news_date_resolver = NewsAgeResolver()  # Singleton class for News age date analytics
            news_ai.dateageresolver = news_date_resolver  # assign singleton instance to the news_ai class
            #news_ai.share_ageresolver()

            logging.info( '%s - Open global LMBD KV cache engine...' % cmi_debug)
            lmdb_dbname = "LMDB_0001"
            lmdb_env = lmdb_io_eng("GLOBAL", lmdb_dbname, args)  # create instance of LMDB

            logging.info( '%s - Execute nlp_read_one AI news sentiment LOOP...' % cmi_debug)
            
            # scan_news_feed() + eval_news_feed_stories()

            articles_found = asyncio.run(news_ai.nlp_read_one(news_symbol, args))  
            if articles_found == 0:
                print ( f"AI news reader found NO articles for Stock [ {news_symbol} ]" )
                exit(1)

            # Threaded optimization : Phase 2
            # The nlp_read_one() scrape/skim takes 10 ~ 15 seconds to complete skimming 100 top level article feed
            # - This gives time for the HF Classifier pipeline preloader (Phase 1) Thread to complete heavy init work
            # - once nlp_read_one() returns, we can instantiate an ml_sentiment class
            # - which should be fast, if the pipeline initiatization Thread completed its heavy init workload
            # - while nlp_read_one() was working
            sent_ai = ml_sentiment(1, args)
            
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
            
            antibot_load_balancer = 0
            ai_sent_start_time = time.perf_counter()  # Mark the start time
            for sn_idx, sn_row in news_ai.yfn.ml_ingest.items():    # Main LOOP - all pages extrated in ml_ingest
                aggmean_sent_df = pd.DataFrame()                    # reset DataFrame for each article
                thint = news_ai.nlp_summary_report(3, sn_idx)       # get this TYPE of new article from ml_ingest : sn_idx = article counter loop

                # ######################################################
                # Anti-bot avoidance scraping load-balancer logic
                # WARN:  eventually calls  sentiment_ai.compute_sentiment()
                if thint == 0.0:    # only compute type = 0.0 pre-processed + validated News articles in ML_ingest
                    if antibot_load_balancer == 0:                  # randomize  craw4ai / BS4 scrapers
                        _atc, _awc, final_results = news_ai.yfn.artdata_C4_depth3(sn_idx, sent_ai, lmdb_env)    # craw4ai engine
                    else:
                        _atc, _awc, final_results = news_ai.yfn.artdata_BS4_depth3(sn_idx, sent_ai, lmdb_env)   # BS4 engine 
                    _rnd_loadb = random.randint(1, 100)             # randomize load balancer decison
                    if _rnd_loadb % 2 == 0:
                        antibot_load_balancer = 0                   # choose CRAW4AI scraper (+ unified BS4/C4 chunker)
                    else:
                        antibot_load_balancer = 1                   # choose BS4 scraper (+ unified BS4/C4 chunker)
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
                        logging.info( '%s - Positive sentiment DF key missing / Create + Set: 0.0' % cmi_debug )
                        aggregate_mean.loc['positive'] = 0.0

                    try:
                        nx = aggregate_mean.loc['negative']
                    except KeyError:
                        aggregate_mean.loc['negative'] = 0.0
                        logging.info( '%s - Negative sentiment DF key missing / Create + Set: 0.0' % cmi_debug )

                    try:
                        zx = aggregate_mean.loc['neutral']
                    except KeyError:
                        logging.info( '%s - Neutral sentiment DF key missing / Create + Set: 0.0' % cmi_debug )
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
                    
                    if ai_nlp_cycle < arg_cycle:        # only counting real articles, not junk, fake, adds etc
                        ai_nlp_cycle += 1
                        pass
                    else:
                        print (f"\n** Exiting cycle @ article: {ai_nlp_cycle}...")
                        break                    
                else:
                    print (f"Skipping:      [ UNREADABLE / Not a candidate for Sentiment analysis] {bad_articles}")
                    print (f"================ End.0 Skipping / No action for article: {sn_idx} ================" )
                    ai_nlp_cycle += 1
                    bad_articles += 1
                    
            ################################################################
            # END  AI AI NLP article processing data scraping loop
            ################################################################
            # DONE
            # - cycling through all articles for this stock symbol
            # - computing sentiment for all articles found
            # - Display final stats and results Summary report

            # DEBUG
            if args['bool_verbose'] is True:        # Logging level
                news_ai.yfn.dump_ml_ingest()        # the list of candidate articles we read
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
            
            # ############################################################### 
            # jumbo Dataframe that holds all sentimnent metrics for this run
            # ############################################################### 
            df_final = pd.concat([final_sent_df, totals_df])
            # print ( f"{df_final}")
            print ( "\n")

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

            # #############################################
            # Final News scan Summary report
            # note - df_final is a jumbo DataFrame that holds all sentimnent metrics for this run
            # but it is not used in the final summary report, but it is available for debugging and analysis
            # 
            sent_summary_results = sent_ai.sentiment_metrics(
                news_symbol.upper(),
                df_final, positive_c, negative_c,
                positive_t, negative_t, neutral_t
            )

            print (f"=================== AI NLP Sentiment processing metrics: {news_symbol.upper()} ===================" )
            print (f"LLM Vec Tokenz:  {_ttcz} - Chars: {_tccz} / Words: {_twcz} / scent/paras: {(_tscz + _tpcz + _trcz)} | AI read time: {(ai_sent_time / 60):.2f} mins" )
            print (f"Human read time: {(hpt_mins):.1f} mins ({(hpt_hours):.1f} hours)\t| Human analyst time: {analyst_time:.1f} hours" )
            print (f"AI performance:  {round((hpt_mins * 60) / (ai_sent_time / 60))} X Faster than a Human\t| Human analyst cost: ${round(analyst_cost):,}" )
            print (" ")
            
            #pd.set_option('display.max_rows', None)
            #pd.set_option('display.max_columns', None)
            #print ( f"DEBUG-#659:  sent_ai.df_final\n{df_final}\n")
            print ("--------------------------------")

            print ( f"Valid AI articles read:     {sent_ai.kv_rehydrated + news_ai.yfn.kv_created_C4 + news_ai.yfn.kv_created_BS4} //"
                    f"Potentially bad: {bad_articles} // Total considerd: {sn_idx}" )
            print ( f"Rehydrated from cache:      {sent_ai.kv_rehydrated}")
            print ( f"New C4 extracted articles:  {news_ai.yfn.kv_created_C4} // New BS4 extracted articles: {news_ai.yfn.kv_created_BS4}")
            print ( f"Total new articles extrctd: {news_ai.yfn.kv_created_C4 + news_ai.yfn.kv_created_BS4}")
            print ( f"Sentimnt chunks from cache: {sent_ai.sen_cache_eng}" )
            print ( f"LLM computed sent chunks:   {sent_ai.sen_llm_eng}" )
            print ( f"Total sentiment chunks:     {sent_ai.df0_row_count}" )
            print ( "\n" )

            # End Summary report
            # ############### Done reading many articles ###################

            #print ( f"{news_ai.yfn.ml_ingest}")

            print ("\n\n")

#################################################################
# Neo4j Graph DATBASE build-out
# TODO: This could probably all be moved into neo4j_graphbb.py
#################################################################
            """
            # Critical Data Payloads  used in the GraphDB build-out
            # not all are used, but this is the full corpus of metrics available
            summary_report{}
                "symbol": symbol,
                "sentiment": sentiment_label,
                "base_sentiment": base,
                "band_progress": progress_pct,
                "signal_clarity": split_vector_model["clarity"],
                "signal_conviction": split_vector_model["conviction"],
                "net_score": net_sentiment,
                "signal_purity": confidence,
                "positive_share": positive_share,
                "neutral_share": neutral_share,
                "negative_share": negative_share,
                "positive_strength": positive_strength,
                "neutral_strength": neutral_strength,
                "negative_strength": negative_strength

            summary_2v_metrics{}
                "sentiment": sentiment
                "direction_score": round(direction_score, 4)
                "clarity": round(clarity, 4)
                "conviction": round(conviction, 4)
                "pos_dir": round(pos_dir, 4)
                "neg_dir": round(neg_dir, 4)

            metrics{}
                "symbol": symbol
                "net_sentiment": round(net_sentiment, 4)
                "confidence": round(confidence, 4)
                "positive_share": round(positive_share, 4)
                "neutral_share": round(neutral_share, 4)
                "negative_share": round(negative_share, 4)
                "positive_strength": round(positive_strength, 4)
                "neutral_strength": round(neutral_strength, 4)
                "negative_strength": round(negative_strength, 4)
                "positive_mean": positive_t
                "neutral_mean": neutral_t
                "negative_mean": negative_t
                "positive_count": positive_c
                "negative_count": negative_c
            """
            cmi_debug = "aop.main()"+"::"+"Neo4j-Graph_LOOP.#1"
            total_rehydrated = sent_ai.kv_rehydrated
            total_new_articles = news_ai.yfn.kv_created_C4 + news_ai.yfn.kv_created_BS4
            total_articles = sent_ai.kv_rehydrated + news_ai.yfn.kv_created_C4 + news_ai.yfn.kv_created_BS4
            
            skip_kg_build = False       # switch to enable/disable Neo4j Aura operations
            
            if skip_kg_build is True:   # Feature-Flag: enable/disbale Neo4j Graph functionality
                pass

            if total_articles > 0:    # symbol might not exist
                kgraphdb = neo4j_auradb("AOP_AURA", args)           # create an inst of an Neo4j AURA Knowledge Graph DB
                try:
                    kgraphdb.con_neo4j_auradb("AOP_AURA")           # connect to free Neo4j AURA DB 
                    found_sym = kgraphdb.check_node_exists("AOP_AURA", news_symbol)  # test - stock symbol exists ?
                    match found_sym:
                        case False:         # NO stock symbol node does NOT exist
                            logging.info( f'%s - Symbol node {news_symbol} NOT in Graph: adding...' % cmi_debug )
                            try:    # Create new via Cypher CREATE via rebuild=False
                                kg_node_id = kgraphdb.create_sym_node(
                                    news_symbol,
                                    df_final,
                                    sent_ai.summary_report,
                                    sent_ai.summary_metrics,
                                    sent_ai.summary_2v_metrics,
                                    rebuild=False
                                    )
                                logging.info( f'%s - Created symbol node {news_symbol}' % cmi_debug )
                                post_symbol_worker(kgraphdb, df_final, news_symbol)
                                
                            except Exception as _fe:
                                logging.info ( f"%s - Exception creating new Symbol node:\n{_fe}" % cmi_debug )
                        # Logic chain structural matching control flow...
                        case True:  # YES stock symbol node DOES exists 
                            logging.info ( f"%s - Symbol node exists: Merging articles -> {news_symbol}" % cmi_debug )
                            # TODO: be carefull updating existing symbol node sentiment metrics
                            # - We should only update sentiment if 100% of articles in KV Cache are analyzed !
                            _attr_count = kgraphdb.check_symbol_attrs(news_symbol)
                            if _attr_count != 17:       # a healthy node has 17 populated node ATTRIBUTES
                                # WARN: 17 is hard coded - see create_sym_node()
                                # If orignal node creation failed, it was created with default min ATTRS = 2
                                # Check + rebuild all node attributes is required !
                                logging.info ( f"%s - Symbol ATTR structure BAD: ({_attr_count} attrs) - rebuilding..." % cmi_debug )
                                print ( f"Symbol ATTR structure BAD ({_attr_count} attrs) - rebuilding..." )
                            else:       # rebuild + update via Cypher MERGE/SET  b/c symbol exists
                                logging.info ( f"%s - Symbol ATTR structure GOOD: ({_attr_count} attrs)" % cmi_debug )

                            try:    # rebuild + update via Cypher MERGE/SET  b/c symbol exists via rebuild=True
                                kg_node_id = kgraphdb.create_sym_node(
                                    news_symbol,
                                    df_final,
                                    sent_ai.summary_report,
                                    sent_ai.summary_metrics,
                                    sent_ai.summary_2v_metrics,
                                    rebuild=True
                                    )
                                logging.info ( f"%s - Post-process node: {kg_node_id} / articles + relationships" % cmi_debug )
                                post_symbol_worker(kgraphdb, df_final, news_symbol)
                                kgraphdb.close_neo4j_auradb("AOP_AURA", kgraphdb.driver)
                                return True

                            except Exception as _ae:
                                logging.errinfoor ( f"%s - Exception rebuilding existing Symbol attribute structure:\n{_ae}" % cmi_debug )    
                        case None:  # ??? needs investigation as to why this corner-case would happen
                            print ("NONE - returned during GraphDB node check!" )
                            kgraphdb.close_neo4j_auradb("AOP_AURA", kgraphdb.driver)
                            return False
                        case 99:
                            print ("EXCEPTION - ocurred during GraphDB node check!" )
                            kgraphdb.close_neo4j_auradb("AOP_AURA", kgraphdb.driver)
                            return False
                        case _:
                            print ("WEIRD return code - during GraphDB node check!" )
                            kgraphdb.close_neo4j_auradb("AOP_AURA", kgraphdb.driver)
                            return False
                except Exception as e:
                        logging.info ( f"%s - Exception checking node entry: {e}" % cmi_debug )
                        return False

# #############################
# Neo4j main Loop logic workflow Helper method
def post_symbol_worker(kgraphdb, df_final, news_symbol):
    """
    Internal Helper method
    For common work that needs to happen once you have created a Symbol Graph node
    """
    cmi_debug = "aop.post_symbol_worker()"+"::"+"Neo4j-Graph_LOOP.#2"
    logging.info( '%s - Checking article nodes...' % cmi_debug )
    _anc, _ans = kgraphdb.create_article_nodes(df_final, news_symbol)   # 2 lists returne
    print ( f"Created {len(_anc)} new article nodes / Skipped existing nodes: {len(_ans) if _ans else '0'}" )
    logging.info( f'%s - Created {len(_anc)} article nodes' % cmi_debug )
    kgraphdb.create_sym_art_rels(news_symbol, df_final, agency="Unknown", author="Unknown", published="Unknown", article_teaser="Unknown")
    logging.info( '%s - Created article relationships -> new parent Symbol node' % cmi_debug )
    kgraphdb.news_agency()
    logging.info( f'%s - Refreshed Yahoo.com node ownership ->  symbol node [ {news_symbol} ]' % cmi_debug )
    return
                
#################################################################################
###############                    QUOTES                            ############
# 3 differnt methods to get a live quote
# NOTE: These 3 routines are *examples* of how to get quotes from the 3 live quote classes::
# TODO: Add a 4th method - via alpaca live API

"""
EXAMPLE: Template Stock Quote code #1
nasdaq.com - live quotes via native JSON API test GET
quote price data is 5 mins delayed
10 data fields provided
"""
def quoute_examples():
    if args['qsymbol'] is not False:
        # ######################## EXAMPLE 1
        cmi_debug = "aop.quote_examples()"+"::"+"TYPE.#1"
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

        print ( "===================== QUOTE TYPE: 1 Nasdaq quote data =======================" )
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
    # ######################## EXAMPLE 2
    marketwatch.com - data via Craw4ai scraper
    quote price data is 15 mins delayed
    """
    if args['qsymbol'] is not False:
        cmi_debug = "aop.quote_examples()"+"::"+"TYPE.#2"
        bc = bc_quote(2, args)                  # create an instance
        bc_symbol = args['qsymbol'].upper()     # what symbol are we getting a quote for?
        print ( " " )
        print ( "================= TYPE #2: MarketWatch Quote  data =======================" )
        print ( f"MarketWatch.com Detailed Quote data for: {bc_symbol}" )
        asyncio.run(bc.c4ai_mwquote(bc_symbol))             # new Crawl4ai scraper
        c = 1
        for k, v in bc.quote.items():
            print ( f"{c} - {k} : {v}" )
            c += 1
        print ( "========================================================" )
        print ( " " )

    """
    # ######################## EXAMPLE 3
    bigcharts.marketwatch.com - data via BS4 scraping
    quote data is 15 mins delayed
    40 data fields provided
    """
    if args['qsymbol'] is not False:
        cmi_debug = "aop.quote_examples()"+"::"+"TYPE.#3"
        bc = bc_quote(5, args)                  # setup an emphemerial dict
        bc_symbol = args['qsymbol'].upper()     # what symbol are we getting a quote for?
        bc.get_quickquote(bc_symbol)            # get the quote
        bc.q_polish()                           # wrangel the data elements
        print ( " " )
        print ( f"Get BIGCharts.com QuickQuote for: {bc_symbol}" )
        print ( "================= quickquote data =======================" )
        c = 1
        for k, v in bc.quote.items():
            print ( f"{c} - {k} : {v}" )
            c += 1
        print ( "========================================================" )
        print ( " " )

    return

if __name__ == '__main__':
    main()
