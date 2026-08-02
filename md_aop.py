#! python3

import websockets.client  # Force-loads the client module into the websockets namespace
import asyncio
import random
import pandas as pd
import logging
import argparse
import time
import threading
import re
from urllib.parse import urlparse
from rich import print
import pprint


# my private classes & methods
from y_cookiemonster import y_cookiemonster
from data_engines_fundamentals.alpaca_md import alpaca_md
from bigcharts_md import bc_quote
from nasdaq_uvoljs import un_volumes
from nasdaq_wrangler import nq_wrangler
from nasdaq_quotes import nquote
from y_daylosers import y_daylosers
from y_smallcaps import smallcap_screen
from y_techevents import y_techevents
from y_topgainers import y_topgainers

"""
Disbaled for now
Market DATA Extractor engines
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

# NEWS Data Extractor engines
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
lmdb_env = {}               # global LMDB KV database (cross classes accessor)  
logging.basicConfig(level=logging.INFO)
work_inst = 0
yti = 1

parser = argparse.ArgumentParser(prog="Aop", description="Entropy apperture engine")
parser.add_argument('-q','--quote', help='Get ticker price action quote', action='store', dest='qsymbol', required=False, default=False)
parser.add_argument('-s','--screen', help='Small cap screener logic', action='store_true', dest='bool_scr', required=False, default=False)
parser.add_argument('-t','--tops', help='show top ganers/losers', action='store_true', dest='bool_tops', required=False, default=False)
parser.add_argument('-u','--unusual', help='unusual up & down volume', action='store_true', dest='bool_uvol', required=False, default=False)
parser.add_argument('-v','--verbose', help='verbose error logging', action='store_true', dest='bool_verbose', required=False, default=False)
parser.add_argument('-x','--xray', help='dump detailed debug data structures', action='store_true', dest='bool_xray', required=False, default=False)
parser.add_argument('--news-cycle', help='Full news cycle extract from every new data engine', action='store_true', dest='news_cycle', required=False, default=False)

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

        print ( "===================== Nasdaq quote data =======================" )
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
    marketwatch.com - data via Craw4ai scraper
    quote price data is 15 mins delayed
    """
    if args['qsymbol'] is not False:
        cmi_debug = "aop.quote_examples()"+"::"+"TYPE.#2"
        bc = bc_quote(2, args)                  # create an instance
        bc_symbol = args['qsymbol'].upper()     # what symbol are we getting a quote for?
        asyncio.run(bc.c4ai_mwquote(bc_symbol))             # new Crawl4ai scraper
        print ( " " )
        print ( f"Get MarketWatch.com Detailed Quote data for: {bc_symbol}" )
        print ( "================= MarketWatch Quote  data =======================" )
        c = 1
        for k, v in bc.quote.items():
            print ( f"{c} - {k} : {v}" )
            c += 1
        print ( "========================================================" )
        print ( " " )

    """
    EXAMPLE #3
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

"""
    #################################################################################
    # ALPACA API Integration - Live quotes and bars ################################
    #################################################################################

    #
    # ALPACA API INTEGRATION
    # Live quotes via Alpaca API - real-time data during market hours
    # OHLCV bars data with 1-minute granularity
    #
    
    if args['alpaca_symbol'] is True:
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
        
    if args['alpaca_bars'] is True:
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
    if args['sec_symbol'] is True:
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
    if args['polygon_symbol'] is True:
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
    if args['tiingo_symbol'] is True:
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

"""
