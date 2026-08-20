#! python3
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
import logging
import time
from rich import print

logging.basicConfig(level=logging.INFO)

#####################################################
class y_unvol:
    """Class to extract Unusual Volume data set from finance.yahoo.com"""
    # global accessors
    uv_dg0 = None        # DataFrame - Full list of top gainers
    uv_dg1 = None        # DataFrame - Ephemerial list of top 10 gainers. Allways overwritten
    uv_dg2 = None        # DataFrame - Top 10 ever 10 secs for 60 secs
    all_tag_tr = None    # BS4 handle of the <tr> extracted data
    rows_extr = 0        # number of rows of data extracted
    ext_req = None       # request was handled by y_cookiemonster
    yti = 0
    cycle = 0            # class thread loop counter

    dummy_url = "https://finance.yahoo.com/markets/stocks/unusual-volume-stocks/"

    yahoo_headers = { \
                        'authority': 'finance.yahoo.com', \
                        'path': '/markets/stocks/most-active/', \
                        'referer': 'https://finance.yahoo.com/markets/', \
                        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="99", "Chromium";v="123"', \
                        'sec-ch-ua-mobile': '"?0"', \
                        'sec-fetch-mode': 'cors', \
                        'sec-fetch-site': 'cross-site', \
                        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36' }

    def __init__(self, yti):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s Instance.#{yti}' % cmi_debug )
        # init empty DataFrame with present colum names
        self.uv_dg0 = pd.DataFrame(columns=[ 'Row', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time'] )
        self.uv_dg1 = pd.DataFrame(columns=[ 'ERank', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time'] )
        self.uv_dg2 = pd.DataFrame(columns=[ 'ERank', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time'] )
        self.yti = yti
        return

#method 1
    def init_dummy_session(self):
        self.dummy_resp0 = requests.get(self.dummy_url, stream=True, headers=self.yahoo_headers, cookies=self.yahoo_headers, timeout=5 )
        hot_cookies = requests.utils.dict_from_cookiejar(self.dummy_resp0.cookies)
        #self.js_session.cookies.update({'A1': self.js_resp0.cookies['A1']} )    # yahoo cookie hack
        return

# method #2
    def ext_get_data(self, yti):
        """
        Leverage BeautifulSoup4 to extract the raw html data from a
        previously rendered webpage - (rendered by y_cookiemonster).
        NOTE:
        - y_cookiemonster now uses playwright for rendering JS
        - ext_req is now the full html page as rendered by playwright (not a response object)
        """
        self.yti = yti
        cmi_debug = __name__+"::"+self.ext_get_data.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        logging.info('%s - ext request pre-processed by cookiemonster...' % cmi_debug )

        # pickup pre-renderd playwright page from (handled by Y_cookiemonster) 
        r = self.ext_req
        logging.info( f"%s - BS4 extractor processing JS page data..." % cmi_debug )
        self.soup = BeautifulSoup(r, 'html.parser')     # playwright rendered data

        self.tag_tbody = self.soup.find('tbody')
        self.tr_rows = self.tag_tbody.find_all("tr")
        logging.info('%s Page processed by BS4 engine' % cmi_debug )
        return
    
# method #3
    def build_uv_df0(self):
        """
        Build-out a fully populated Pandas DataFrame containg all the extracted/scraped fields from the
        html/markup table data Wrangle, clean/convert/format the data correctly.
        """

        cmi_debug = __name__+"::"+self.build_uv_df0.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        time_now = time.strftime("%H:%M:%S", time.localtime() )
        logging.info('%s - Create clean NULL DataFrame' % cmi_debug )
        self.uv_dg0 = pd.DataFrame()             # new df, but is NULLed
        x = 0
        self.rows_extr = int( len(self.tag_tbody.find_all('tr')) )
        self.rows_tr_rows = int( len(self.tr_rows) )
        #logging.info( f'%s - Rows 1 extracted: {self.rows_extr}' % cmi_debug )
        #logging.info( f'%s - Rows 2 extracted: {self.rows_tr_rows}' % cmi_debug )

        for datarow in self.tr_rows:

            # >>>DEBUG<< for whedatarow.stripped_stringsn yahoo.com changes data model...
            y = 1
            print ( f"===================== Debug =========================" )
            print ( f"Data {y}: {datarow}" )
            for i in datarow.find_all("td"):
                print ( f"===================================================" )
                if i.canvas is not None:
                    print ( f"Data {y}: Found Canvas, skipping..." )
                else:
                    print ( f"Data {y}:  {i.text}" )
                    print ( f"Data s/s: {next(i.stripped_strings)}" )
                #logging.info( f'%s - Data: {debug_data.strings}' % cmi_debug )
                y += 1
            print ( f"===================== Debug =========================" )
            # >>>DEBUG<< for when yahoo.com changes data model...
          
            # Data Extractor Generator
            def extr_gen(): 
                for i in datarow.find_all("td"):
                    if i.canvas is not None:
                        yield ( f"canvas" )
                    else:
                        yield ( f"{next(i.stripped_strings)}" )

            ################################ 1 ####################################
            extr_strs = extr_gen()
            co_sym = next(extr_strs)            # 1 : ticker symbol info / e.g "NWAU"
            co_name = next(extr_strs)           # 2 : company name / e.g "Consumer Automotive Finance, Inc."
            mini_chart = next(extr_strs)        # 3 : embeded mini GFX chart tag is: SPARKLINE
            
            rel_volume = next(extr_strs)        # 4 : relative volume 1 day
            price = next(extr_strs)             # 5 : Intraday price e.g "0.0031"
            price_change = next(extr_strs)      # 6 : Intraday price change e.g "+0.23"
            pctg_change = next(extr_strs)       # 7 : Percentage changed
            
            ################################ 2 ####################################
            vol = next(extr_strs)               # 8 : Day Volume with scale indicator e.g "5.748M"
            avg_vol = next(extr_strs)           # 9 : Avg. Dayily vol over 3 months (with scale indicviator)  e.g "61.447M"
            
            ################################ 3 ####################################
            mktcap = next(extr_strs)            # 10 : Market cap (intraday) with scale indicator / e.g "15.753B"
            peratio = next(extr_strs)           # 11 : PE ratio TTM (Trailing 12 months)  e.g "N/A" or "--" or "26.83"

            ################################ 4 ####################################
            pctg_change52w  = next(extr_strs)   # 12  Percentage change across 52-week range (this is a CRITICAL metric)
            price_range52w = next(extr_strs)    # 13  Complex Mini Graphic - IGNORED by Bespin !!!

            ################################ 5 ####################################
            # now wrangle the data...
            co_sym_lj = f"{co_sym:<6}"                                   # left justify TXT in DF & convert to raw string
            co_name_lj = np.array2string(np.char.ljust(co_name, 60) )    # left justify TXT in DF & convert to raw string
            co_name_lj = (re.sub(r'[\'\"]', '', co_name_lj) )            # remove " ' and strip leading/trailing spaces
            price_cl = (re.sub(r'\,', '', price))                        # remove ,
            price_clean = float(price_cl)

            change_sign_multiplier = -1 if str(price_change).strip().startswith("-") else 1
            price_chg_raw = re.sub(r'[\+\-,]', "", str(price_change))           # remove all non numeric tags from the price change number
            price_chg_clean = float(price_chg_raw) * change_sign_multiplier     # convert price_change into a real signed float

            # WARNING: Percentgae change has "%" sign tagged onto number.
            if pctg_change == "N/A" or pctg_change == "0.00%":          # Bad data found
                pct_clean = float(0.0)                                  # Set N/A or 0.00% to a real float = 0.0
                logging.info( f"{cmi_debug} : % CHANGE is BAD, reset to 0.00..." )
            else:
                logging.info( f"{cmi_debug} : % CHANGE {pctg_change} [+-%] tag, stripping..." )
                pct_cl = re.sub(r'[\%\+\-,]', "", pctg_change )         # remove all non numeric tags from the number
                pct_sign_multiplier = -1 if str(pctg_change).strip().startswith("-") else 1
                pct_clean = float(pct_cl) * pct_sign_multiplier         # convert pct_change into a real signed float
                logging.info( f"{cmi_debug} : % CHANGE set to true signed numeric val: {pct_clean}..." )

            ################################ 5 ####################################
            mktcap = (re.sub(r'[N\/A]', '0', mktcap))               # handle N/A
            TRILLIONS = re.search('T', mktcap)
            BILLIONS = re.search('B', mktcap)
            MILLIONS = re.search('M', mktcap)

            if TRILLIONS:
                mktcap_clean = float(re.sub('T', '', mktcap))
                mb = "LT"
                logging.info( f'%s : #{x} : {co_sym_lj} Mkt Cap: TRILLIONS : T' % cmi_debug )

            if BILLIONS:
                mktcap_clean = float(re.sub('B', '', mktcap))
                mb = "LB"
                logging.info( f'%s : #{x} : {co_sym_lj} Mkt cap: BILLIONS : B' % cmi_debug )

            if MILLIONS:
                mktcap_clean = float(re.sub('M', '', mktcap))
                mb = "SM"
                logging.info( f'%s : #{x} : {co_sym_lj} Mkt cap: MILLIONS : M' % cmi_debug )

            if not TRILLIONS and not BILLIONS and not MILLIONS:
                mktcap_clean = 0    # error condition - possible bad data
                mb = "LZ"           # Zillions
                logging.info( f'%s : #{x} : {co_sym_lj} bad mktcap data N/A : Z' % cmi_debug )
                # handle bad data in mktcap html page field

            ################################ 6 ####################################
            # now construct our list for concatinating to the dataframe 
            logging.info( f"%s ============= Data prepared for DF =============" % cmi_debug )

            self.list_data = [[ \
                       x, \
                       re.sub(r'\'', '', co_sym_lj), \
                       co_name_lj, \
                       price_clean, \
                       price_chg_clean, \
                       pct_clean, \
                       mktcap_clean, \
                       mb, \
                       time_now ]]

            ################################ 6 ####################################
            self.df_1_row = pd.DataFrame(self.list_data, columns=[ 'Row', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time' ], index=[x] )
            self.uv_dg0 = pd.concat([self.uv_dg0, self.df_1_row])  
            x+=1

        logging.info('%s - populated new DF0 dataset' % cmi_debug )
        return x        # number of rows inserted into DataFrame (0 = some kind of #FAIL)
                        # sucess = lobal class accessor (y_toplosers.uv_dg0) populated & updated

# method #4
    def topg_listall(self):
        """Print the full DataFrame table list of Yahoo Finance Top Gainers"""
        """Sorted by % Change"""

        cmi_debug = __name__+"::"+self.topg_listall.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        pd.set_option('display.max_rows', None)
        pd.set_option('max_colwidth', 30)
        print ( self.uv_dg0.sort_values(by='Pct_change', ascending=False ) )    # only do after fixtures datascience dataframe has been built
        return

# method #5
    def build_top10(self):
        """
        Get top gainers from main DF (df0) -> temp DF (df1)
        Number of rows to grab is now set from num of rows that BS4 actually extracted (rows_extr)
        df1 is ephemerial. Is allways overwritten on each run
        """

        cmi_debug = __name__+"::"+self.build_top10.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        logging.info('%s - Drop all rows from DF1' % cmi_debug )
        self.uv_dg1.drop(self.uv_dg1.index, inplace=True)
        logging.info('%s - Copy DF0 -> ephemerial DF1' % cmi_debug )
        self.uv_dg1 = self.uv_dg0.sort_values(by='Pct_change', ascending=False ).head(self.rows_extr).copy(deep=True)    # create new DF via copy of top 10 entries
        self.uv_dg1.rename(columns = {'Row':'ERank'}, inplace = True)    # Rank is more accurate for this Ephemerial DF
        self.uv_dg1.reset_index(inplace=True, drop=True)    # reset index each time so its guaranteed sequential
        return

# method #6
    def print_top10(self):
        """
        Prints the Top 10 Dataframe
        Number of rows to print is now set from num of rows that BS4 actually extracted (rows_extr)
        """

        cmi_debug = __name__+"::"+self.print_top10.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        pd.set_option('display.max_rows', None)
        pd.set_option('max_colwidth', 30)
        self.uv_dg1.style.set_properties(**{'text-align': 'left'})
        print ( f"{self.uv_dg1.sort_values(by='Pct_change', ascending=False ).head(self.rows_extr)}" )
        return

# method #7
    def build_tenten60(self, cycle):
        """Build-up 10x10x060 historical DataFrame (df2) from source df1"""
        """Generally called on some kind of cycle"""

        cmi_debug = __name__+"::"+self.build_tenten60.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        self.uv_dg2 = self.uv_dg2.append(self.uv_dg1, ignore_index=False)    # merge top 10 into
        self.uv_dg2.reset_index(inplace=True, drop=True)    # ensure index is allways unique + sequential
        return
