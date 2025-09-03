#! python3

from bs4 import BeautifulSoup
import logging
import pandas as pd
from requests_html import HTMLSession
import requests
#import modin.pandas as pd

# logging setup
logging.basicConfig(level=logging.INFO)


#####################################################

class y_cookiemonster:
    """Class to extract Top Gainer data set from finance.yahoo.com"""

    # global accessors
    tg_df0 = ""          # DataFrame - Full list of top gainers
    tg_df1 = ""          # DataFrame - Ephemerial list of top 10 gainers. Allways overwritten
    tg_df2 = ""          # DataFrame - Top 10 ever 10 secs for 60 secs
    tl_dfo = ""
    tl_df1 = ""
    tl_df2 = ""

    all_tag_tr = ""      # BS4 handle of the <tr> extracted data
    yti = 0
    cycle = 0           # class thread loop counter

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
        
##############################################################################
    def __init__(self, yti):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s - Instantiate.#{yti}' % cmi_debug )
        # init empty DataFrame with present colum names
        self.tg_df0 = pd.DataFrame(columns=[ 'Row', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time'] )
        self.tg_df1 = pd.DataFrame(columns=[ 'ERank', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time'] )
        self.tg_df2 = pd.DataFrame(columns=[ 'ERank', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time'] )
        self.yti = yti
        return

##############################################################################
# method #1
    def get_scap_data(self):
        """
        Connect to finance.yahoo.com and extract (scrape) the raw sring data out of
        current a DEBUF function for General news scraping
        """

        cmi_debug = __name__+"::"+self.get_scap_data.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        url="https://finance.yahoo.com"

        js_session = HTMLSession()                  # Create a new session        
        with js_session.get(url) as r:          # must do a get() - NO setting cookeis/headers)
            logging.info(f'%s  - Simple HTML Request get()...' % cmi_debug )

        #r = requests.get("https://finance.yahoo.com/" )
        logging.info('%s - page read, not JS rendering...' % cmi_debug )
        #r.text
        r.html.render(timeout=10)

        #self.soup = BeautifulSoup(r.text, 'html.parser')
        # ATTR style search. Results -> Dict
        # <tr tag in target merkup line has a very complex 'class=' but the attributes are unique. e.g. 'simpTblRow' is just one unique attribute
        #logging.info('%s - save data object handle' % cmi_debug )
        #self.tag_tbody = self.soup.find('tbody')
        #self.all_tag_tr = self.soup.find_all(attrs={"class": "simpTblRow"})   # simpTblRow
        #self.tr_rows = self.tag_tbody.find(attrs={"class": "simpTblRow"})
        #print ( f">>> DEBUG:\n {r.text}" )

        logging.info('%s - close url handle' % cmi_debug )
        r.close()
        return r

###########################################################################################
# method #2
    def get_js_data(self, js_url):
        """
        Connect to finance.yahoo.com and open a Javascript Webpage
        Process with Javascript engine and return JS webpage handle
        Optionally the Javascript engine can render the webspage as Javascript and
        and then hand back the processed JS webpage. - This is currently didabled
        """

        cmi_debug = __name__+"::"+self.get_js_data.__name__+".#"+str(self.yti)
        logging.info('%s - IN' % cmi_debug )
        js_url = "https://" + js_url

        #test_url = "https://www.whatismybrowser.com/detect/is-javascript-enabled"
        #test_url = "https://www.javatester.org/javascript.html"
        #test_url = "https://finance.yahoo.com/screener/predefined/small_cap_gainers/"

        logging.info( f"%s - Javascript engine setup..." % cmi_debug )
        logging.info( f"%s - URL: {js_url}" % cmi_debug )
        logging.info( f"%s - Init JS_session HTMLsession() setup" % cmi_debug )

        js_session = HTMLSession()
        try:
            with js_session.get( js_url ) as self.js_resp0:
                logging.info( f"%s - JS_session.get() sucessful: {js_url}" % cmi_debug )
                logging.info( f"%s - js.render() page now..." % cmi_debug )
                self.js_resp0.html.render()
                hot_cookies = requests.utils.dict_from_cookiejar(self.js_resp0.cookies)
                logging.info( f"%s - Swap {len(self.js_resp0.cookies)} JS cookies into yahoo_headers" % cmi_debug )
                js_session.cookies.update(self.yahoo_headers)
        finally:
            js_session.close()

        return self.js_resp0
