#! python3

# import argparse
# import asyncio

import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import logging
from urllib.parse import urlparse
from rich import print

from ml_sentiment import ml_sentiment
from ml_urlhinter import url_hinter
from ml_yf_nlp_news_engine import yfnews_reader
# previously: yfnews_reader() sourced from ml_yf_news_c4::yfnews_reader

# logging setup
logging.basicConfig(level=logging.INFO)

# Gobals for NewsAgeResolver class
#
# Compiled regular expression once at module level
# - matches "3 hours ago", "1 day ago", "45 minutes ago", "2 weeks ago", "3 months ago", "1 year ago" etc.
# Case-insensitive, tolerant of surrounding whitespace.
_SKIM_AGE_PATTERN = re.compile(
    r"^\s*(\d+)\s+(minute|hour|day|week|month|year)s?\s+ago\s*$",
    re.IGNORECASE,
)

# Approximate day-counts for calendar-fuzzy units. These are estimates
# by nature ("1 month ago" on Yahoo is already a heavy quantization),
# so fixed approximations are appropriate here.
_DAYS_PER_MONTH = 30.44
_DAYS_PER_YEAR = 365.25


# ML / NLP section #############################################################
class ml_nlpreader:
    """
    Class to identify, rank, classify stocks NEWS articles
    Updated to work as async and crawl4ai + BS4
    
    """

    # global accessors
    args = []               # class dict to hold global args being passed in from main() methods
    dateageresolver = None  # singleton class of News Article Age date Resolver() 
    cycle = 0               # class thread loop counter
    ml_yfn_dataset = None   # Yahoo Finance News reader instance
    yfn = None              # class of @ml_yahoofinews_crawl4ai.py/yfnews_reader
    yfn_uh = None           # URL Hinter instance for the YFN reader
    yti = 0
    caller = None

    def __init__(self, yti, global_args, caller):
        cmi_debug = __name__+"::" + self.__init__.__name__
        logging.info(f'%s   Instantiate.#{yti} via {caller}' % cmi_debug)
        #self.yfn = yfnews_reader(1, "DUMMY0", global_args )        # instantiate our own class of YFN
        self.args = global_args
        self.yti = yti
        return

    def share_ageresolver(self):
        """
        DELETE ME... we're doing this in nlp_read_one() now
        Share the singleton class of News Article Age date Resolver()  with the YFN reader instance
        Must instantiate the NewsAgeResolver() class before calling this method
        """
        self.yfn.dateageresolver = self.dateageresolver
        return
    
    # ##############################################################################
    async def nlp_read_one(self, news_symbol, global_args):
        """
        DEPTH -> 0 and -> 1
        Main controler for depth 0 and 1 data extraction

        Async version of nlp_read_one that uses crawl4ai
        The machine will now read!
        Read finance.yahoo.com / News 'Brief headlines' using crawl4ai
        Reads ALL news articles for only ONE stock symbol (why this method is title "read_one").
        """
        print(" ")
        print(f"ML (NLP) AI / News Sentiment for 1 symbol [ {news_symbol} ]")
        self.args = global_args
        cmi_debug = __name__+"::" + self.nlp_read_one.__name__
        logging.info(f'%s   - ENTRY.#{self.yti}' % cmi_debug)
        news_symbol = str(news_symbol).upper()
        
        self.yfn = yfnews_reader(1, news_symbol, global_args )          # instantiate our own class of YFN (frm ml_yf_nlp_news_engine)
        ml_yfn_dataset = self.yfn    
        self.yfn.dateageresolver = self.dateageresolver                 # Share singleton class of News Article Age date Resolver() with this YFN instance

        ml_yfn_dataset.form_endpoint(news_symbol)                       # extablish the exct news url endpoint
        logging.info(f"%s - Form NEWS endpoint for {news_symbol} + globalize url_hinter @ #1" % cmi_debug)
        self.yfn_uh = url_hinter(1, self.args)                          # instantiate URL hinter 
        ml_yfn_dataset.yfn_uh = self.yfn_uh                     # give UFN reader access to the URL hinter instance

        # 3 Main steps execuete @here : Depth -> 0 + Depth -> 1
        # print a report of the Depth 0 Top Level news skim run
        _url_hash0 = await ml_yfn_dataset.yahoofin_news_depth0(0)   # scrape NOW @ Depth 0 yahoofin_news_depth0()

        if _url_hash0:												# Depth: 0
            articles_found = ml_yfn_dataset.list_news_candidates_depth0(news_symbol, 0, 1, _url_hash0)
            # Depth: 1 - updates ml_ingest
            eval_state, bad_url_count = ml_yfn_dataset.eval_news_feed_stories(news_symbol)
            self.ml_yfn_dataset = ml_yfn_dataset                    # set global dataset -> ml_yfn_dataset            
            print(" ")
            print(f"Skim Depth: 0 - Candidates: {articles_found} / Maybe good: {len(ml_yfn_dataset.ml_ingest)} / (Bad urls: {bad_url_count})")
            print("========================================================================================")
  
            # DEBUG: xray debug
            if self.args.get('bool_xray', False):
                ml_yfn_dataset.dump_ml_ingest()
                return articles_found
        else:
            logging.error( "%s - No Top level NEWS articles found to skim !!" % cmi_debug)    
            return 0

    # ##############################################################################
    def nlp_summary_report(self, yti, ml_idx):
        """
        CRITICAL:
        - Assumes ml_ingest has already been pre-populated with Master candidate article list
        NOTE:
        - Reads 1 (ONE) article ONLY from the ml_ingest{} DB and processes it...
        - Executes Depth 2 analysis via ml_yfn_dataset::interpret_page_depth2() 
        - returns the thint heuristic inferred from interpret_page_depth2()
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
            logging.error(f'%s - No data in ML-Ingest DB for index: {ml_idx}' % cmi_debug)
            return 9.9

        sn_row = self.ml_yfn_dataset.ml_ingest[ml_idx]      # real a row of data
        
        # ################# 1: Real valid news article
        if sn_row['type'] == 0:  # REAL valid news article
            print(f"Analyzing...   {sn_row['symbol']} / Valid News article: {ml_idx} / ({self.ml_yfn_dataset.articles_found}) Candidates")
            t_url = urlparse(sn_row['url'])
            uhint, uhdescr = self.yfn_uh.uhinter("00", t_url)
            thint = sn_row['thint']
            logging.info(f"%s - Logic.#0 Hints for url: [ t:0 / u:{uhint} / h: {thint} ] / {uhdescr}" % cmi_debug)
            
            # Do deep analysis on the page @ Depth 2
            r_uhint, r_thint, r_xturl = self.ml_yfn_dataset.interpret_page_depth2(ml_idx, sn_row)
            logging.info(f"%s - Inferred conf: {r_xturl}" % cmi_debug)
            p_r_xturl = urlparse(r_xturl)
            inf_type = self.yfn_uh.confidence_lvl(thint)
            print(f"Article type:  [ +{uhint} ] / {sn_row['url']}")
            ##-debug print(f"Origin URL:    [ {t_url.netloc} ] / {inf_type[0]} / ", end="")
            ##-debug print(f"{locality_code.get(inf_type[1])}")
            uhint, uhdescr = self.yfn_uh.uhinter("02", p_r_xturl)
            ##-debug print(f"Target URL:    [ {p_r_xturl.netloc} ] / {uhdescr} / ", end="")
            ##-debug print(f"{locality_code.get(uhint)} [ u:{uhint} ]")
            return thint
        
        # ################# 1: Fake news article - Micro-ad
        elif sn_row['type'] == 1:
            print(f"Analyzing...   {sn_row['symbol']} / Fake Micro-ad art: {ml_idx} - AI will not eval sentiment")
            t_url = urlparse(sn_row['url'])
            uhint, uhdescr = self.yfn_uh.uhinter("10", t_url)
            thint = sn_row['thint']
            logging.info(f"%s       - Logic.#1 hint origin url: t:1 / u:{uhint} / h: {thint} {uhdescr}" % cmi_debug)
            
            r_uhint, r_thint, r_xturl = self.ml_yfn_dataset.interpret_page_depth2(ml_idx, sn_row) # Depth 2 analysis of page
            
            try:
                url_test = len(r_xturl)
                logging.info(f"%s       - Logic.#1 hint ext url: {r_xturl}" % cmi_debug)
                p_r_xturl = urlparse(r_xturl)
                inf_type = self.yfn_uh.confidence_lvl(thint)
                print(f"Article type:  [ +{uhint} ] / {sn_row['url']}")
                ##-debug print(f"Origin:        [ {t_url.netloc} ] / {inf_type[0]} / {uhdescr} /", end="")
                ##-debug print(f"{locality_code.get(inf_type[1], 'in flux')}")
                uhint, uhdescr = self.yfn_uh.uhinter("11", p_r_xturl)
                ##-debug print(f"Hints:         {uhdescr} / ", end="")
                ##-debug print(f"{locality_code.get(uhint, 'in flux')} [ u:{uhint} ]")
                logging.info( "%s - skipping..." % cmi_debug)
                return thint
            except Exception as e:
                logging.info(f"%s       - BAD artile URL {url_test} : {e}" % cmi_debug)
                return thint

        # ################# 2: Video story
        elif sn_row['type'] == 2:
            print(f"Analyzing...   {sn_row['symbol']} / Video article: {ml_idx} - AI will not eval sentiment")
            t_url = urlparse(sn_row['url'])
            thint = sn_row['thint']
            inf_type = self.yfn_uh.confidence_lvl(thint)
            uhint, uhdescr = self.yfn_uh.uhinter("20", t_url)
            print(f"Article type:  [ +{uhint} ] / Video stream cannot be processed by AI model")
            ##-debug print(f"URL:           {sn_row['url']}")
            ##-debug print(f"Origin:        [ {t_url.netloc} ] / {inf_type[0]} / ", end="")
            ##-debug print(f"{locality_code.get(inf_type[1], 'in flux')}")
            logging.info( "%s - skipping..." % cmi_debug)
            return thint
        
        # ################# 3: External publication
        elif sn_row['type'] == 3:
            print(f"Analyzing...   {sn_row['symbol']} / Random Filler item: {ml_idx} - AI will not eval sentiment")
            t_url = urlparse(sn_row['url'])
            thint = sn_row['thint']
            inf_type = self.yfn_uh.confidence_lvl(thint)
            uhint, uhdescr = self.yfn_uh.uhinter("30", t_url)
            print(f"Article type:  [ +{uhint} ] / Unreliable external article data")
            ##-debug print(f"URL:           {sn_row['url']}")
            ##-debug print(f"Origin:        [ {t_url.netloc} ] / {inf_type[0]} / ", end="")
            ##-debug print(f"{locality_code.get(inf_type[1], 'in flux')}")
            logging.info( "%s - skipping..." % cmi_debug)
            return thint

        # ################# 5: Yahoo Premium subscription ad
        elif sn_row['type'] == 5:
            t_url = urlparse(sn_row['url'])
            thint = sn_row['thint']
            inf_type = self.yfn_uh.confidence_lvl(thint)
            uhint, uhdescr = self.yfn_uh.uhinter("50", t_url)
            print(f"Article: {ml_idx} - {inf_type[0]}: 5 - NOT an NLP candidate")
            logging.info( "%s - skipping..." % cmi_debug)
            return thint
        
        # ################# 9: Placeholder - Not yet defined
        elif sn_row['type'] == 9:
            print(f"Article: {ml_idx} - Type 9 - NOT yet defined - NOT an NLP candidate")
            logging.info( "%s - skipping..." % cmi_debug)
            thint = sn_row['thint']
            return thint
        
        # ################# + : catchall for Bad data
        else:
            print(f"Article: {ml_idx} - ERROR BAD Data | unknown article type: {sn_row['type']}")
            logging.info( "%s - #? skipping..." % cmi_debug)
            thint = sn_row['thint']
            return thint

# ################## Class #2
# A critical helper class for...
# - handleing article dates at the Depth-0 and Depth-1 levels
#
class NewsAgeResolver:
 
    def mark_skim_fetch(self):
        """
        Stamp the anchor timestamp for a skim-page fetch.
 
        Call this at the MOMENT the skim page is fetched - NOT at
        LMDB-write time. All relative ages on that page are relative
        to when Yahoo rendered it; resolving against a later "now"
        introduces silent drift equal to the fetch->write gap.
 
        Returns the anchor so it can also be carried explicitly
        alongside the skim results if preferred.
        """
        self.skim_anchor_utc = datetime.now(timezone.utc)
        return self.skim_anchor_utc
 
    def resolve_skim_age(self, age_text, anchor_utc=None):
        """
        Convert a Depth--zero skim age string ("3 hours ago") into an
        estimated absolute publish timestamp, anchored at skim fetch time.
 
        Parameters
        ----------
        age_text   : raw age string from the skim headline list
        anchor_utc : timezone-aware datetime of the skim fetch.
                     Defaults to self.skim_anchor_utc (set by
                     mark_skim_fetch). Passing it explicitly is
                     preferred when resolving in a later pipeline stage.
 
        Returns
        -------
        dict with explicit string state (matches Bespin state style):
          state              : "resolved" | "empty" | "unreadable" | "junk"
                               ("junk" = zero-quantity age, e.g.
                               "0 days ago" - the empirical signature
                               of ad/firewall stub rows in the skim list)
          published_utc      : ISO-8601 UTC estimate  (None if not resolved)
          published_epoch    : float epoch seconds    (None if not resolved)
          age_seconds        : seconds before anchor  (None if not resolved)
          precision          : quantization unit of the estimate
                               ("minute"|"hour"|"day"|"week"|"month"|"year")
          provenance         : "skim_estimate"  - ALWAYS this value here.
                               The Depth-1 empirical date parser writes
                               "article_empirical" and overwrites this
                               record's timestamp fields when available.
          raw_age_text       : the original input, preserved for telemetry
 
        Precision semantics
        -------------------
        Yahoo floors relative ages: "3 hours ago" means the true publish
        time lies in [anchor - 4h, anchor - 3h]. The estimate returned
        is the FLOOR interpretation (anchor - 3h), i.e. the youngest
        time consistent with the text. Expect the Depth-1 empirical
        date to be equal-or-older than this estimate by up to one
        `precision` unit. Log (estimate - empirical) deltas as telemetry
        to confirm the flooring assumption against real data.
        """
        anchor = anchor_utc if anchor_utc is not None else getattr(
            self, "skim_anchor_utc", None)
        if anchor is None:
            raise ValueError(
                "No anchor timestamp. Call mark_skim_fetch() at skim "
                "fetch time, or pass anchor_utc explicitly.")
        if anchor.tzinfo is None:
            raise ValueError(
                "anchor_utc must be timezone-aware (naive datetimes "
                "invite silent local-vs-UTC bugs).")
 
        # ---- classify the input into an explicit string state ----
        if age_text is None or not str(age_text).strip():
            state = "empty"
            m = None
        else:
            m = _SKIM_AGE_PATTERN.match(str(age_text))
            if m is None:
                state = "unreadable"
            elif int(m.group(1)) == 0:
                # Empirical Bespin finding: junk skim rows (clickbait
                # ads, firewall stubs) always carry a zero age
                # ("0 days ago"). Zero-quantity ages are therefore a
                # junk marker, NOT a fresh article. Without this state,
                # "0 days ago" would resolve to published == anchor,
                # i.e. junk timestamped as maximally fresh - the worst
                # possible input to recency-decay weighting.
                state = "junk"
            else:
                state = "process"
 
        match state:
            case "empty":
                return self._unresolved("empty", age_text)
 
            case "unreadable":
                # Malformed / non-age text (some ad rows carry no age
                # string at all) - a secondary junk-filter signal.
                return self._unresolved("unreadable", age_text)
 
            case "junk":
                # Zero-quantity age: known junk-row signature.
                # Deliberately NEVER produces a timestamp.
                return self._unresolved("junk", age_text)
 
            case "process":
                quantity = int(m.group(1))
                unit = m.group(2).lower()
 
                match unit:
                    case "minute":
                        delta = timedelta(minutes=quantity)
                    case "hour":
                        delta = timedelta(hours=quantity)
                    case "day":
                        delta = timedelta(days=quantity)
                    case "week":
                        delta = timedelta(weeks=quantity)
                    case "month":
                        delta = timedelta(days=quantity * _DAYS_PER_MONTH)
                    case "year":
                        delta = timedelta(days=quantity * _DAYS_PER_YEAR)
                    case _:
                        # Regex guarantees a known unit; defensive only.
                        return self._unresolved("unreadable", age_text)
 
                published = anchor - delta
                return {
                    "state": "resolved",
                    "published_utc": published.isoformat(),
                    "published_epoch": published.timestamp(),
                    "age_seconds": delta.total_seconds(),
                    "precision": unit,
                    "provenance": "skim_estimate",
                    "raw_age_text": age_text,
                }
 
            case _:
                return self._unresolved("unreadable", age_text)
 
    @staticmethod
    def _unresolved(state, age_text):
        """Uniform shape for non-resolved outcomes - same keys, None values."""
        return {
            "state": state,
            "published_utc": None,
            "published_epoch": None,
            "age_seconds": None,
            "precision": None,
            "provenance": "skim_estimate",
            "raw_age_text": age_text,
        }
 
    def parse_empirical_published(self, date_text):
        """
        Parse the Depth-1 in-article date, e.g.
        "Tue, August 11, 2026 at 1:51 PM PDT", into the SAME record
        shape as resolve_skim_age, with provenance "article_empirical".
 
        When this succeeds for an article, its timestamp fields should
        OVERWRITE the skim estimate in LMDB (keep raw_age_text from the
        skim record for telemetry comparison).
 
        Note on timezone abbreviations: %Z parsing of "PDT"/"PST" is
        unreliable across platforms, so the abbreviation is mapped
        explicitly and the datetime is built in America/Los_Angeles,
        then converted to UTC.
        """
        if date_text is None or not str(date_text).strip():
            return self._unresolved_empirical("empty", date_text)
 
        text = str(date_text).strip()
 
        # Split off the trailing timezone abbreviation explicitly.
        tz_map = {
            "PDT": ZoneInfo("America/Los_Angeles"),
            "PST": ZoneInfo("America/Los_Angeles"),
            "EDT": ZoneInfo("America/New_York"),
            "EST": ZoneInfo("America/New_York"),
            "UTC": timezone.utc,
            "GMT": timezone.utc,
        }
        parts = text.rsplit(" ", 1)
        if len(parts) == 2 and parts[1].upper() in tz_map:
            body, tz = parts[0], tz_map[parts[1].upper()]
        else:
            body, tz = text, ZoneInfo("America/Los_Angeles")  # Yahoo default
 
        try:
            naive = datetime.strptime(body, "%a, %B %d, %Y at %I:%M %p")
        except ValueError:
            return self._unresolved_empirical("unreadable", date_text)
 
        aware = naive.replace(tzinfo=tz)
        published = aware.astimezone(timezone.utc)
        return {
            "state": "resolved",
            "published_utc": published.isoformat(),
            "published_epoch": published.timestamp(),
            "age_seconds": None,   # empirical dates are absolute, not relative
            "precision": "minute",
            "provenance": "article_empirical",
            "raw_age_text": date_text,
        }
 
    @staticmethod
    def _unresolved_empirical(state, date_text):
        return {
            "state": state,
            "published_utc": None,
            "published_epoch": None,
            "age_seconds": None,
            "precision": None,
            "provenance": "article_empirical",
            "raw_age_text": date_text,
        }
