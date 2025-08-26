#!/usr/bin/env python3

import os
import json
import logging
import hashlib

from pathlib import Path
from typing import List
  
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, CrawlResult
from crawl4ai import JsonCssExtractionStrategy

logging.basicConfig(level=logging.INFO)

# ###################### Main class
class hedgeweek_news:
    """
    Class to crawl data from Forbes via Crawl4ai
    Using CSS Structured JSON Schema method
    Schema is in JSON file: /json/HEDGEWEEK_crawl4ai_schema.json
    """
    # global class attributes
    inst_id = 0
    cur_dir = None
    json_file = None
    DF_data = []
    DB_insert_data = {}
    url = None
    
    def __init__(self, inst_id):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s - Instantiate.#{inst_id}' % cmi_debug )

        self.inst_id = inst_id
        __cur_dir__ = Path(__file__).parent
        self.cur_dir = __cur_dir__
        self.json_file = f"{self.cur_dir}/json/HEDGEWEEK_crawl4ai_schema.json"
        logging.info( f'%s - JSON craw4ai schema file: [ {self.json_file} ]...' % cmi_debug )
        
    # ###################### method : 1
    async def craw4ai_str_schema_extr(self):
        # WARN: This is an asyncio function. must be called by asyncio.run()
        cmi_debug = __name__+"::"+self.craw4ai_str_schema_extr.__name__+".#"+str(self.inst_id)
        logging.info( f'%s - JSON craw4ai schema file: [ {self.json_file} ]...' % cmi_debug )        
        schema_file_path = f"{self.json_file}"
        if os.path.exists(schema_file_path):
            with open(schema_file_path, "r") as f:
                schema = json.load(f)
            logging.info( f'%s - craw4ai JSON schema\n{json.dumps(schema, indent=2)}.' % cmi_debug ) 
        else:
            logging.info( f'%s - FAILED to load JSON craw4ai schema file: [ {self.json_file} ]...' % cmi_debug )
            return 1

        logging.info( f'%s - INIT craw4ai extraction strategy...' % cmi_debug )
        extraction_strategy = JsonCssExtractionStrategy(schema)
        logging.info( f'%s - INIT craw4ai Crawler RunConfig()...' % cmi_debug )

        config = CrawlerRunConfig(extraction_strategy=extraction_strategy, scan_full_page=True)
        # This is where we define the external data sources that this scraper will scrape
        # WARN: Each url must match the JSON schema in: FORBES_crawl4ai_schema.json or the
        #       crawl4ai scraper will not extract any data.
        
        # use urls[list] if we need ot loop through a list of differnt urls with same SCHEMA.

        self.url = "https://www.hedgeweek.com/news/"
        # Multiple pages for this News sevvice can be explcitily accessed by
        # manipulating the URL to advance to tghe next page. NO JS or Button code needed
        

        # helper funtion for AsyncWebCrawler() as crawler:
        self.kt = 0
        def multi_page_wrangeler(self, cycle, result):
            count = int(cycle)
            cmi_debug = __name__+"::"+"data_wrangler_sanitizer"+".#"+str(self.inst_id)
            logging.info(f'%s - Data wrangling' % cmi_debug )

            if result.success:
                logging.info( f'%s - extract json content Len: {len(result.extracted_content)}...' % cmi_debug )
                data = json.loads(result.extracted_content)
                #print (f"{data}")
                for idx, item in enumerate(data):
                    logging.info( f"{cmi_debug} - cycle [ {idx} / Analyze data: {str(item)[:30]}..." )
                    kt = 0
                    try:
                        t = (count, item)
                        try:
                            item["Title"]
                            item["Ext_url"]
                            logging.info( f'%s - Validated good JSON keys...' % cmi_debug )
                            self.DF_data.append(t)      # NO key errors = append to working list
                            self.kt += 1
                        except KeyError as missing_key:
                            logging.info(f'%s - BAD Data / Missing JSON Key: {missing_key}' % cmi_debug )
                            # skip element. Do NOT add to DF_data - Its not what we want. Its a partial dupe/add/something bad
                        except Exception as e:
                            logging.info(f'%s - ERROR: {e}' % cmi_debug )
                            # Some kind of error - skipp and do NOT add to DB_data
                        count += 1
                    except IndexError:
                            logging.info(f'%s - Failed to unwind JSON Dict package' % cmi_debug )
                logging.info( f"{cmi_debug} - Wrangler sanitizer complete: [ {count} ] / {self.kt}] / {result.success}" )
            else:
                logging.info(f'%s - JSON data payload is empty' % cmi_debug )
                pass
                
            return
                            
        #self.page_cycle = 0
        for i in range (1,5):
            cmi_debug = __name__+"::"+"async_data_get"+".#"+str(self.inst_id)
            url = self.url+str(i)
            async with AsyncWebCrawler() as crawler:
                logging.info(f'%s - doing async webcrawl NOW..' % cmi_debug )
                result = await crawler.arun(
                        url, config=config)
                multi_page_wrangeler(self, i, result)
            logging.info( f"{cmi_debug} - Multi pager cycler complete: [ {i} ] / {result.success}" )

    ##########################################################
        cmi_debug = __name__+"::"+"prepare_final_DF_data"+".#"+str(self.inst_id)
        self.DB_insert_data = {} 
        logging.info(f"%s   - Build final DB insertion dict..." % cmi_debug )
        #logging.info(f"%s - Dump JSON data:\n{json.dumps(data, indent=2)}"  % cmi_debug )
        #print (f"{self.DF_data}")
        dedupe_set = set()
        realigned_v = 0
        for dict in self.DF_data:                   # this is where the final dataset can be accessed
            logging.info(f'{cmi_debug}   - Analyse final data structure...' )
            v = dict[0]                             # tuple element 0 = index num
            w = dict[1]                             # tuple element 1 = dict{}
            url = dict[1]["Ext_url"]                # get url
            # Handle relative URLs
            if url.startswith('/'):
                url = 'https://www.forbes.com' + url
            uh = hashlib.sha256(url.encode())       # set hash encoding of the url
            ihash = uh.hexdigest()                  # compute hash
            if ihash not in dedupe_set:             # dedupe membership test
                dedupe_set.add(ihash)               # add ihash to dupe_set for next membership test
                w["urlhash"] = ihash                # insert new urlhash element into the dict
                w["Ext_url"] = url                  # update with full URL
                logging.info( f'{cmi_debug}   - Add Row [ {realigned_v} ] to DB with Data: {str(w)[:30]}' )
                row = {realigned_v: w}              # form final dict data structure
                self.DB_insert_data.update(row)     # append row
                realigned_v += 1
            else:
                logging.info(f"{cmi_debug}   - Duplicate data: at {v} / skipping..." )
                pass
 
        # This is where we will insert each element in the LMDB KV Database
        # after all data scraping has completed, not in the middle of network transaction/scraping
        # as that will slow down the data scraper.       
        logging.info(f"{cmi_debug} -\n{json.dumps(self.DB_insert_data, indent=2)}")
        logging.info(f"{cmi_debug} - complete Forbes data crawl/scrape" )
        print (f"[Complete] + FXstreet News Data Extractor | Rows: {len(self.DB_insert_data)} / {self.kt}" )
        return int(len(self.DB_insert_data))