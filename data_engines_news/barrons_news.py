#! python

import os
import json
import logging
import hashlib

from pathlib import Path
from typing import List
from pathlib import Path
from typing import List
  
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, CrawlResult
from crawl4ai import JsonCssExtractionStrategy

logging.basicConfig(level=logging.INFO)

# ###################### Main class
class barrons_news:
    """
    Class to crawl data from Barrons via Craw4ai
    Using CSS Structured JSON Schema method
    Schema is is JSON file: /json/BARRONS_crawl4ai_schema.json
    """
    # global class attributes
    inst_id = 0
    cur_dir = None
    json_file = None
    DF_data = []
    DB_insert_data = {}
    
    def __init__(self, inst_id):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s - Instantiate.#{inst_id}' % cmi_debug )

        #self.args = global_args                            # Only set once per INIT. all methods are set globally
        self.inst_id = inst_id
        __cur_dir__ = Path(__file__).parent
        self.cur_dir = __cur_dir__
        self.json_file = f"{self.cur_dir}/json/BARRONS_crawl4ai_schema.json"

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
            logging.info( f'%s - FAIELD to load JSON craw4ai schema file: [ {self.json_file} ]...' % cmi_debug )
            return 1

        logging.info( f'%s - INIT craw4ai extraction strategy...' % cmi_debug )
        extraction_strategy = JsonCssExtractionStrategy(schema)
        logging.info( f'%s - INIT craw4ai Crawler RunConfig()...' % cmi_debug )
        config = CrawlerRunConfig(extraction_strategy=extraction_strategy)

        # This is where we defin the extenal data sources that this scraper will scrape
        # the crawler will calculate the number of urls, so just add them to this is
        # and you are good to go
        # WARN: Each url much match the JSON schema in: BARRONS_crawl4ai_schema.json or the
        #       craw4ai scraper will not extract any data.
        # For differnt URL's with differnrt doc schema' within the barrons.com doomain, create a sperate function
        
        urls = [
            "https://www.barrons.com/real-time/1",
            "https://www.barrons.com/real-time/2",
            "https://www.barrons.com/real-time/3",
            "https://www.barrons.com/real-time/4",
            "https://www.barrons.com/real-time/5"
        ]
      
        # WARN: There may be a faster way to do Parallel crawls using: crawler.arun_many()
        # but I havent figurredd out how to use...
        # - crawler.arun_many() with CrawlerRunConfig(extraction_strategy=extraction_strategy)
        # - The results: List[CrawlResult] object seems to not like a url [list]
        # for now, I am looping async with AsyncWebCrawler() as crawler:
        
        count = 0
        for i in range(len(urls)):
            async with AsyncWebCrawler() as crawler:
                logging.info(f'%s - doing async webcrawl NOW..' % cmi_debug )
                results: List[CrawlResult] = await crawler.arun(
                        urls[i], config=config)

                logging.info(f'%s- Data wrangeling' % cmi_debug )
                for result in results:
                    if result.success:
                        data = json.loads(result.extracted_content)
                        logging.info( f'%s - cycle over data list {i}..' % cmi_debug )
                        for idx, item in enumerate(data):
                            try:
                                t = (count, item)
                                self.DF_data.append(t)      # append to working list
                                count += 1
                            except IndexError:
                                logging.info(f'%s - Failed to unwind JSON Dict package' % cmi_debug )

        self.DB_insert_data = {} 
        logging.info(f'%s - Build final DB insertion dict...' % cmi_debug )
        for dict in self.DF_data:                   # this is where the final dataset can be accessed
            v = dict[0]                             # tupple elelemt 0
            w = dict[1]                             # tupple elelemt 1
            url = dict[1]["Ext_url"]                # get url
            uh = hashlib.sha256(url.encode())       # set hash encoding of the url
            ihash = uh.hexdigest()                  # compute hash
            w["urlhash"] = ihash                    # insert new urlhash element into the dict
            row = {v: w}                            # form final dict data structure
            self.DB_insert_data.update(row)         # append row
 
        # This is where we will insert the each element in the LMDB KV Database
        # after all data scraping has completed, not in the middle of network transaction/scraping
        # as that will slow down the data scaper.       
        # if you want to dump the dict...
        #    print (f"{json.dumps(self.DB_insert_data, indent=2)}" )
            
        logging.info(f"%s - complete Barrons data craw/scrap..." % cmi_debug )
        print (f"[Complete] + Barrons News Data Extractor | Rows: {len(self.DB_insert_data)}" )
        return int(len(self.DB_insert_data))