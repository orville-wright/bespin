
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
class benzinga_news:
    """
    Class to crawl data from Benzinga via Craw4ai
    Using CSS Structured JSON Schema method
    Schema is is JSON file: /json/BENZINGA_crawl4ai_schema.json
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
        self.json_file = f"{self.cur_dir}/json/BENZINGA_crawl4ai_schema.json"

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
        
        js_cmds = [
            "window.scrollTo(0, document.body.scrollHeight);",
             "document.querySelector('div.sc-kCzxht.hbQQwA.load-more-button')?.click();"
        ]
        
        config = CrawlerRunConfig(extraction_strategy=extraction_strategy, scan_full_page=True, js_code=js_cmds)

        # This is where we defin the extenal data sources that this scraper will scrape
        # the crawler will calculate the number of urls, so just add them to this is
        # and you are good to go
        # WARN: Each url much match the JSON schema in: BENZINGA_crawl4ai_schema.json or the
        #       craw4ai scraper will not extract any data.
        # For differnt URL's with differnrt doc schema' within the benzingas.com doomain, create a sperate function
        
        urls = [
            "https://www.benzinga.com/news"
        ]
        url = "https://www.benzinga.com/news"
        # WARN: There may be a faster way to do Parallel crawls using: crawler.arun_many()
        # but I havent figurredd out how to use...
        # - crawler.arun_many() with CrawlerRunConfig(extraction_strategy=extraction_strategy)
        # - The results: List[CrawlResult] object seems to not like a url [list]
        # for now, I am looping async with AsyncWebCrawler() as crawler:
        
        count = 0
        async with AsyncWebCrawler() as crawler:
            logging.info(f'%s - doing async webcrawl NOW..' % cmi_debug )
            result = await crawler.arun(
                    url, config=config)

            logging.info(f'%s- Data wrangeling' % cmi_debug )
            for result in result:
                if result.success:
                    data = json.loads(result.extracted_content)
                    #print (f"({json.dumps(data, indent=2)}")
                    logging.info( f'%s - cycle over data list..' % cmi_debug )
                    for idx, item in enumerate(data):
                        logging.info( f'{item}')
                        try:
                            t = (count, item)
                            try:
                                item["Title"]
                                item["Ext_url"]
                                self.DF_data.append(t)      # NO key errors = append to working list
                            except KeyError as missing_key:
                                logging.info(f'%s - BAD Data / Missing JSON Key: {missing_key}' % cmi_debug )
                                # skip elemenmt. Its not what we want. Its a partial dupe/add/something bad
                            count += 1
                        except IndexError:
                            logging.info(f'%s - Failed to unwind JSON Dict package' % cmi_debug )

        self.DB_insert_data = {} 
        logging.info(f"%s - Build final DB insertion dict..." % cmi_debug )
        #logging.info(f"{cmi_debug} - {json.dumps(data, indent=2)}")
        #print (f"{self.DF_data}")
        dedupe_set = set()
        dedupe_set = set()
        realigned_v = 0
        for dict in self.DF_data:                   # this is where the final dataset can be accessed
            v = dict[0]                             # tupple elelemt 0 = index num
            w = dict[1]                             # tupple elelemt 1 = dict{}
            url = dict[1]["Ext_url"]                # get url
            uh = hashlib.sha256(url.encode())       # set hash encoding of the url
            ihash = uh.hexdigest()                  # compute hash
            if ihash not in dedupe_set:             # dedupe membership test
                dedupe_set.add(ihash)               # add ihash to dupe_set for next memebrship test
                w["urlhash"] = ihash                # insert new urlhash element into the dict
                #print (f"Adding row: {v}...{w}")
                row = {realigned_v: w}              # form final dict data structure
                self.DB_insert_data.update(row)     # append row
                realigned_v += 1
            else:
                logging.info(f"%s - Duplicate data: at {v} / skipping..." % cmi_debug )
                pass
 
        # This is where we will insert the each element in the LMDB KV Database
        # after all data scraping has completed, not in the middle of network transaction/scraping
        # as that will slow down the data scaper.       
        # if you want to dump the dict...
        logging.info(f"{cmi_debug} -\n{json.dumps(self.DB_insert_data, indent=2)}")
        logging.info(f"{cmi_debug} - complete Benzinga data craw/scrape" )
        print (f"[Complete] + Benzinga News Data Extractor | Rows: {len(self.DB_insert_data)}" )
        return int(len(self.DB_insert_data))