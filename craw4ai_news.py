#! python


from requests_html import HTMLSession
from bs4 import BeautifulSoup
import pandas as pd
#import modin.pandas as pd
import numpy as np
import re
import logging
import argparse
import time
import hashlib
#from rich import print
#from rich.markup import escape

import asyncio
import os
import json
import time
from pathlib import Path
from typing import List

from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode, CrawlResult
from crawl4ai import JsonCssExtractionStrategy
from crawl4ai import LLMConfig
from crawl4ai import BrowserConfig


class craw4ai:
    """Class to extract Top Gainer data set from finance.yahoo.com"""

     
##############################################################################
    def __init__(self, yti):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s - Instantiate.#{yti}' % cmi_debug )
        # init empty DataFrame with present colum names
        self.yti = yti
        return


    async def css_struct_extract_schema(self, this_url):
        """Extract structured data using CSS selectors"""
        # Check if schema file exists
        print ( f"### DEBUG: Loading YF schema / recvd URL: {this_url}" )
        schema_file_path = f"{__cur_dir__}/YF_MainNews_schema.json"
        if os.path.exists(schema_file_path):
            with open(schema_file_path, "r") as f:
                schema = json.load(f)
                extraction_strategy = JsonCssExtractionStrategy(schema)
                config = CrawlerRunConfig(extraction_strategy=extraction_strategy, delay_before_return_html=30, verbose=True)
            
                # Use the fast CSS extraction (no LLM calls during extraction)
                async with AsyncWebCrawler(config=BrowserConfig(headless=True, verbose=True, use_persistent_context=True, user_data_dir="/home/dbrace/code/bespin/.chrome_cache/")) as crawler:
                #async with AsyncWebCrawler() as crawler:
                    resultx: List[CrawlResult] = await crawler.arun(
                        "https://finance.yahoo.com/", config=config
                    )

                    for result in resultx:
                        print(f"#### DEBUG: URL: {result.url}")
                        print(f"#### DEBUG: Success: {result.success}")
                        if result.success:
                            #print ( f"{result.cleaned_html}" )
                            data = json.loads(result.extracted_content)
                            print(json.dumps(data, indent=2))
                        else:
                            print("Failed to extract structured data")
        else:
            # Generate schema using LLM (one-time setup)
            print ( f"### DEBUG: FAILED to load YF schema..." )
            return 1
                    # Create no-LLM extraction strategy with the generated schema
        return


