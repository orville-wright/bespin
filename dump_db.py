#!/usr/bin/env python3

import argparse
from datetime import datetime
import json
import lmdb
import logging
from pprint import pprint
import random
from rich import print
import string
import sys
import zstandard as zstd

from typing import Any, Dict, List, Tuple, Optional


from datastore_eng_LMDB import lmdb_io_eng

logging.basicConfig(level=logging.INFO)
global args
args = {}

parser = argparse.ArgumentParser(prog="Aop", description="LMBD Maintence tool")
parser.add_argument('-a','--articles', help='dump all article data', nargs="*", dest='bool_article', required=True, default=False)
parser.add_argument('-d','--deep', help='Deep data dump of values', action='store_true', dest='bool_data', required=False, default=False)
parser.add_argument('-i','--init', help='create new emplt KV db', action='store_true', dest='bool_init', required=False, default=False)
parser.add_argument('-k','--key', help='filter output by KEY substring', action='store', dest='key_filter', required=False, default=None)
parser.add_argument('-v','--verbose', help='verbose error logging', action='store_true', dest='bool_verbose', required=False, default=False)


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

################# 1
def dump_lmdb_by_key(lmdb_instance, key_filter):
    """Filter LMDB entries by stock ticker (element #2) or URL hash fragment (element #3).

    Key format: {db_id}.{ticker}.{url_hash}
      e.g.  0001.XRX.f308c6c74e14976ac6e940c20a329c5e063cf5cfde402d591cfcd28ace1c2b2d

    Matching rules:
      - ticker  : case-insensitive exact match  (e.g. -k XRX)
      - url_hash: case-sensitive substring match (e.g. -k f308c6)
    """
    try:
        with lmdb_instance.RO_env.begin() as txn:
            cursor = txn.cursor()
            total = 0
            matches = 0
            filter_upper = key_filter.upper()
            for key, value in cursor:
                key_str = key.decode('utf-8')
                total += 1

                parts = key_str.split('.')
                if len(parts) != 3:
                    continue                         # skip any malformed keys

                db_id, ticker, url_hash = parts

                ticker_match   = filter_upper == ticker.upper()
                urlhash_match  = key_filter in url_hash
                if not ticker_match and not urlhash_match:
                    continue

                matched_on = "ticker" if ticker_match else "url_hash"
                #value_str = value.decode('utf-8')

                _v_dict = json.loads(value.decode('utf-8'))
                working_article = _v_dict["article"]
                print ( f"LMBD Database: {db_id} / Ticker: {ticker} / Filtering by:{matched_on}" ) 
                print ( f"============================ News article:  {working_article} ====================================" )
                print( f"URL hash:  {_v_dict["urlhash"]}" )
                print( f"Sentences: {_v_dict["scentence"]} / Paragraphs: {_v_dict["paragraph"]} / Randoms: {_v_dict["random"]}" )
                print ( f"Chunk metrics: @ <= 512 - Chunks: {_v_dict["chunk_count"]+1} / Positive: {_v_dict["positive_count"]} Neutral: {_v_dict["neutral_count"]} Negative: {_v_dict["negative_count"]}")
                print ( f"Text metrics:  Total characters: {_v_dict["chars_count"]} / Total words: {_v_dict["total_words"]} Total tokens: {_v_dict["total_tokens"]}" )
                print ( f"Chunk analytics")

                _v_key = 0  # chunk dict allways starts at 000 - ensure reset for each run
                for _v_chunk_dict in range(int(_v_dict["chunk_count"])+1):  # chunk count is 0 indexed, so add 1 to include the last chunk
                    _v_key = f"{_v_chunk_dict:03}"
                    try: 
                        _v_sub_dict = (_v_dict[_v_key])
                    except Exception as e:
                        print (f"Error Type: {type(e).__name__}")
                        break
                    else:
                        print ( f"  ======================================= {working_article} : {_v_key} =======================================" )
                        print ( f"  Chunk dict: {_v_key} / Chunk id: {_v_sub_dict["chunk"]} / Ticker: {_v_sub_dict["symbol"]}" )
                        print ( f"  N-grams:    {_v_sub_dict["n-grams"]} / Tokens: {_v_sub_dict["tokenz"]} / Alphas: {_v_sub_dict["alphas"]}" )
                        print ( f"  Chunk sentement:    {_v_sub_dict["sent_type"]} / Sentment score: {_v_sub_dict["sent_score"]} / Chunker used: {_v_sub_dict["trct_state"]}" )
                        matches += 1
                        _v_key = 0
                        _v_chunk_dict = 0

                print (" ")
            print(f"\nKey filter '{key_filter}': {matches} match(es) from {total} total entries")
    except lmdb.Error as e:
        print(f"LMDB Error: {e}")
    except Exception as e:
        print(f"dump_lmdb_by_key Error: {e}")
    return 0

################# 2
def dump_lmdb_deep(lmdb_instance, key_filter):
    """Print full values for all LMDB entries whose key contains key_filter.
    Values are parsed and pretty-printed as JSON when possible.
    Requires key_filter — call only after validating --key is set."""
    try:
        with lmdb_instance.RO_env.begin() as txn:
            cursor = txn.cursor()
            total = 0
            matches = 0
            for key, value in cursor:
                key_str = key.decode('utf-8')
                total += 1
                if key_filter not in key_str:
                    continue
                value_str = value.decode('utf-8')
                matches += 1
                print(f"\n{'='*70}")
                print(f"[{matches:03}] KEY: {key_str}")
                print(f"{'='*70}")
                try:
                    parsed = json.loads(value_str)
                    print(json.dumps(parsed, indent=2))
                except (json.JSONDecodeError, ValueError):
                    print(value_str)
            print(f"\n{'='*70}")
            print(f"Deep dump '{key_filter}': {matches} match(es) from {total} total entries")
    except lmdb.Error as e:
        print(f"LMDB Error: {e}")
    except Exception as e:
        print(f"dump_lmdb_deep Error: {e}")
    return 0

################# 3
def dump_lmdb_basic(lmdb_instance):
    # you must manually open the DB yourself first...
    try:
        with lmdb_instance.RO_env.begin() as txn:
            cursor = txn.cursor()
            count = 0
            for key, value in cursor:
                key_str = key.decode('utf-8')
                value_str = value.decode('utf-8')
                print(f"{count:03} / KEY: {key_str} / {value_str[:40]}{'...' if len(value_str) > 40 else ''}")
                count += 1            
        return 1
    except lmdb.Error as e:
        print(f"LMDB Open Error: {e}")
        return 2
    except Exception as e:
        print(f"Dump RO mode - Error Exception: {e}")
        return 0

################# 4
def dump_lmdb_articles(lmdb_instance, ticker_filter=None):
    # you must manually open the DB yourself first...
    try:
        with lmdb_instance.RO_env.begin() as txn:
            cursor = txn.cursor()
            count = 0
            for key, value in cursor:
                key_str = key.decode('utf-8')
                count += 1
                if ticker_filter not in key_str:
                    continue
                value_str = value.decode('utf-8')
                matches += 1
                print (f"=============================== Begin Article ====================================" )
                print(f"[{matches:03}] KEY: {key_str}")
                try:
                    _read_zstd_blob = json.loads(value_str['zstd_blob'])
                    _decompressor = zstd.ZstdDecompressor()
                    _pure_article_text = _decompressor.decompress(_read_zstd_blob).decode('utf-8')
                    print( f"Article text: {_pure_article_text}" )
                    print (f"=============================== End Article ====================================" )
                except (json.JSONDecodeError, ValueError):
                    print(value_str)
            print(f"\n{'='*70}")
            print(f"Deep dump '{ticker_filter}': {matches} match(es) from {count} total entries")
    except lmdb.Error as e:
        print(f"LMDB Open Error: {e}")
        return 2
    except Exception as e:
        print(f"Dump RO mode - Error Exception: {e}")
        return 0
    
################# Main()
lmdb_dbname = "LMDB_0001"
lmdb_inst = lmdb_io_eng("RO_DUMP", lmdb_dbname, args)
lmdb_inst.open_lmdb_RO("RO_DUMP")


if args['bool_data'] is True and args['key_filter'] is None:
    print("ERROR: --deep requires --key to also be specified")
    parser.print_help()
    sys.exit(1)

# parser.add_argument('-n','--newsai-sent', help='AI NLP News sentiment AI for 1 stock', nargs="*", dest='newsai_sent', required=False, default=False)
if args['bool_article'] is True:
    print("Dumping article data for all entries...")
    dump_lmdb_articles(lmdb_inst, args['ticker_filter'])

if args['bool_data'] is True and args['key_filter'] is not None:
    print(f"Deep dump for key filter: '{args['key_filter']}'")
    dump_lmdb_deep(lmdb_inst, args['key_filter'])
elif args['key_filter'] is not None:
    print(f"Filtering LMDB entries by key: '{args['key_filter']}'")
    dump_lmdb_by_key(lmdb_inst, args['key_filter'])
else:
    dump_lmdb_basic(lmdb_inst)

if args['bool_init'] is True:
    print ( "Initializing New Empty LMDB KV Database..." )
    lmdb_dbname = "LMDB_0001"
    lmdb_inst = lmdb_io_eng(1, lmdb_dbname, args)
    env = lmdb_inst.open_lmdb_RW(1)
    env.close()

