#!/usr/bin/env python3

import argparse
import base64
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

parser = argparse.ArgumentParser(prog="dump_db", description="LMBD Data Mgmt tool")
parser.add_argument(
        '-a','--articles',
        help='Dump article text for a ticker. [symbol, count num|0]', 
        nargs="*", dest='bool_articles', required=False, default=False )

parser.add_argument(
    '-b','--basic',
    help='Simple 1-line view of all LMBD KV entries',
    action='store_true', dest='bool_basic', required=False, default=False )

parser.add_argument(
    '-d','--deep',
    help='Deep dump of values. Requires -k|--key TICKER/URLHASH', 
    action='store_true', dest='bool_deep', required=False, default=False )

parser.add_argument(
    '-i','--init',
    help='Reset empty KV db of all data',
    action='store_true', dest='bool_init', required=False, default=False )

parser.add_argument(
    '-k','--key',
    help='Filter output by KEY sub-string',
    action='store', dest='key_filter', required=False, default=None )

parser.add_argument(
    '-v','--verbose',
    help='Verbose error logging',
    action='store_true', dest='bool_verbose', required=False, default=False )

parser.add_argument(
    '-x','--xray',
    help='Full dict record XRAY. Requires -k|--key TICKER/URLHASH',
    action='store_true', dest='bool_xray', required=False, default=False )


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
# book_deep
# dump_lmdb_by_key(lmdb_inst, args['key_filter'], urlhash=_urlhash)
def dump_lmdb_by_key(lmdb_instance, key_filter, urlhash):
    """
    Filter LMDB entries by stock ticker (LMDB primark key element #2) or URL hash fragment (primary key element #3).
    Full Key format: {db_id}.{ticker}.{url_hash}
    
    e.g.  0001.YDES.e6e1c98884593bef4bb921a3b0231aaeb620add2eb78651bacee004684618719
    e.g.  0001.XRX.f308c6c74e14976ac6e940c20a329c5e063cf5cfde402d591cfcd28ace1c2b2d
        PK-element-1 : 001 (not filterable as yet)
        PK-element-2 : symbol
        PK-element-3 : urlhash
    
    Note:
    - Filtering by symbol will print all LMBD entries that match the symbol
    - Filtering by urlhash will only print the single article that matches the urlhash (useful if youre looking at 1 article).
        Filter Matching rules:
        - ticker  : case-insensitive exact match  (e.g. -k XRX)
        - url_hash: case-sensitive substring match (e.g. -k f308c6)

    Will print Parent dict and all blocklet chunk sub-dicts for each matching entry. 
    Will not print article the contents of the article's full stored text field (i.e. data in "ZSTD article blob"

    3rd cmline param is Yes/No to dump the ZSTD compressed article text field. If Yes, will decompress and print the full article text.
    1 = Yes, 0 = No. Default = 0
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
                working_article = _v_dict["article"]        # article number
                print ( f"LMBD Database: {db_id} / Ticker: {ticker} / Filtering by:{matched_on}" ) 
                print ( f"============================ Begin News article:  {working_article} ====================================" )
                print(json.dumps(_v_dict, indent=4))
                print ( f"============================ End News article:  {working_article} ====================================" )

                #print( f"URL hash:  {_v_dict["urlhash"]}" )
                #print( f"Skimmed date :  {_v_dict["skim_age"]}" )
                #print( f"Published date ISO:  {_v_dict["iso_age"]}" )
                #print( f"Sentences: {_v_dict["scentence"]} / Paragraphs: {_v_dict["paragraph"]} / Randoms: {_v_dict["random"]}" )
                #print ( f"Chunk blocklets: {_v_dict["chunk_count"]} / Positive: {_v_dict["positive_count"]} Neutral: {_v_dict["neutral_count"]} Negative: {_v_dict["negative_count"]}")

                try:
                    _zstd_article_text = _v_dict["zstd_blob"]  # test if dic has ZSTD compressed article entry
                    print ( f"ZSTD article blob: {_zstd_article_text[:100]}{'...' if len(_zstd_article_text) > 1 else ''}" )
                    if urlhash == 1:
                        b64_binencd_cmprssd_data = base64.b64decode(_zstd_article_text)
                        decompressor = zstd.ZstdDecompressor()
                        zstd_blob_uncompressed = decompressor.decompress(b64_binencd_cmprssd_data).decode('utf-8')
                        print ( f"{zstd_blob_uncompressed}" )
                except KeyError:
                    print ( "LMDB entry has no ZSTD compressed article entry." )

                print ( f"\nText metrics:  Total characters: {_v_dict["chars_count"]} / Total words: {_v_dict["total_words"]} Total tokens: {_v_dict["total_tokens"]}" )
                print ( "\nChunk sub-dict data")

                _v_key = 0  # chunk sub dict allways starts at 000 - ensure reset for each run

                for key, sub_dict in _v_dict.items():
                    if isinstance(sub_dict, dict):
                        print(f"=== Chunk sub-dict key: {key}")
                        # Iterate through the key-value pairs of the sub-dict
                        cycle = int(0)
                        for k, v in sub_dict.items():
                            if cycle < 3:
                                print(f"  {k}: {v}\t", end="")
                                cycle += 1
                            else:
                                print ("")
                                print(f"  {k}: {v}\t", end="")
                                cycle = int(1)
                        print()  # Empty line for spacing

                        _v_dict = json.loads(value.decode('utf-8'))
                        working_article = _v_dict["article"]        # article number
                        print ( f"LMBD Database: {db_id} / Dumping {article_limit} Articles entries for: {ticker_filter}" ) 
                        print ( f"================= News article:  {working_article} / Item {matches} of {article_limit}  ====================================" )
                        try:
                            _zstd_article_text = _v_dict["zstd_blob"]  # test if dic has ZSTD compressed article entry
                            b64_binencd_cmprssd_data = base64.b64decode(_zstd_article_text)
                            #print ( f"ZSTD article blob: {_zstd_article_text[:100]}{'...' if len(_zstd_article_text) > 1 else ''}" )
                            decompressor = zstd.ZstdDecompressor()
                            zstd_blob_uncompressed = decompressor.decompress(b64_binencd_cmprssd_data).decode('utf-8')
                            #= zstd.ZstdDecompressor().decompress(_zstd_article_text).decode('utf-8')
                            print ( f"{zstd_blob_uncompressed}" )                                                        
                            matches += 1
                            if matches > article_limit:
                                print ( f"Limit of {article_limit} reached for ticker filter '{ticker_filter}'. Stopping article dump.\n" )
                                break
                            total += 1
                        except KeyError:
                            print ( "LMDB entry has no ZSTD compressed article entry." )
                            total += 1
                        except Exception as e:
                            print ( f"Error decompressing ZSTD article blob: {e}" )
                            total += 1
                
                print (" ")
            print(f"\nKey filter '{key_filter}': {matches} match(es) from {total} total entries")
    except lmdb.Error as e:
        print(f"LMDB Error: {e}")
    except Exception as e:
        print(f"dump_lmdb_by_key Error: {e}")
    return 0

################# 2
def dump_lmdb_xray(lmdb_instance, key_filter):
    """
    Print an xray of values for 1 explcit LMDB enty that matches the key_filter.
    Will also dump the article text field
    Values are parsed and pretty-printed as standard JSON when possible.

    Requires key_filter — call only after validating --key is set.
    """
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
        print(f"dump_lmdb_xray Error: {e}")
    return 0

# ################################## 2
# -a or --article
# parser.add_argument('-n','--newsai-sent', help='AI NLP News sentiment AI for 1 stock', nargs="*", dest='newsai_sent', required=False, default=False)

def dump_lmdb_articles(lmdb_instance, ticker_filter, article_limit):
    try:
        with lmdb_instance.RO_env.begin() as txn:
            cursor = txn.cursor()
            if article_limit is None:
                article_limit = int(0)  # No limit if not specified

            total = int(0)
            matches = int(1)
            for key, value in cursor:
                key_str = key.decode('utf-8')
                total += 1
                parts = [p.strip() for p in key_str.split('.')]
                if len(parts) != 3:
                    total += 1
                    print ( "Bad keys - not 3 parts !!")
                    continue                         # skip any malformed keys

                db_id, ticker_symbol, url_hash = parts
                if ticker_symbol.upper() != ticker_filter:
                    total += 1
                    continue
                else:
                    _v_dict = json.loads(value.decode('utf-8'))
                    working_article = _v_dict["article"]        # article number
                    print ( f"LMBD Database: {db_id} / Dumping {article_limit} Articles entries for: {ticker_filter}" ) 
                    print ( f"================= News article:  {working_article} / Item {matches} of {article_limit}  ====================================" )
                    try:
                        _zstd_article_text = _v_dict["zstd_blob"]  # test if dic has ZSTD compressed article entry
                        b64_binencd_cmprssd_data = base64.b64decode(_zstd_article_text)
                        #print ( f"ZSTD article blob: {_zstd_article_text[:100]}{'...' if len(_zstd_article_text) > 1 else ''}" )
                        decompressor = zstd.ZstdDecompressor()
                        zstd_blob_uncompressed = decompressor.decompress(b64_binencd_cmprssd_data).decode('utf-8')
                        #= zstd.ZstdDecompressor().decompress(_zstd_article_text).decode('utf-8')
                        print ( f"{zstd_blob_uncompressed}" )                                                        
                        matches += 1
                        if matches > article_limit:
                            print ( f"Limit of {article_limit} reached for ticker filter '{ticker_filter}'. Stopping article dump.\n" )
                            break
                        total += 1
                    except KeyError:
                        print ( "LMDB entry has no ZSTD compressed article entry." )
                        total += 1
                    except Exception as e:
                        print ( f"Error decompressing ZSTD article blob: {e}" )
                        total += 1

        print (" ")
        print( f"Ticker filter '{ticker_filter}': {matches} match(es) from {total} total entries")
    except lmdb.Error as e:
        print( f"LMDB Error: {e}")
    except Exception as e:
        print( f"dump_lmdb_by_key Error: {e}")
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


# ################################## main()
# differnt ways to dump the LMDB...
# 1. dump_lmdb_by_key       : bool_deep        : -d or --deep (reqwuires a key filter)
# 2. dump_lmdb_xray         : bool_xray        : -x or --xray
# 3. dump_lmdb_basic()      : no switches / no options
# 4. dump_lmdb_articles()   : bool_articles     : -a or --articles
#    NOTE: -k or --key = your supplied filter

################# Main()
lmdb_dbname = "LMDB_0001"
lmdb_inst = lmdb_io_eng("RO_DUMP", lmdb_dbname, args)
lmdb_inst.open_lmdb_RO("RO_DUMP")
# Instance attributes
# db_open_state = {}  #
# lmdb_env = {}       # LMDB global instance, opened @ main::newsai_sent
# RO_env = {}         # LMDB environment instance for RO mode
# RW_env = {}         # LMDB environment instance for RW mode

# -b' or '--basic'
# bool_basic
# if args.bool_verbose is True: 

if args.bool_basic is True:
    print( "List entire LMDV in simple format... ")
    dump_lmdb_basic(lmdb_inst)
    lmdb_inst.close_lmdb("BASIC_DUMP")
    sys.exit(0)

# -a or --articles
# bool_articles
elif args.get('bool_articles'): 
    _filter = None
    articles_list = args['bool_articles']
    try:
        _ticker_symbol = articles_list[0]
        match _ticker_symbol:
            case None:
                print("No ticker symbol filter provided.")
                parser.print_help()
                sys.exit(3)
            # Matches if the first element is a string
            case str(symbol):
                _filter = symbol.upper()
                # Check length before accessing index 1 to avoid another IndexError
                if len(articles_list) > 1 and articles_list[1] != 0:
                    article_limit = int(articles_list[1])
                    print(f"Dumping article TEXT for {article_limit} {_filter} entries...")
                    dump_lmdb_articles(lmdb_inst, _filter, article_limit)
                    lmdb_inst.close_lmdb("ARTICLES_DUMP")
                    sys.exit(0)
                else:
                    print(f"Dumping article TEXT for all {_filter} entries...")
                    dump_lmdb_articles(lmdb_inst, _filter, 0)
                    lmdb_inst.close_lmdb("ARTICLES_DUMP")
                    sys.exit(0)
            case _:
                print(f"Bad parameters: {articles_list}")
                parser.print_help()
                sys.exit(1)

    except (IndexError, ValueError) as e:
        print(f"ERROR: Invalid parameters provided: {e}")
        parser.print_help()
        sys.exit(2)


# -d or --deep
# requries a key filter -k or --key
# key can be symbol ticker or urlhash
# - if ticker symbol, it will recursively print all records for that symbol
# - if urlhash, it will explicitly match just that urlhash (which are mostly 99% unique)
#elif args['bool_deep'] is True:
elif args.bool_deep is True: 
    if args['key_filter'] is not None:
        _key_options = args['key_filter']
        try:
            _urlhash = _key_options[1]
            match _urlhash:
                case None:
                    print("No URLhash id found - not filtering for specific article !")
                case str(_urlhash):
                    print(f"Dumping article TEXT for URLhash: {_urlhash}")
                case _:
                    print ("Fall thru DEFAULT on key parameter...")

            print( f"Full dump filtered by key: {args['key_filter']}")
            dump_lmdb_by_key(lmdb_inst, args['key_filter'], urlhash=_urlhash)
            lmdb_inst.close_lmdb("DEEP_DUMP")
            sys.exit(0)

        except (IndexError, ValueError) as e:
            print(f"ERROR: Invalid parameters provided: {e}")
            parser.print_help()
            sys.exit(2)
    
    else:
        print ( "ERROR: Deep dump requries a key filter use  -k or --key option]..." )
        print ( " " )
        parser.print_help()
        sys.exit(1)

# -x or --xray
# dump_lmdb_xray
elif args['bool_xray'] is True:
    if args['key_filter'] is not None:
        dump_lmdb_xray(lmdb_inst, args['key_filter'])
        lmdb_inst.close_lmdb("XRAY_DUMP")
        sys.exit(0)
    else:
        print ( f"ERROR: XRAY dump of single LMDB Value entry requries a key filter [add -k or --key option] !" )
        print ( f" " )
        parser.print_help()
        sys.exit(1)

elif args['bool_init'] is True:
    lmdb_dbname = "LMDB_0001"
    print ( f"Init empty LMDB by dropping data from: {lmdb_inst.RO_env} @ {lmdb_inst.db_path}{lmdb_dbname}..." )
    # god damn it... close it first !!!
    lmdb_inst.close_lmdb("INIT_CLOSE") 
    lmdb_inst.drop_lmdb_RW("INIT_DROP")
else:
    print ( f"ERROR: No valid dump option selected. Please choose one of the following:" )
    parser.print_help()
    sys.exit(1)