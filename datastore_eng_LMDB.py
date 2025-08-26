#!/usr/bin/env python3

import argparse
from datetime import datetime
import json
import lmdb
import logging
import random
from rich import print
import string
import sys
from typing import Any, Dict, List, Tuple, Optional



# ML / NLP section #############################################################
class lmdb_io_eng:
    """
    Class to manage Global access to the LMDB K/V Database
    Assumption:
    - Create a class for each LMDB database.
    - So we dont have to provide DB name every time (just during INIT)
    """

    # global attribute
    args = []           # class dict to hold global args being passed in from main() methods
    cr_package = None   # full reslts dict{} of dict_processor ruin
    cursor = None       # current LMDB Transaction Cursor - not sure if this is safe to store as global attribute
    cycle = 0           # class thread loop counter
    db_path = "datastore/"       # filesystem path to locale of LMDB K/V Database
    db_name = None      # LMDB Database instance name
    db_open_state = 0   # 0=closed, 1=open
    env = None          # current opened LMDB database I/O Transaction handle
    sent_ai = None      # sentiment_ai instance, set by main() before calling kv_cache_engine()
    yti = 0
    _n = 0             # negative sentiment count
    _p = 0             # positive sentiment count
    _z = 0             # neutral sentiment count
        
################# init
    def __init__(self, yti, db_name, global_args):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s - Instantiate.#{yti} LMDB instance: {db_name}' % cmi_debug )
        self.args = global_args                            # Only set once per INIT. all methods are set globally
        self.yti = yti
        self.db_name = db_name
        self._n = 0
        self._p = 0
        self._z = 0
        return

################# 1
    def open_lmdb_RO(self, yti):
        cmi_debug = __name__+"::"+self.open_lmdb_RO.__name__+".#"+str(self.yti)
        logging.info( f'%s   - open_lmdb_RO.#{self.yti} DB Instance: {self.db_name}' % cmi_debug )
        db_inst = self.db_path + self.db_name
        try:
            self.env = lmdb.open(db_inst, readonly=True)     # map_size: Maximum size DB = 1GB
            logging.info( f'%s   - Successfully opened KVstore - READ-ONLY mode' % cmi_debug )
            logging.info( f'%s   - Instance remains globally open: {self.db_name}' % cmi_debug )
            return self.env
        except lmdb.Error as e:
            print(f"LMDB Open Error: {e}")
            print(f"Database: {db_inst} - not found.")
            return 0
        except Exception as e:
            print(f"Error open_lmdb_RO Exception: {e}")
            return 0
            
################# 2
    def open_lmdb_RW(self, yti):
        cmi_debug = __name__+"::"+self.open_lmdb_RW.__name__+".#"+str(self.yti)
        logging.info( f'%s    - open_lmdb_RO.#{self.yti} Instance: {self.db_name}' % cmi_debug )
        db_inst = self.db_path+self.db_name

        try:
            self.env = lmdb.open(db_inst, map_size=1024*1024*1024, readonly=False)     # map_size: Maximum size DB = 1GB
            logging.info( f'%s    - Successfully openend KVstore - READ-WRITE mode.#{self.yti} {self.db_name}' % cmi_debug )
            logging.info( f'%s    - KVstore remains globally open.#{self.yti} Instance: {self.db_name}' % cmi_debug )
            return self.env
        except lmdb.Error as e:
            print(f"LMDB Open Error: {e}")
            print(f"Database: {db_inst} - not found.")
            return 0
        except Exception as e:
            print(f"Open RW mode - Error Exception: {e}")
            return 0
            
################# 3
    def dump_lmdb_RO(self, yti):
        cmi_debug = __name__+"::"+self.dump_lmdb_RO.__name__+".#"+str(self.yti)
        logging.info( f'%s    - dump_lmdb.#{self.yti} DB Instance: {self.db_name}' % cmi_debug )
        db_inst = self.db_path + self.db_name
        
        try:
            self.env = lmdb.open(db_inst, readonly=True)     # map_size: Maximum size DB = 1GB
            logging.info( f'%s   - Successfully opened KVstore - READ-ONLY mode.#{self.yti} {self.db_name}' % cmi_debug )
            logging.info( f'%s   - KVstore remains globally open.#{self.yti} instance: {self.db_name}' % cmi_debug )
            with self.env.begin() as txn:
                cursor = txn.cursor()
                count = 0
                for key, value in cursor:
                    key_str = key.decode('utf-8')
                    value_str = value.decode('utf-8')
                    print(f"{count:03} / KEY: {key_str} -> VALUE: {value_str[:50]}{'...' if len(value_str) > 50 else ''}")
                    count += 1            
            return 1
        except lmdb.Error as e:
            print(f"LMDB Open Error: {e}")
            print(f"Database: {db_inst} - not found.")
            return 0
        except Exception as e:
            print(f"Dump RO mode - Error Exception: {e}")
            return 0

################# 4
    def drop_lmdb_RW(self, yti):
        cmi_debug = __name__+"::"+self.drop_lmdb_RW.__name__+".#"+str(self.yti)
        logging.info( f'%s   - drop_lmdb_RW.#{self.yti} Instance: {self.db_name}' % cmi_debug )
        db_inst = self.db_path+self.db_name

        try:
            self.env.close()
            db_inst = self.db_path + self.db_name
            self.env = lmdb.open(db_inst, max_dbs=0)     # max_dbs=0 for default DB only
            _db0 = self.env.open_db(key=None)            # default DB addressed by key=None, returns handle of default DB
            with self.env.begin(write=True) as txn:
                txn.drop(_db0, delete=False)            # delete all keys in db0, do not delete db0 virtual named DB)
            self.env.close()
            logging.info( f'%s - DROPPED default database - READ-WRITE mode.#{self.yti} {self.db_name}' % cmi_debug )
            return 1
        except lmdb.Error as e:
            print(f"LMDB Open Error: {e}")
            print(f"Database: {db_inst} - not found.")
            return 0
        except Exception as e:
            print(f"Drop RW mode - Error Exception: {e}")
            return 0

################# 5
    def close_lmdb(self, yti):
        cmi_debug = __name__+"::"+self.close_lmdb.__name__+".#"+str(self.yti)
        logging.info( f'%s   - close_lmdb.#{self.yti} Instance: {self.db_name}' % cmi_debug )
        try:
            if self.env is not None:
                self.env.close()
                logging.info( f'%s   - Successfully closed LMDB instance.#{self.yti} {self.db_name}' % cmi_debug )
            else:
                logging.warning( f'%s   - No open LMDB instance to close.#{self.yti} {self.db_name}' % cmi_debug )
            return 1
        except lmdb.Error as e:
            print(f"LMDB Close Error: {e}")
            return 0
        except Exception as e:
            print(f"Close instance - Error Exception: {e}")
            return 0
        
################# 6
    def kv_cache_engine(self, _yti, symbol, data_row, item_idx, global_sent_ai):
        cmi_debug = __name__+"::"+self.kv_cache_engine.__name__+".#"+str(self.yti)
        logging.info( f'%s  - kv_cache_engine.#{_yti} KVstore instance: {self.db_name}' % cmi_debug )

        # Deep Caching engine (LMDB KV store)
        # Has article been read/extracted, and its metadata existing in KVstore
        # Attempt to rehydrate the article metadata from Deep Cache
        #
        # RETURNS:
        #   - ec, 0, 0, {}, {}
        #   (@1: error code, @2: total_tokens, @3: total_words, #4: sen_data, @5: final_results)
        #
        # error_codes:
        #   0 = SUCCESS, Data rehydrated from Deep Cache
        #   1 = LMDB I/O FAILURE : cant deserialize JSON data package
        #   2 = CORRUPT data : No KEY found in JSON data package (URL hash of article)
        #   3 = DEEP CACHE MISS : NO BS4 Deep Cache entry found. Cache MISS !!
        #   4 = LMDB I/O FAILURE : Failed to open DB in RO mode

        _sentiment_count = dict()
        _sentiment_count["neutral"] = 0     # reset chunk metrics. Make sur eelemts exist
        _sentiment_count["positive"] = 0
        _sentiment_count["negative"] = 0
        
        logging.info( f'%s - Open LMDB READ-ONLY mode...' % cmi_debug )
        #kv_success = None  # debig control switch
        _kv_success = self.open_lmdb_RO(3)
        if _kv_success is not None:                      #    LMDB opened sucessfully
            ################# LMDB Deep Cache KV store engine
            #
            # KVstore REHYDRATON Engine
            _url_hash = data_row['urlhash']             # current article URL hash from main skimm list
            _key = "0001"+"."+symbol+"."+_url_hash      # we are looking at the artile here. So test for this K/V data
            bs4_kvs_key = _key.encode('utf-8')          # byte encode 
            logging.info( f'%s - Check Deep Cache KVstore: {_key}' % cmi_debug )
            with _kv_success.begin() as txn:
                _key_found = txn.get(bs4_kvs_key)         # lookup key in KVstore
                if _key_found is not None:
                    logging.info( f'%s - Deep Cache KV entry found: validating...' % cmi_debug )
                    _final_results = dict()             # ensure _final_results = empty
                    try:
                        _v_str = _key_found.decode('utf-8') # lookup KEY & Deserialiize into string
                    except (UnicodeDecodeError, json.JSONDecodeError) as e:
                        logging.info( f'%s - Error.#1 Deserializing data: {e}"...' % cmi_debug )
                        print (f"================================ End.#1 KV Cache Hit + Data Corrupt (deserializing) ! Net read... {item_idx} ================================" )
                        return 1, 0, 0, None, None   #  LMDB I/O FAILURE : cant deserialize data
                    else:
                        _final_results = json.loads(_v_str)        # parse JSON                   
                        try:
                            kv_url_hash = _final_results['urlhash']
                        except KeyError as _f:
                            logging.info( f'%s - Data corrupt.#2 : No URL hash Key found: {_f}"...' % cmi_debug )
                            print (f"================================ End.#2 KV Cache Hit + Data Corrupt (no URL hash) ! Net read... {item_idx} ================================" )
                            return 2, 0, 0, None, None   # CORRUPT data : No KEY found (URL hash of article)
                        else:
                            _total_tokens = 0
                        
                            # reset sent_count before we start
                            _no_header = False
                            global_sent_ai.active_urlhash = kv_url_hash   # tell ml_sentiment class url_hash we are rehydrating
                            # read the Deep cache entry, rehydrate the save_sentiment DF from cahed data
                            logging.info( f'%s - Rehydrate : Sent DF metrics from Deep Cache...' % cmi_debug )
                            #print (f"##-debug-578: pre-check FR: {sentiment_ai.sentiment_count["positive"]} / {sentiment_ai.sentiment_count["neutral"]} / {sentiment_ai.sentiment_count["negative"]}")
                            for _dc_k, _dc_v in _final_results.items():
                                if isinstance(_dc_v, dict):
                                    _total_tokens += int(_dc_v['tokenz'])
                                    _chunk=_dc_v['chunk']
                                    _chunk_sent = _dc_v['sent_type']
                                    _sentiment_count[_chunk_sent] += 1  # incr sentiment type counter
                                    sen_package = dict(sym=symbol,
                                                    article=_final_results['article'],
                                                    urlhash=kv_url_hash,
                                                    chunk=_chunk,
                                                    rank=_dc_v['sent_score'],
                                                    sent=_dc_v['sent_type'],
                                                    )

                                    #print (f"##-debug-241: kveng - senpkg / KEY: {_dc_k} {_chunk}: {_chunk_sent} - {_sen_p} / {_sen_z} / {_sen_n}")
                                    if _no_header is False:
                                        logging.info( f'%s - Rehydrate : Found JSON dict element ! Processing metrics...' % cmi_debug )
                                        _no_header = True
                                    global_sent_ai.save_sentiment_df(item_idx, sen_package)   # safe global sent DF @ sentiment_ai.sen_df0
                                    continue    # not looking at dict{} element in JSON package
                                    #print (f"##-debug-578: post-check FR: {sentiment_ai.sentiment_count["positive"]} / {sentiment_ai.sentiment_count["neutral"]} / {sentiment_ai.sentiment_count["negative"]}")
                                else:
                                    logging.info( f'%s - Rehydrate : Skip root element {_dc_k} / Scanning for dict...' % cmi_debug )
                                    continue    # ensure for loop continues to next element in JSON package
                            _no_header = False
                            # rehydrate pos/nwg/neut sentiment count DF from Depp Cache entry
                            _total_words = _final_results["total_words"]
                            _total_chars = _final_results["chars_count"]
                            #print (f"##-debug-578: final-check FR: {sentiment_ai.sentiment_count["positive"]} / {sentiment_ai.sentiment_count["neutral"]} / {sentiment_ai.sentiment_count["negative"]}")
                            _sent_z = _sentiment_count["neutral"]
                            _sent_p = _sentiment_count["positive"]
                            _sent_n = _sentiment_count["negative"]
                            
                            # must return self.sen_data, and must exec after return...
                            #   - sen_df_row = pd.DataFrame(self.sen_data, columns=[ 'art', 'urlhash', 'positive', 'neutral', 'negative'] )
                            #   - self.sen_stats_df = pd.concat([self.sen_stats_df, sen_df_row])
                            self.sen_data = [[
                                    item_idx,
                                        kv_url_hash,
                                        _sent_p,
                                        _sent_z,
                                        _sent_n
                                        ]]
                            
                            #sent_fz = _final_results["neutral_count"]
                            #sent_fp = _final_results["positive_count"]
                            #sent_fn = _final_results["negative_count"]

                            #print (f"JSON: {_final_results}")
                            print ( f"Total tokenz: {_total_tokens} / Words: {_total_words} / Chars: {_total_chars} / Postive: {_sent_p} / Neutral: {_sent_z} / Negative: {_sent_n}")
                            print (f"BS4 KV Cache:  [ HIT.#0 / Deep cache Read success ! Rehydrated from KVstore... ] {item_idx}" )
                            return 0, _total_tokens, _total_words, self.sen_data, _final_results
                            #
                            # SUCCESS !!!
                            # ##### END of Deep Cache HIT run... prints Metrics all rehydrated from Deep Cache  
                else:
                    logging.info( f'%s - Deep Cache MISS : No KVstore entry found !' % cmi_debug )
                    print (f"KV Cache.#3:   [ MISS.#3 / No KV cache entry ! Force Net read... ] {item_idx}" )
                    return 3, 0, 0, None, None

            logging.info( f"%s - Deep Cache ERROR.#4 : ! LMDB I/O cant open RO mode" % cmi_debug )
            print (f"================================ End.#4 KV Cache MISS ! LMDB RO open failure ! Net read... {item_idx} ================================" )
            return 4, 0, 0, None, None   # LMDB I/O FAILURE : Failed to open DB in RO mode

        # #########################################
        # private helper function : BS4 extractor
        def dump_kvcache_bs4(self, symbol, _urlhash):
            with self.kvio_eng.env.begin() as txn0:
                print(f"BS4 Dumping LMDB KV cache database...")    
                cursor0 = txn0.cursor()
                count = 0
                for _key0, _value0 in cursor0:
                    _find_me = "0001."+symbol+"."+_urlhash
                    match _key0.decode('utf-8'):
                        case str(_find_me):
                            key_str = _key0.decode('utf-8')
                            value_str = _value0.decode('utf-8')
                            print(f"LMDB -  SEARCH: {_find_me}" )
                            print(f"LMDB -  KEY:    {key_str} -> VALUE: {value_str}\n")
                        case _:
                            print(f"LMDB -  didnt find any LMBD data for: {symbol} / {_urlhash}")
                    count += 1
                print(f"\nBS4 Total entries in LMDB database: {count}")    
                #self.kvio_eng.env.close()
            return