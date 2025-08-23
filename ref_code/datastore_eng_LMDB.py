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
    db_path = "/home/dbrace/code/bespin/datastore/"       # filesystem path to locale of LMDB K/V Database
    db_name = None      # LMDB Database instance name
    db_open_state = 0   # 0=closed, 1=open
    env = None          # current opened LMDB database I/O Transaction handle
    yti = 0

        
################# init
    def __init__(self, yti, db_name, global_args):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%s - Instantiate.#{yti} LMDB instance: {db_name}' % cmi_debug )
        self.args = global_args                            # Only set once per INIT. all methods are set globally
        self.yti = yti
        self.db_name = db_name
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
        logging.info( f'%s   - open_lmdb_RO.#{self.yti} Instance: {self.db_name}' % cmi_debug )
        db_inst = self.db_path+self.db_name

        try:
            self.env = lmdb.open(db_inst, map_size=1024*1024*1024, readonly=False)     # map_size: Maximum size DB = 1GB
            logging.info( f'%s   - Successfully openend KVstore - READ-WRITE mode.#{self.yti} {self.db_name}' % cmi_debug )
            logging.info( f'%s   - KVstore remains globally open.#{self.yti} Instance: {self.db_name}' % cmi_debug )
            return self.env
        except lmdb.Error as e:
            print(f"LMDB Open Error: {e}")
            print(f"Database: {db_inst} - not found.")
            return 0
        except Exception as e:
            print(f"Error Exception: {e}")
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
                print (f"Dumping KV datastore..." )
                for key, value in cursor:
                    key_str = key.decode('utf-8')
                    value_str = value.decode('utf-8')
                    print(f"{count:03} / KEY: {key_str} -> VALUE: {value_str[:50]}{'...' if len(value_str) > 50 else ''}")
                    count += 1            
            print (f"Dump completed !!" )
            return 1
        except lmdb.Error as e:
            print(f"LMDB Open Error: {e}")
            print(f"Database: {db_inst} - not found.")
            return 0
        except Exception as e:
            print(f"Error Exception: {e}")
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
            print(f"Error Exception: {e}")
            return 0
