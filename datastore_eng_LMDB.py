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
    db_path = "datastore/LMDB_tests_kvstore01/"       # filesystem path to locale of LMDB K/V Database
    db_name = None      # LMDB Database instance name
    db_open_state = 0   # 0=closed, 1=open
    env = None          # current opened LMDB database I/O Transaction handle
    yti = 0

        
################# init
#
    def __init__(self, yti, db_name, global_args):
        cmi_debug = __name__+"::"+self.__init__.__name__
        logging.info( f'%sn- Instantiate.#{yti} LMDB instance: {db_name}' % cmi_debug )
        self.args = global_args                            # Only set once per INIT. all methods are set globally
        self.yti = yti
        self.db_name = db_name
        return

################# 1
    def open_lmdb_RO(self, yti):
        cmi_debug = __name__+"::"+self.open_lmdb_RO.__name__+".#"+str(self.yti)
        logging.info( f'%s    - open_lmdb.#{self.yti} DB Instance: {self.db_name}' % cmi_debug )
        db_inst = self.db_path + self.db_name
        try:
            self.env = lmdb.open(db_inst, readonly=True)     # map_size: Maximum size DB = 1GB
            logging.info( f'%s   - Successfully opened KVstore - READ-ONLY mode.#{self.yti} {self.db_name}' % cmi_debug )
            logging.info( f'%s   - KVstore remains globally open.#{self.yti} instance: {self.db_name}' % cmi_debug )
            return 1
        except lmdb.Error as e:
            print(f"LMDB Open Error: {e}")
            print(f"Database: {db_inst} - not found.")
            return 0
        except Exception as e:
            print(f"Error open_lmdb_RO Exception: {e}")
            return 0
            
################# 1
    def open_lmdb_RW(self, yti):
        cmi_debug = __name__+"::"+self.open_lmdb_RW.__name__+".#"+str(self.yti)
        logging.info( f'%s   - open_lmdb.#{self.yti} Instance: {self.db_name}' % cmi_debug )
        db_inst = self.db_path+self.db_name

        try:
            self.env = lmdb.open(db_inst, map_size=1024*1024*1024, readonly=False)     # map_size: Maximum size DB = 1GB
            logging.info( f'%s   - Successfully openend KVstore - READ-WRITE mode.#{self.yti} {self.db_name}' % cmi_debug )
            logging.info( f'%s   - KVstore remains globally open.#{self.yti} Instance: {self.db_name}' % cmi_debug )
            return 1
        except lmdb.Error as e:
            print(f"LMDB Open Error: {e}")
            print(f"Database: {db_inst} - not found.")
            return 0
        except Exception as e:
            print(f"Error Exception: {e}")
            return 0
            
            
        
