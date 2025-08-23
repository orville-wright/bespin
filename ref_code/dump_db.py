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


from datastore_eng_LMDB import lmdb_io_eng

global args
args = {}
global parser
parser = argparse.ArgumentParser(prog="Aop", description="LMBD Maintence tool")
parser.add_argument('-d','--dump', help='Open READ-ONLY mode anmd dump KV db', action='store_true', dest='bool_dump', required=False, default=False)
parser.add_argument('-i','--init', help='create new emplt KV db', action='store', dest='lmdb_name', required=False, default=False)

#parser.add_argument('-q','--quote', help='Get ticker price action quote', action='store', dest='qsymbol', required=False, default=False)

parser.add_argument('-v','--verbose', help='verbose error logging', action='store_true', dest='bool_verbose', required=False, default=False)
logging.basicConfig(level=logging.INFO)

def main():
    global args
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

    if args['bool_dump'] is True:        # Logging level
        print ( "Dump DB via READ-ONLY mode..." )
        dump_lmdb()
        print ( "===== E n d =====\n" )
    
    if args['lmdb_name'] is not False:        # Logging level
        print ( "Init new empty LMDB DB..." )
        print ( f"Initializing New Empty LMDB KV Database: {args['lmdb_name'].upper()}..." )
        lmdb_dbname = args['lmdb_name'].upper()
        lmdb_inst = lmdb_io_eng(1, lmdb_dbname, args)
        env = lmdb_inst.open_lmdb_RW(1)
        env.close()
        print ( "===== E n d =====\n" )
    return
   

def dump_lmdb():
    lmdb_dbname = "LMDB_0001"
    print (f"Test Open Read-Only LMDB instance {lmdb_dbname} and Dump Contents...")
    lmdb_inst = lmdb_io_eng(1, lmdb_dbname, args)
    lmdb_inst.open_lmdb_RO(1)
    lmdb_inst.dump_lmdb_RO(1)
    print (f"============ done ============")
    print ( " " )
    return



if __name__ == '__main__':
    main()

