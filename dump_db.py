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

logging.basicConfig(level=logging.INFO)
global args
args = {}

parser = argparse.ArgumentParser(prog="Aop", description="LMBD Maintence tool")
parser.add_argument('-v','--verbose', help='verbose error logging', action='store_true', dest='bool_verbose', required=False, default=False)
parser.add_argument('-i','--init', help='create new emplt KV db', action='store_true', dest='bool_init', required=False, default=False)


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

lmdb_dbname = "LMDB_0001"
lmdb_inst = lmdb_io_eng(1, lmdb_dbname, args)
lmdb_inst.open_lmdb_RO(1)
lmdb_inst.dump_lmdb_RO(1)

if args['bool_init'] is True:
    print ( "Initializing New Empty LMDB KV Database..." )
    lmdb_dbname = "LMDB_0001"
    lmdb_inst = lmdb_io_eng(1, lmdb_dbname, args)
    env = lmdb_inst.open_lmdb_RW(1)
    env.close()

