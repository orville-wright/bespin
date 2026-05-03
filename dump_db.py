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
parser.add_argument('-k','--key', help='filter output by KEY substring', action='store', dest='key_filter', required=False, default=None)
parser.add_argument('-d','--deep', help='Deep data dump of values', action='store_true', dest='bool_data', required=False, default=False)


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

def dump_lmdb_by_key(lmdb_instance, key_filter):
    """Scan all LMDB entries and print only those whose key contains key_filter."""
    try:
        with lmdb_instance.RO_env.begin() as txn:
            cursor = txn.cursor()
            total = 0
            matches = 0
            for key, value in cursor:
                key_str = key.decode('utf-8')
                total += 1
                if key_filter in key_str:
                    value_str = value.decode('utf-8')
                    print(f"{matches:03} / KEY: {key_str} -> VALUE: {value_str[:80]}{'...' if len(value_str) > 80 else ''}")
                    matches += 1
            print(f"\nKey filter '{key_filter}': {matches} match(es) from {total} total entries")
    except lmdb.Error as e:
        print(f"LMDB Error: {e}")
    except Exception as e:
        print(f"dump_lmdb_by_key Error: {e}")


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


lmdb_dbname = "LMDB_0001"
lmdb_inst = lmdb_io_eng("RO_DUMP", lmdb_dbname, args)
lmdb_inst.open_lmdb_RO("RO_DUMP")


if args['bool_data'] is True and args['key_filter'] is None:
    print("ERROR: --deep requires --key to also be specified")
    parser.print_help()
    sys.exit(1)

if args['bool_data'] is True and args['key_filter'] is not None:
    print(f"Deep dump for key filter: '{args['key_filter']}'")
    dump_lmdb_deep(lmdb_inst, args['key_filter'])
elif args['key_filter'] is not None:
    print(f"Filtering LMDB entries by key: '{args['key_filter']}'")
    dump_lmdb_by_key(lmdb_inst, args['key_filter'])
else:
    lmdb_inst.dump_lmdb_RO("RO_DUMP")

if args['bool_init'] is True:
    print ( "Initializing New Empty LMDB KV Database..." )
    lmdb_dbname = "LMDB_0001"
    lmdb_inst = lmdb_io_eng(1, lmdb_dbname, args)
    env = lmdb_inst.open_lmdb_RW(1)
    env.close()

