#!/usr/bin/env python3

"""
LMDB Lightning Memory-Mapped Database

Opens an existing LMDB database in: ../datastore/LMDB_tests_kvstore01/LMDB_0001
executes a number of basic functions to excercise the operational functionality
of the LMDB system

Reference code to show how to do key functions.
"""

from datetime import datetime
import json
import lmdb
import random
import string
from typing import Any, Dict, List, Tuple, Optional

dbpath = None
key_total = 0

def main():
    build("../datastore/LMDB_tests_kvstore01/LMDB_0001")
    deser("../datastore/LMDB_tests_kvstore01/LMDB_0001")
    
def build(dbname):    
    print (f"\n\n----------------------- Create Database Data Entries -----------------")
    db_path = dbname

    def generate_random_string(length_range=(5, 20)):
        length = random.randint(*length_range)
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    def generate_random_data():
        data_types = ['string', 'number', 'dict', 'list', 'boolean']
        data_type = random.choice(data_types)
        
        if data_type == 'string':
            return generate_random_string((10, 50))
        elif data_type == 'number':
            return random.uniform(1, 10000)
        elif data_type == 'dict':
            return {
                'id': random.randint(1000, 9999),
                'name': generate_random_string((5, 15)),
                'timestamp': datetime.now().isoformat(),
                'value': random.uniform(0, 100)
            }
        elif data_type == 'list':
            return [generate_random_string((3, 10)) for _ in range(random.randint(2, 5))]
        else:  # boolean
            return random.choice([True, False])
        
    try:
        # Open the existing LMDB database
        # map_size: Maximum size of the database (1GB in this case)
        # readonly=False: Allow write operations
        env = lmdb.open(db_path, map_size=1024*1024*1024, readonly=False)
        
        print(f"Successfully opened LMDB database: {db_path}")
        print("Generating 10 random key-value pairs...\n")
        
        # Begin a write transaction
        with env.begin(write=True) as txn:
            for i in range(10):
                # Generate random key (ensuring uniqueness with timestamp)
                timestamp_suffix = str(int(datetime.now().timestamp() * 1000000))[-6:]
                random_key = f"key_{generate_random_string((5, 10))}_{timestamp_suffix}"
                
                # Generate random data
                random_data = generate_random_data()
                
                # Convert data to JSON string for storage (LMDB stores bytes)
                data_json = json.dumps(random_data, default=str)
                
                # Store the key-value pair
                # Both key and value must be bytes in LMDB
                txn.put(random_key.encode('utf-8'), data_json.encode('utf-8'))
                
                print(f"Entry {i+1:2d}: Key = '{random_key}'")
                print(f"          Value = {data_json}")
                print(f"          Type = {type(random_data).__name__}")
                print("-" * 60)
        
        print (f"Compiled MAX_Key_len: {env.max_key_size()}")

        # Verify the entries were stored by reading them back
        print("\nVerifying stored entries:")
        #with env.begin(read=True) as txn:
        with env.begin() as txn:
            cursor = txn.cursor()
            count = 0
            for key, value in cursor:
                key_str = key.decode('utf-8')
                value_str = value.decode('utf-8')
                print(f"KEY: {key_str} -> VALUE: {value_str[:50]}{'...' if len(value_str) > 50 else ''}")
                count += 1

            print(f"\nTotal entries in database: {count}")
            key_total = count
      
        # Close the environment
        #env.close()
        print(f"Database operations completed successfully!")
        
    except lmdb.Error as e:
        print(f"LMDB Error: {e}")
        print(f"Database: ../datastore/LMDB_tests_kvstore01/LMDB_0001 - not found.")
    except Exception as e:
        print(f"Unexpected error: {e}")

###################################### ENd phase 1 ##############################################
#
#
#################################### START phase 2 ##############################################

# Helper function 1
def deserialize_value(value_bytes: bytes) -> Any:
    try:
        # Decode bytes to string, then parse JSON
        value_str = value_bytes.decode('utf-8')
        return json.loads(value_str)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        print(f"Error deserializing value: {e}")
        return None

# Helper function 2
# moved

# Helper function 3
def read_specific_key(db_path: str, key: str) -> Optional[Any]:
    try:
        env = lmdb.open(db_path)
        with env.begin() as txn:
            value_bytes = txn.get(key.encode('utf-8'))

            if value_bytes is not None:
                env.close()
                return deserialize_value(value_bytes)
            else:
                env.close()
                return None
    except lmdb.Error as e:
        print(f"LMDB Error: {e}")
        return None

# Helper function 4
def read_keys_with_prefix(db_path: str, prefix: str) -> List[Tuple[str, Any]]:
    matching_entries = []

    try:
        env = lmdb.open(db_path, readonly=True)

        with env.begin(write=False) as txn:
            cursor = txn.cursor()

            # Set cursor to first key >= prefix
            if cursor.set_range(prefix.encode('utf-8')):
                # Iterate while keys match prefix
                for key_bytes, value_bytes in cursor:
                    key = key_bytes.decode('utf-8')

                    # Stop if we've moved beyond the prefix
                    if not key.startswith(prefix):
                        break

                    value = deserialize_value(value_bytes)
                    matching_entries.append((key, value))

        env.close()
        return matching_entries

    except lmdb.Error as e:
        print(f"LMDB Error: {e}")
        return []

# Helper function 5
#deleted

# Helper function 6
def display_entry_details(key: str, value: Any, index: int):
    print(f"Entry {index}:")
    print(f"  Key: '{key}'")
    print(f"  Type: {type(value).__name__}")
    print(f"  Value: {value}")

    # Additional details based on type
    if isinstance(value, dict):
        print(f"  Dict keys: {list(value.keys())}")
        print(f"  Dict size: {len(value)} items")
    elif isinstance(value, list):
        print(f"  List length: {len(value)} items")
        print(f"  List contents: {value}")
    elif isinstance(value, str):
        print(f"  String length: {len(value)} characters")
    elif isinstance(value, (int, float)):
        print(f"  Numeric value: {value}")

    print("-" * 70)

# Helper function 7 
def search_by_value_content(db_path: str, search_term: str) -> List[Tuple[str, Any]]:
    """
    Search for entries containing a specific term in their JSON representation.
    Args: db_path: Path to LMDB database directory
          search_term: Term to search for in the serialized values
    Returns:  List of (key, deserialized_value) tuples containing the search term
    """
    matching_entries = []

    try:
        env = lmdb.open(db_path, readonly=True)

        with env.begin(write=False) as txn:
            cursor = txn.cursor()

            for key_bytes, value_bytes in cursor:
                value_str = value_bytes.decode('utf-8')

                # Search in the JSON string representation
                if search_term.lower() in value_str.lower():
                    key = key_bytes.decode('utf-8')
                    value = deserialize_value(value_bytes)
                    matching_entries.append((key, value))

        env.close()
        return matching_entries

    except lmdb.Error as e:
        print(f"LMDB Error: {e}")
        return []

#####################
# main app start...
def deser(dbpath):
    db_path = dbpath
    print (f"\n\n======================= Reader + Deseralizer =================================")
    print("LMDB Data Reader and Deserializer")
    print("=" * 50)
    
    #all_entries = read_all_entries(db_path)
    #
    ###
    #def read_all_entries(db_path: str) -> List[Tuple[str, Any]]:
    all_entries = []
    print("\nPhase #1 - Reading all entries from database:")
    try:
        env = lmdb.open(db_path, readonly=True)
        with env.begin() as txn:
            cursor = txn.cursor()
            for key_bytes, value_bytes in cursor:   # Iterate through all key-value pairs
                key = key_bytes.decode('utf-8')
                value = deserialize_value(value_bytes)
                all_entries.append((key, value))
        #env.close()
        #return entries
    except lmdb.Error as e:
        print(f"LMDB Error: {e}")
        print("No entries found or database error occurred.")
        return []

    print(f"Found: {len(all_entries)} total entries\n")
    print(f"Listing first 5 entries...\n")

    # Display first few entries with detailed information
    for i, (key, value) in enumerate(all_entries[:5], 1):
        display_entry_details(key, value, i)

    if len(all_entries) > 5:
        print(f"... and {len(all_entries) - 5} more entries\n")

    print(f"------------------------- end phase ---------------------------")

    #############################################################
    # Method 2: Analyze data types
    print("\nPhase #2 - Data type analysis:")
    #type_counts = analyze_data_types(all_entries)
    type_counts = {}
    for key, value in all_entries:
        value_type = type(value).__name__
        type_counts[value_type] = type_counts.get(value_type, 0) + 1
    
    for data_type, count in type_counts.items():
        print(f"  {data_type}: {count} entries")
    
    print(f"------------------------- end phase ---------------------------")

    #############################################################
    # Method 3: Read specific key (example)
    print("\nPhase #3 - Reading specific key (first key as example):")
    if all_entries:
        first_key = all_entries[0][0]        
        try:
            env = lmdb.open(db_path)
            with env.begin() as txn:
                value_bytes = txn.get(first_key.encode('utf-8'))
                if value_bytes is not None:
                    specific_value = deserialize_value(value_bytes)
                    if specific_value is not None:
                        print(f"Found Key '{first_key}'")
                        print(f"- Original type: {type(specific_value).__name__}")
                        print(f"- Value: {specific_value}")

                        # Demonstrate that the object is fully functional
                        if isinstance(specific_value, dict):
                            print(f"  Can access dict keys: {list(specific_value.keys())}")
                            if 'name' in specific_value:
                                print(f"  Dict['name'] = {specific_value['name']}")
                        elif isinstance(specific_value, list):
                            print(f"  Can access list elements: first = {specific_value[0] if specific_value else 'N/A'}")
                        
                        print(f"\n------------------------- end phase ---------------------------")
                    else:
                        print(f"Key '{first_key}' not found")
                else:
                    env.close()
                    print (f"Cannot deserialize First Key: {first_key}")
                    return None
        except lmdb.Error as e:
            print(f"LMDB Open DB Error: {e}")
        except Exception as x:
            print(f"ERROR Exception triggered:: {x}")
            return

    ##################################################################
    # Method 4: Read keys with prefix
    print("\nPhase #4 - Reading keys with prefix 'key_':")
    prefix_entries = read_keys_with_prefix(db_path, "key_")
    print(f"Found {len(prefix_entries)} entries with prefix 'key_'")
    print(f"\n------------------------- end phase ---------------------------")

    ##################################################################
    # Method 5: Demonstrate working with different data types
    print("\n5. Working with deserialized data by type:")
    for key, value in all_entries[:3]:  # Show first 3
        print(f"\nKey: {key}")
        print(f"Type: {type(value).__name__}")

        if isinstance(value, dict):
            print("  → This is a dictionary, can access its items:")
            for dict_key, dict_value in value.items():
                print(f"    {dict_key}: {dict_value}")

        elif isinstance(value, list):
            print("  → This is a list, can iterate through items:")
            for idx, item in enumerate(value):
                print(f"    [{idx}]: {item}")

        elif isinstance(value, str):
            print(f"  → This is a string with length {len(value)}")
            print(f"    Upper case: {value.upper()}")

        elif isinstance(value, (int, float)):
            print(f"  → This is a number, {value}... its can perform math:")
            print(f"    => Value + 3 x 2: {(value + 3) * 2}")
            print(f"    => Value + 3 Squared: {(value +3) ** 2}")

        elif isinstance(value, bool):
            print(f"  → This is a boolean: {value}")
            print(f"    Negated: {not value}")

    print(f"\n------------------------- end phase ---------------------------")
    ##################################################################
    # Method 6: Delete a K/V pair from the database
    
    txn0 = env.begin(write=True)
    cursor = txn0.cursor()
    dbstats = txn0.stat()
    dbcount = dbstats['entries']
    keys_to_del = random.randint(1, dbcount)
    print(f"\nPhase #6 - Delete {keys_to_del} random K/V pairs from the database:")
    
    try:
        env = lmdb.open(db_path, readonly=False)
        with env.begin(write=True) as txn:
            cursor = txn.cursor()
            for _k, _v in cursor:
                stats = txn.stat()
                entry_count = stats['entries']
                print(f"Cycle: {keys_to_del} / Total number of K/V pairs remaining: {entry_count} / deleting K/V pair @:", end="")
                _rk = random.randint(0, entry_count)
                print(f" {_rk}")
                txn.delete(_k)
                keys_to_del -= 1
                if keys_to_del == 0:
                    break
        env.close()
    except lmdb.Error as e:
        print(f"LMDB Open DB Error: {e}")
    except Exception as x:
        print(f"ERROR Exception triggered:: {x}")
        return                    

    ##################################################################
    # Bonus: Example of searching by content
    print("\n" + "=" * 50)
    print("6. Bonus: Searching by value content:")
    search_results = search_by_value_content("../datastore/LMDB_tests_kvstore01/LMDB_0001", "name")
    print(f"Found {len(search_results)} entries containing 'name':")
    for key, value in search_results[:3]:  # Show first 3 matches
        print(f"  {key}: {value}")
    
    print(f"\n\n------------------------- end phase ---------------------------")
    

if __name__ == "__main__":
    main()
