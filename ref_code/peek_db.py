#! python3

import lmdb

# Open an LMDB environment (creates 'my_database' directory if it doesn't exist)
# map_size sets the maximum size of the database file in bytes

db_path = "datastore/LMDB_tests_kvstore01/"
lmdb_dbname = "LMDB_0001"
my_database = db_path + lmdb_dbname

print (f"Opening LMDB instance: {my_database}" )
env = lmdb.open(my_database, map_size=1024*1024*1024) # 1.0 GiB

with env.begin(write=True) as txn:
    cursor = txn.cursor()
    cursor.first()
    i = 0
    for i in range(0, 10, 1):
        print(f"Key: {cursor.key()} ->\n{cursor.item()}")
        print( f"========================= {i} ==========================")

env.close()

