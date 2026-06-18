# datastore_eng_LMDB.py — LMDB Key-Value Cache Engine

## Overview

`datastore_eng_LMDB.py` provides the `lmdb_io_eng` class — Bespin's embedded key-value datastore layer built on [LMDB](http://www.lmdb.tech/doc/) (Lightning Memory-Mapped Database). It acts as a high-speed article cache for the ML/NLP sentiment pipeline, persisting scraped article text and computed sentiment metrics so that subsequent runs can rehydrate results instantly rather than re-scraping the web.

The cache is shared by two extraction engines operating in parallel:
- **C4 engine** (`C4_lmdb_env`) — results from the Crawl4ai async extractor
- **BS4 engine** (`BS4_lmdb_env`) — results from the BeautifulSoup4 HTML parser

Both engines write to the same physical LMDB environment (`datastore/LMDB_0001`) under distinct logical keys, so they never conflict.

---

## Architecture Context

```
ml_yf_nlp_news_engine.py
  ├── artdata_BS4_depth3()  ──> BS4_lmdb_env (lmdb_io_eng instance)
  └── artdata_C4_depth3()   ──> C4_lmdb_env  (lmdb_io_eng instance)
                                      │
                                      ▼
                             datastore/LMDB_0001/
                             (memory-mapped KV file)
                                      │
                                      ▼
                             dump_db.py  (inspection utility)
```

`aop.py` and `xop.py` (the main entry points) instantiate `lmdb_io_eng` and pass it to `ml_yf_nlp_news_engine` before starting the NLP pipeline loop.

---

## File Location and Database Path

| Item | Value |
|------|-------|
| Source file | `datastore_eng_LMDB.py` |
| Default DB directory | `datastore/` (relative to working directory) |
| Default DB name | `LMDB_0001` |
| Max DB size | 1 GB (set via `map_size=1024*1024*1024` in RW mode) |

---

## Class: `lmdb_io_eng`

```python
from datastore_eng_LMDB import lmdb_io_eng
```

A per-database LMDB manager. One instance is created per logical database so the database name never needs to be passed on every call — it is fixed at construction time.

### Class Attributes

| Attribute | Type | Default | Description |
|-----------|------|---------|-------------|
| `args` | `list` | `[]` | Global args dict passed in from caller |
| `cr_package` | `None` | `None` | Full results dict from a dict-processor run |
| `cursor` | `None` | `None` | Current LMDB transaction cursor (stored globally for reference) |
| `cycle` | `int` | `0` | Thread loop counter |
| `db_path` | `str` | `"datastore/"` | Filesystem path to the LMDB database directory |
| `db_name` | `str` | `"DB_name_not_set"` | LMDB database instance name (set in `__init__`) |
| `db_open_state` | `dict` | `{}` | Maps `db_name` → open environment object (or `None` if closed) |
| `lmdb_env` | `dict` | `{}` | Global LMDB environment instance |
| `rehy_count` | `int` | `0` | Counter: how many articles were successfully rehydrated from cache |
| `RO_env` | `dict` | `{}` | LMDB environment opened in read-only mode |
| `RW_env` | `dict` | `{}` | LMDB environment opened in read-write mode |
| `sent_ai` | `None` | `None` | `ml_sentiment` instance (set externally before calling `kv_cache_engine`) |
| `yti` | `int` | `0` | Instance identifier passed at construction |
| `_n` | `int` | `0` | Negative sentiment chunk count (reset per article) |
| `_p` | `int` | `0` | Positive sentiment chunk count (reset per article) |
| `_z` | `int` | `0` | Neutral sentiment chunk count (reset per article) |

---

## Constructor

```python
lmdb_io_eng(yti, db_name, global_args)
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `yti` | `int` or `str` | Instance identifier (used in log messages and debug output) |
| `db_name` | `str` | Name of the LMDB database (e.g. `"LMDB_0001"`) — combined with `db_path` to form the full filesystem path |
| `global_args` | `dict` | Global CLI args dict passed from the calling module |

**Example:**
```python
lmdb_inst = lmdb_io_eng("GLOBAL", "LMDB_0001", args)
```

---

## Methods

### 1. `open_lmdb_RO(yti)` — Open Read-Only

```python
env = lmdb_inst.open_lmdb_RO(_yti)
```

Opens the LMDB database in **read-only mode**. Stores the open environment in `self.RO_env` and records it in `self.db_open_state`.

| Parameter | Description |
|-----------|-------------|
| `_yti` | Caller identifier (used in logging) |

**Returns:**
- `lmdb.Environment` — the open LMDB environment object (on success)
- `1` — `lmdb.Error` (LMDB-specific open failure)
- `0` — unexpected exception

**Notes:**
- The LMDB file at `db_path + db_name` must already exist.
- Read-only mode does not set `map_size`; the DB uses whatever size was set when it was created.
- The environment stays globally open until `close_lmdb()` is called.

---

### 2. `open_lmdb_RW(yti)` — Open Read-Write

```python
env = lmdb_inst.open_lmdb_RW(yti)
```

Opens the LMDB database in **read-write mode** with a 1 GB map size. Stores the environment in `self.RW_env` and records it in `self.db_open_state`.

| Parameter | Description |
|-----------|-------------|
| `yti` | Caller identifier (used in logging) |

**Returns:**
- `lmdb.Environment` — the open LMDB environment (on success)
- `1` — `lmdb.Error`
- `0` — unexpected exception

**Notes:**
- Creates the database directory/file if it does not already exist.
- `map_size=1024*1024*1024` (1 GB) — the maximum total size the database can ever reach.
- The environment stays globally open until `close_lmdb()` is called.

---

### 3. `dump_lmdb_RO(_yti)` — Dump All Entries (Read-Only)

```python
result = lmdb_inst.dump_lmdb_RO(_yti)
```

Iterates over **all key-value entries** in the already-open `RO_env` and prints them to stdout. Each entry is printed as a zero-padded counter, the key, and the first 50 characters of the value (with `...` truncation).

| Parameter | Description |
|-----------|-------------|
| `_yti` | Caller identifier |

**Returns:**
- `1` — success
- `2` — `lmdb.Error`
- `0` — unexpected exception

**Prerequisite:** The DB must already be open via `open_lmdb_RO()`.

**Example output:**
```
000 / KEY: 0001.AAPL.f308c6c74e... -> VALUE: {"article": 0, "urlhash": "f308...
001 / KEY: 0001.NVDA.a1b2c3d4e5... -> VALUE: {"article": 1, "urlhash": "a1b2...
```

---

### 4. `drop_lmdb_RW(_yti)` — Drop All Keys

```python
result = lmdb_inst.drop_lmdb_RW(_yti)
```

**Deletes all key-value entries** from the default database but preserves the database structure. Useful for resetting the cache without deleting the database file.

| Parameter | Description |
|-----------|-------------|
| `_yti` | Caller identifier |

**Returns:**
- `1` — success (all keys dropped, environment closed and set to `None`)
- `0` — `lmdb.Error` or unexpected exception

**Warning:** This operation is irreversible. The DB must be **closed before calling this method** (the method reopens it internally in RW mode, drops keys, then closes it again).

**Implementation detail:** Uses `lmdb.open(db_inst, max_dbs=0)` to address the default database, then `txn.drop(_db0, delete=False)` — deletes all data without destroying the named database handle itself.

---

### 5. `close_lmdb(_yti)` — Close Environment

```python
result = lmdb_inst.close_lmdb(_yti)
```

Gracefully closes whichever environment is currently open (`RO_env` or `RW_env`). Sets both to `None` and clears `db_open_state`.

| Parameter | Description |
|-----------|-------------|
| `_yti` | Caller identifier |

**Returns:**
- `1` — successfully closed
- `2` — `lmdb.Error`
- `0` — unexpected exception

**Notes:**
- Checks `RO_env` first, then falls through to `RW_env`.
- Safe to call even if no environment is currently open (both are `None`).
- Always call this before switching between RO and RW mode — `kv_cache_engine()` does this automatically.

---

### 6. `kv_cache_engine(_yti, symbol, data_row, item_idx, global_sent_ai, _extr_eng)` — Cache Read Engine

```python
ec, total_tokens, total_words, sen_data, final_results = lmdb_inst.kv_cache_engine(
    _yti, symbol, data_row, item_idx, global_sent_ai, _extr_eng
)
```

The core cache lookup method. On a **cache hit**, rehydrates all sentiment metrics from the LMDB KV store in near-instant time (no network I/O required). On a **cache miss**, returns immediately so the caller can perform a live web fetch.

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `_yti` | `str` | Caller identifier (e.g. `"BS4"`, `"C4"`) |
| `symbol` | `str` | Stock ticker symbol (e.g. `"AAPL"`) |
| `data_row` | `dict` | Article row from the candidate list — must contain `'urlhash'` key |
| `item_idx` | `int` | Article index in the current processing run |
| `global_sent_ai` | `ml_sentiment` | Sentiment AI instance; `save_sentiment_df()` is called on cache hit |
| `_extr_eng` | `str` | Extraction engine tag for logging (`"BS4"` or `"C4"`) |

#### Return Values

Returns a 5-tuple: `(error_code, total_tokens, total_words, sen_data, final_results)`

| Error Code | Meaning | `total_tokens` | `total_words` | `sen_data` | `final_results` |
|------------|---------|---------------|--------------|-----------|----------------|
| `0` | **SUCCESS** — data rehydrated from Deep Cache | `int` | `int` | `list` | `dict` |
| `1` | LMDB I/O failure — cannot deserialize JSON data | `0` | `0` | `None` | `None` |
| `2` | Corrupt data — no URL hash key in JSON payload | `0` | `0` | `None` | `None` |
| `3` | **Cache MISS** — no entry found for this key | `0` | `0` | `None` | `None` |
| `4` | LMDB I/O failure — failed to open DB in RO mode | `0` | `0` | `None` | `None` |

#### Key Construction

The lookup key is constructed as:
```
{db_id}.{symbol}.{urlhash}
```
Example:
```
0001.AAPL.f308c6c74e14976ac6e940c20a329c5e063cf5cfde402d591cfcd28ace1c2b2d
```

- `db_id` is always `"0001"` (hardcoded)
- `symbol` is the ticker in its original case (e.g. `"AAPL"`)
- `urlhash` is the SHA-256 hex digest of the article URL, taken from `data_row['urlhash']`

#### Cache Hit Behavior

On a hit (error code `0`):
1. Decodes the value bytes to UTF-8
2. Parses the JSON payload
3. Iterates over chunk-level sub-dicts (keyed `"000"`, `"001"`, ...) and accumulates sentiment counts
4. Calls `global_sent_ai.save_sentiment_df(item_idx, sen_package, engine_id=0)` for each chunk
5. Returns aggregated `total_tokens`, `total_words`, `sen_data`, and the full `final_results` dict

#### State Management

- If the DB is already open when called, it force-closes it first (`close_lmdb("GLOBAL")`)
- Reopens in RO mode for the cache lookup
- The `with txn:` context manager auto-closes the transaction (but **not** the environment) on exit

---

### 7. `dump_kvcache_bs4(symbol, _urlhash)` (Private Helper)

```python
# defined inside kv_cache_engine — not directly callable from outside
```

A private inline function that searches the KV cache for a specific `symbol`+`urlhash` combination and prints the matching entry. Uses Python `match`/`case` syntax against the decoded key string.

**Note:** This function is defined inside `kv_cache_engine` (indented under it) but is effectively unreachable from outside the class. It serves as a debug utility for inspecting individual cache entries.

---

## Key-Value Data Schema

Each LMDB entry stores a JSON-serialized article record. The key is the article's URL hash fingerprint (see [Key Construction](#key-construction) above). The value is a UTF-8 encoded JSON string with the following fields:

### Root-Level Fields

| Field | Type | Description |
|-------|------|-------------|
| `article` | `int` | Article index number in the processing run |
| `urlhash` | `str` | SHA-256 URL hash (matches the key's third segment) |
| `total_tokens` | `int` | Total transformer tokens across all text chunks |
| `total_words` | `int` | Total word count of the full article |
| `chars_count` | `int` | Total character count of the full article |
| `scentence` | `int` | Number of sentences detected (note: typo preserved from source) |
| `paragraph` | `int` | Number of paragraphs detected |
| `random` | `int` | Random tag (internal tracking value) |
| `positive_count` | `int` | Count of positively-scored chunks in this article |
| `neutral_count` | `int` | Count of neutrally-scored chunks |
| `negative_count` | `int` | Count of negatively-scored chunks |
| `chunk_count` | `int` | Total number of text chunk blocklets |
| `zstd_blob` | `str` | Base64-encoded Zstandard-compressed article text |

### Per-Chunk Blocklet Sub-Dicts

Each text chunk is stored as a sub-dict keyed by its zero-padded index (`"000"`, `"001"`, `"002"`, ...):

| Field | Type | Description |
|-------|------|-------------|
| `chunk` | `int` | Chunk index number |
| `symbol` | `str` | Stock ticker for this article |
| `n-grams` | `int` | N-gram count for this chunk |
| `tokenz` | `int` | Transformer token count for this chunk |
| `alphas` | `int` | Alphabetic character count |
| `sent_type` | `str` | Sentiment classification: `"positive"`, `"neutral"`, or `"negative"` |
| `sent_score` | `float` | Raw sentiment confidence score from the model |
| `trct_state` | `str` | Chunker method used (truncation state identifier) |

### Example JSON Value (abbreviated)

```json
{
  "article": 0,
  "urlhash": "f308c6c74e14976ac6e940c20a329c5e063cf5cfde402d591cfcd28ace1c2b2d",
  "total_tokens": 412,
  "total_words": 308,
  "chars_count": 1847,
  "scentence": 14,
  "paragraph": 6,
  "random": 3,
  "positive_count": 3,
  "neutral_count": 2,
  "negative_count": 1,
  "chunk_count": 6,
  "zstd_blob": "KLUv/QBY...",
  "000": {
    "chunk": 0,
    "symbol": "AAPL",
    "n-grams": 47,
    "tokenz": 68,
    "alphas": 312,
    "sent_type": "positive",
    "sent_score": 0.9823,
    "trct_state": "full"
  },
  "001": { "..." }
}
```

---

## Decompressing the `zstd_blob`

The article text is compressed with Zstandard and Base64-encoded before storage:

```python
import base64
import zstandard as zstd

b64_data = lmdb_value_dict["zstd_blob"]
raw_bytes = base64.b64decode(b64_data)
decompressor = zstd.ZstdDecompressor()
article_text = decompressor.decompress(raw_bytes).decode('utf-8')
```

This is the same decompression pattern used by `dump_db.py`'s `dump_lmdb_articles()` function.

---

## Integration with `dump_db.py`

`dump_db.py` is a standalone CLI inspection utility built on top of `lmdb_io_eng`. It imports the class directly and provides four dump modes:

| Flag | Function | Description |
|------|----------|-------------|
| `-b` / `--basic` | `dump_lmdb_basic()` | One-line view of all KV entries (key + first 40 chars of value) |
| `-d` / `--deep` | `dump_lmdb_by_key()` | Full chunk-by-chunk breakdown filtered by ticker or URL hash fragment (requires `-k`) |
| `-x` / `--xray` | `dump_lmdb_xray()` | Full pretty-printed JSON for entries matching a key filter (requires `-k`) |
| `-a` / `--articles` | `dump_lmdb_articles()` | Decompress and print full article text for a ticker (requires ticker symbol and optional count) |
| `-i` / `--init` | inline | Drop all keys from the default DB (reset) |

### Usage Examples

```bash
# Simple 1-line summary of all entries
python dump_db.py --basic

# Deep dump for all AAPL articles
python dump_db.py --deep --key AAPL

# Deep dump for a specific URL hash prefix
python dump_db.py --deep --key f308c6c7

# Full JSON xray for a specific article
python dump_db.py --xray --key f308c6c7

# Decompress and print article text for 3 NVDA articles
python dump_db.py --articles NVDA 3

# Reset (drop all data from) the DB
python dump_db.py --init

# Enable verbose logging
python dump_db.py --basic --verbose
```

---

## Integration with `aop.py` / `xop.py`

Both main entry points instantiate `lmdb_io_eng` and pass it to the NLP engine before starting the article processing loop:

```python
from datastore_eng_LMDB import lmdb_io_eng

lmdb_dbname = "LMDB_0001"
lmdb_env = lmdb_io_eng("GLOBAL", lmdb_dbname, args)  # create instance

# ... pass lmdb_env to ml_nlpreader / ml_yf_nlp_news_engine ...
articles_found = asyncio.run(news_ai.nlp_read_one(news_symbol, args))
```

Inside `ml_yf_nlp_news_engine`, the instance is assigned to either `self.BS4_lmdb_env` or `self.C4_lmdb_env` depending on the extraction engine being invoked.

---

## Open/Close Lifecycle

LMDB environments must be explicitly opened before use and closed after. The `kv_cache_engine()` manages its own open/close cycle internally, but when calling other methods directly you must manage the lifecycle yourself:

```python
# Read-only lifecycle
inst = lmdb_io_eng("my_caller", "LMDB_0001", args)
inst.open_lmdb_RO("my_caller")
inst.dump_lmdb_RO("my_caller")
inst.close_lmdb("my_caller")

# Read-write lifecycle
inst = lmdb_io_eng("my_caller", "LMDB_0001", args)
inst.open_lmdb_RW("my_caller")
# ... write transactions ...
inst.close_lmdb("my_caller")

# Drop all keys
inst = lmdb_io_eng("my_caller", "LMDB_0001", args)
# Note: drop_lmdb_RW opens and closes internally — do NOT pre-open
inst.drop_lmdb_RW("my_caller")
```

**Important:** LMDB does not support simultaneous RO and RW environments on the same path from the same process. Always call `close_lmdb()` before switching modes. `kv_cache_engine()` does this automatically when it detects an open environment.

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `lmdb` | Lightning Memory-Mapped Database Python bindings |
| `json` | Serialize/deserialize the JSON value payload |
| `logging` | Structured debug and info logging |
| `rich` | Rich terminal print output (`from rich import print`) |
| `typing` | Type annotations (`Any`, `Dict`, `List`, `Tuple`, `Optional`) |

`lmdb` must be installed:
```bash
pip install lmdb
```

---

## Error Handling

All public methods catch two exception classes:
- `lmdb.Error` — LMDB-specific errors (permission denied, DB not found, map full, etc.)
- `Exception` — catch-all for unexpected failures

Both print an error message to stdout and return a non-zero error code. No exception propagates to the caller.

---

## Known Limitations and Notes

- **Single default DB per environment.** `drop_lmdb_RW()` uses `max_dbs=0` and `key=None` to target only the default DB. Named sub-databases are not used.
- **`db_open_state` is a class-level dict.** Across multiple instances sharing the same class, this dict is shared. In practice each `lmdb_io_eng` instance is associated with a uniquely named DB so collisions do not occur.
- **`cursor` class attribute is stored but not used.** It was intended for a global cursor, but all current transaction cursors are opened locally within `with txn:` blocks.
- **`dump_kvcache_bs4` is unreachable.** The private helper is defined inside `kv_cache_engine` at the wrong indentation level — it is valid Python but logically dead code.
- **`sent_ai` class attribute.** The `sent_ai` attribute on the class is never used internally; sentiment AI is always passed as a parameter to `kv_cache_engine()`.

---

## Version History

| Version | Date | Author | Notes |
|---------|------|--------|-------|
| 1.0.0 | 2026-06-18 | Claude Sonnet 4.6 (Anthropic) | Initial README — full documentation of `datastore_eng_LMDB.py` generated by automated codebase analysis |
