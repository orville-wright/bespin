# ml_yf_nlp_news_engine.py

## Yahoo Finance News Scraper — Dual Engine (Crawl4ai + BS4)

`ml_yf_nlp_news_engine.py` is the production-grade Yahoo Finance news reader at the core of Bespin's ML/NLP sentiment pipeline. It implements a **4-depth crawl pipeline** with two independent article extraction engines — Crawl4ai (async, CSS schema-based) and BeautifulSoup4 (HTML session-based) — and feeds extracted article text into the HuggingFace sentiment analysis pipeline via `ml_sentiment.py`.

---

## Module Role in the Bespin Architecture

```
aop.py / xop.py  (CLI orchestrators)
        │
        ▼
ml_yf_nlp_orchestrator.py  (ml_nlpreader class)
        │
        ├── Depth 0: yahoofin_news_depth0()    ← Crawl4ai async news feed skim
        ├── Depth 1: eval_news_feed_stories()  ← Article candidate evaluation
        ├── Depth 2: interpret_page_depth2()   ← Article type interpretation
        └── Depth 3: artdata_BS4_depth3()      ← BS4 full article extraction
                     artdata_C4_depth3()       ← Crawl4ai full article extraction
                            │
                            ▼
                    ml_sentiment.py            (HuggingFace NLP pipeline)
                            │
                            ▼
                   datastore_eng_LMDB.py       (Dual KV cache: C4 + BS4 stores)
```

This module is instantiated and driven by `ml_yf_nlp_orchestrator.py::ml_nlpreader.nlp_read_one()`. It is never called directly from `aop.py` or `xop.py`.

---

## Class: `yfnews_reader`

```python
from ml_yf_nlp_news_engine import yfnews_reader

reader = yfnews_reader(yti=1, symbol="AAPL", global_args=args_dict)
```

### Constructor Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `yti` | `int` | Unique instance identifier (used in debug/logging labels) |
| `symbol` | `str` | Stock ticker symbol (e.g. `"AAPL"`) |
| `global_args` | `dict` | CLI args dict passed from the main orchestrator |

### Key `global_args` Keys Used

| Key | Type | Purpose |
|-----|------|---------|
| `bool_verbose` | `bool` | Enable verbose terminal output |
| `bool_xray` | `bool` | Enable deep debug data dumps |

---

## Class-Level Attributes

These are set at the class level (shared defaults) and overridden on each instance:

| Attribute | Type | Description |
|-----------|------|-------------|
| `symbol` | `str` | Stock ticker being processed |
| `yti` | `int` | Instance UID |
| `yfqnews_url` | `str` | The Yahoo Finance news endpoint URL (set by `form_endpoint()`) |
| `extracted_articles` | `list` | Raw article data extracted by Crawl4ai at Depth 0 |
| `articles_found` | `int` | Count of articles found in the Depth 0 skim |
| `ml_ingest` | `dict` | Master candidate article dataset (`{ nlp_x: article_row_dict }`) |
| `ml_brief` | `list` | List of article titles (reserved for future Naive Bayes use) |
| `yfn_jsdb` | `dict` | In-memory cache for HTML session get() responses, keyed by URL hash |
| `yfn_c4_result` | `dict` | In-memory cache for Crawl4ai extraction results, keyed by URL hash |
| `C4_lmdb_env` | `lmdb_io_eng` | LMDB instance for Crawl4ai article cache |
| `BS4_lmdb_env` | `lmdb_io_eng` | LMDB instance for BeautifulSoup4 article cache |
| `sent_ai` | `ml_sentiment` | HuggingFace sentiment analysis instance (set at Depth 3) |
| `sen_stats_df` | `pd.DataFrame` | Aggregated sentiment stats for articles processed |
| `yfn_uh` | `url_hinter` | URL type classifier instance (shared from orchestrator) |
| `YF_sym_main_schema` | `str` | Path to `json/YF_sym_main_schema.json` (Depth 0 skim schema) |
| `YF_sym_article_schema` | `str` | Path to `json/YF_sym_article_schema.json` (Depth 3 article schema) |
| `nlp_x` | `int` | Running counter for entries in `ml_ingest` |
| `result_engine` | `str` | Tracks which cache engine path was used for the last article |

---

## 4-Depth Crawl Pipeline

The pipeline processes one stock symbol at a time. Each depth builds on the previous:

```
Depth 0: Top-level news feed skim
         → Crawl4ai async scrape of finance.yahoo.com/quote/SYM/news/
         → Extracts: Title, Teaser, Publisher, Ext_url for all articles
         → Returns: url_hash of the news feed page

Depth 1: News feed story evaluation
         → Cycles through self.extracted_articles (from Depth 0)
         → Classifies each article URL via url_hinter (uhint codes 0–10)
         → De-duplicates articles by URL hash
         → Populates self.ml_ingest{} with candidate article metadata

Depth 2: Article page interpretation
         → No network requests
         → For each entry in ml_ingest, determines article "viability"
         → Sets viable=1 (local full article) or viable=0 (stub/video/external)
         → Returns (uhint, thint, durl) for routing to the correct Depth 3 engine

Depth 3a: BS4 article extractor
         → Full HTML get() via requests_html HTMLSession
         → Parses <p> tags via BeautifulSoup4
         → Extracts text from CSS class `body yf-v6n2s3`
         → Runs HuggingFace sentiment via ml_sentiment.compute_sentiment(ext=1)
         → Writes result to BS4 LMDB cache
         → Returns (total_tokens, total_words, bs4_final_results)

Depth 3b: Crawl4ai article extractor
         → Async crawl via c4_engine_depth3()
         → Extracts text using json/YF_sym_article_schema.json CSS selectors
         → Targets CSS class `div.body.yf-v6n2s3 div.bodyItems-wrapper`
         → Detects PREMIUM paywalled articles (early exit)
         → Runs HuggingFace sentiment via ml_sentiment.compute_sentiment(ext=0)
         → Writes result to C4 LMDB cache
         → Returns (total_tokens, total_words, c4_final_results)
```

---

## Method Reference

### `__init__(yti, symbol, global_args)` — Constructor

Initializes instance state. Sets up the Crawl4ai JSON schema file paths relative to the module's directory.

```python
reader = yfnews_reader(1, "NVDA", args)
```

---

### `form_endpoint(symbol)` — Build News URL

Sets `self.yfqnews_url` — the Yahoo Finance news endpoint for the given symbol.

```python
reader.form_endpoint("NVDA")
# → self.yfqnews_url = "https://finance.yahoo.com/quote/NVDA/news/"
```

Available Yahoo Finance URL patterns:
- All news: `https://finance.yahoo.com/quote/IBM/?p=IBM`
- Symbol news: `https://finance.yahoo.com/quote/IBM/news?p=IBM`
- Press releases: `https://finance.yahoo.com/quote/IBM/press-releases?p=IBM`
- Research reports: `https://finance.yahoo.com/quote/IBM/reports?p=IBM`

---

### `async yahoofin_news_depth0(idx_x)` — Depth 0: News Feed Skim

**Async method.** Performs a full-page Crawl4ai scrape of the Yahoo Finance news feed for `self.yfqnews_url`.

**What it does:**
1. Loads `json/YF_sym_main_schema.json` CSS extraction schema
2. Configures Crawl4ai with `JsonCssExtractionStrategy`, full-page scroll JS, and `CacheMode.BYPASS`
3. Runs `AsyncWebCrawler.arun()` asynchronously
4. Parses `result.extracted_content` → `self.yfn_crawl_data` (list of article dicts)
5. Stores the result in `self.yfn_jsdb[aurl_hash]`

**Returns:** `aurl_hash` (str) on success, `None` on failure.

**Crawl4ai JS commands executed at Depth 0:**
```javascript
window.scrollTo(0, document.body.scrollHeight);
await new Promise(resolve => setTimeout(resolve, 1000));
```

**Schema used (`YF_sym_main_schema.json`):**
```json
{
  "baseSelector": "section.container.yf-1m8w4l1 ul.stream-items.yf-ydpc1 li",
  "fields": [
    { "name": "Title",     "selector": "div a",           "type": "attribute", "attribute": "title" },
    { "name": "Teaser",    "selector": "div div img",      "type": "attribute", "attribute": "alt"   },
    { "name": "Ext_url",   "selector": "div a",           "type": "attribute", "attribute": "href"  },
    { "name": "Publisher", "selector": "div div div div", "type": "text" }
  ]
}
```

---

### `list_news_candidates_depth0(symbol, depth, scan_type, hash_state)` — Depth 0: Report

Validates the Depth 0 result and prints a numbered article list to the terminal. Sets `self.extracted_articles` from the cached data in `self.yfn_jsdb[hash_state]`.

**Returns:** `article_count` (int)

---

### `eval_news_feed_stories(symbol)` — Depth 1: Article Evaluation

Iterates through `self.extracted_articles` and builds the `ml_ingest{}` master candidate dataset.

**For each article:**
1. Extracts `Title`, `Ext_url`, `Publisher`, `Teaser` from the crawl4ai data
2. Validates URL format (`http`/`https` prefix)
3. Calls `url_hinter.uhinter()` to classify the URL type → `uhint` (0–10)
4. Maps `uhint` to `thint` (float confidence hint)
5. Performs URL deduplication via SHA-256 hash set membership test
6. Appends unique entries to `self.ml_ingest{self.nlp_x: nd}`

**`ml_ingest` row structure:**
```python
{
    "symbol":    "AAPL",
    "urlhash":   "sha256_hex_string",
    "type":      0,          # uhint code (0=local article, 1=stub, 2=video, etc.)
    "thint":     0.0,        # float confidence hint
    "uhint":     0,          # url_hinter code
    "publisher": "Reuters",
    "title":     "Article headline text",
    "teaser":    "Article teaser/summary text",
    "url":       "https://finance.yahoo.com/news/..."
}
```

**Returns:** `(0, bad_url_count)` on success, `1` if no articles available.

---

### `interpret_page_depth2(item_idx, data_row)` — Depth 2: Page Interpretation

Reads one `ml_ingest` row and determines article viability based on `uhint` code. Sets `data_row['viable']` flag and updates `self.ml_ingest[item_idx]`.

**Viability mapping:**

| `uhint` | Type | `viable` |
|---------|------|---------|
| 0 | Local full article | 1 |
| 1 | Fake local stub | 0 |
| 2 | Video | 0 |
| 3 | External publication | 0 |
| 4 | Research report | 0 |
| other | Unknown | 0 |

**Returns:** `(uhint, thint, durl)` — the type code, confidence hint, and destination URL.

---

### `artdata_BS4_depth3(item_idx, sentiment_ai, lmdb_inst)` — Depth 3: BS4 Extractor

**Network-heavy synchronous method.** Reads one article by URL using BeautifulSoup4 and runs the NLP sentiment pipeline.

**Flow:**

```
1. Check LMDB cache (via lmdb_inst.kv_cache_engine())
   ├── Cache HIT  → rehydrate sentiment counts from LMDB, return early
   └── Cache MISS → proceed to network read

2. Check yfn_jsdb{} for cached HTML session response
   ├── Found → use cached HTML
   └── Missing → init_live_session() + do_simple_get() to fetch HTML

3. Parse HTML with BeautifulSoup4
   → soup.find(attrs={"class": "body yf-v6n2s3"})   # article body
   → local_news.find_all("p")                         # all paragraph tags

4. Call sentiment_ai.compute_sentiment(symbol, item_idx, local_stub_news_p, hs, ext=1)
   → HuggingFace transformer NLP pipeline (BS4 mode)

5. Build bs4_final_results dict with sentiment counts + token/word/char metrics

6. Re-open LMDB in RW mode and write new cache entry
   → Key format: "0001.{SYMBOL}.{url_hash}"

7. Close LMDB

8. Return (total_tokens, total_words, bs4_final_results)
```

**BS4 CSS target classes:**
- `"body yf-v6n2s3"` — Main article body wrapper
- `"main yf-cfn520"` — Article metadata (title, publisher, date) above the body
- `"body yf-3qln1o"` — Alternative article body class (stub format)

**Returns:** `(total_tokens, total_words, bs4_final_results)` or `(0, 0, None)` on failure.

**`bs4_final_results` dict structure:**
```python
{
    'article':        item_idx,
    'urlhash':        url_hash_str,
    'total_tokens':   int,
    'chars_count':    int,
    'total_words':    int,
    'scentence':      int,   # sentence chunk count
    'paragraph':      int,   # paragraph chunk count
    'random':         int,   # random text chunk count
    'positive_count': int,
    'neutral_count':  int,
    'negative_count': int,
    'bs4_rows':       int    # number of <p> tags found
}
```

---

### `artdata_C4_depth3(item_idx, sentiment_ai, lmdb_inst)` — Depth 3: Crawl4ai Extractor

**Network-heavy synchronous method.** Reads one article using Crawl4ai CSS schema extraction and runs the NLP sentiment pipeline.

**Flow:**

```
1. Check LMDB cache (via lmdb_inst.kv_cache_engine())
   ├── Cache HIT  → rehydrate, return early
   └── Cache MISS → proceed to Crawl4ai extraction

2. Check yfn_jsdb{} for cached Crawl4ai result
   ├── Found → use cached result
   └── Missing → asyncio.run(c4_engine_depth3(durl, item_idx))

3. Extract article paragraphs from c4_dict['data']
   → for each element: art_all_p.append(element.get('Content'))

4. Handle special cases:
   → total_chars == 0: no data, return (0, 0, 0)
   → total_chars == 7: check for PREMIUM paywall flag, return (0, 0, None)

5. Call sentiment_ai.compute_sentiment(symbol, item_idx, art_all_p, hs, ext=0)
   → HuggingFace transformer NLP pipeline (Crawl4ai mode)

6. Re-open LMDB in RW mode and write new cache entry

7. Close LMDB

8. Return (total_tokens, total_words, c4_final_results)
```

**Returns:** `(total_tokens, total_words, c4_final_results)` or `(0, 0, 0)` / `(0, 0, None)` on failure.

---

### `async c4_engine_depth3(durl, item_idx)` — Crawl4ai Engine (private)

**Private async helper** for `artdata_C4_depth3()`. Performs the actual Crawl4ai article extraction.

**What it does:**
1. Loads `json/YF_sym_article_schema.json` CSS extraction schema
2. Configures Crawl4ai with `JsonCssExtractionStrategy` and `CacheMode.BYPASS`
3. Runs `AsyncWebCrawler.arun(durl, config=config)`
4. Parses `result.extracted_content` → `self.yfn_crawl_data` (list of content dicts)
5. Stores result in `self.yfn_c4_result[aurl_hash]`

**Schema used (`YF_sym_article_schema.json`):**
```json
{
  "baseSelector": "div.body.yf-v6n2s3",
  "fields": [
    { "name": "Content",         "selector": "div.bodyItems-wrapper", "type": "text" },
    { "name": "Premium_paywall", "selector": "a.topic-link",          "type": "text" }
  ]
}
```

**Crawl4ai JS commands at Depth 3:**
```javascript
window.scrollTo(0, document.body.scrollHeight);
await new Promise(resolve => setTimeout(resolve, 2000));
```

**Returns:** `CrawlResult` object on success, `None` on failure.

---

### `do_simple_get(_url)` — Basic HTML Fetch

Performs a simple `requests_html.HTMLSession.get()` on `_url` (no JavaScript rendering).

- Stores raw HTML text in `self.yfn_htmldata`
- Creates entry in `self.yfn_jsdb[url_hash]` with keys `url`, `data`, `result`
- Used as the fallback network read path in `artdata_BS4_depth3()`

**Returns:** `(error_code, url_hash)` — `error_code=0` on success, `1` on HTTP failure.

---

### `init_live_session(id_url)` — Session Warm-Up

Performs a `requests.get()` to populate `yahoo_headers` with live cookies from Yahoo Finance before the BS4 article read.

---

### `update_headers(ch)` — Cookie Path Update

Updates the `path` cookie in the `requests_html` session after `init_live_session()`.

---

### `share_hinter(hinst)` — Inject URL Hinter

Accepts an external `url_hinter` instance and assigns it to `self.yfn_uh`. Called by the orchestrator before any depth processing begins.

---

### `dump_ml_ingest()` — Debug Dump

Prints the contents of `self.ml_ingest{}` to the terminal for debugging. Lists all NLP candidate articles with their metadata.

---

## LMDB Caching Architecture

Both Depth 3 extractors check the LMDB cache **before** making network requests. On a cache hit, sentiment metrics are fully rehydrated from the stored JSON, and the HuggingFace LLM pipeline is **skipped entirely**.

```
Article URL hash
       │
       ▼
kv_cache_engine("BS4" or "C4", symbol, data_row, item_idx, sent_ai, extr_eng)
       │
       ├── ec=0 → Cache HIT  → rehydrate sentiment from JSON → return early
       ├── ec=1 → Deserialize failure → force network read
       ├── ec=2 → No URL hash key → force network read
       ├── ec=3 → Cache MISS → force network read
       └── ec=4 → LMDB RO open failure → force network read
```

**LMDB key format:**
```
"0001.{SYMBOL}.{SHA256_URL_HASH}"
e.g. "0001.AAPL.3f4a8b2c1d..."
```

**LMDB value format:** JSON-serialized `_final_data_dict` containing all sentiment chunk data, token counts, and ZSTD-compressed article text.

**Two separate LMDB stores:**
- `C4_lmdb_env` — Crawl4ai extraction results (`datastore/` path, managed by `lmdb_io_eng`)
- `BS4_lmdb_env` — BeautifulSoup4 extraction results (same path, different DB name)

---

## URL Classification

Articles are classified before Depth 3 processing by `ml_urlhinter.url_hinter`:

| `uhint` | `thint` | URL Pattern | Type | Viable |
|---------|---------|-------------|------|--------|
| 0 | 0.0 | `/news/`, `/markets/`, `/sectors/`, `/economy/`, `/technology/`, etc. | Local full article | Yes |
| 1 | 1.0 | `/m/...`, `/live/...` | Fake local micro-stub | No |
| 2 | 4.0 | `/video/...` | Video content | No |
| 3 | 1.1 | Absolute URL (`https://www.otherdomain.com/...`) | External filler page | No |
| 4 | 7.0 | `/research/...` | Research/analyst report | No |
| 5 | 6.0 | `/about/...` | Premium subscription page | No |
| 9 | 9.9 | Unknown path segment | Not yet defined | No |
| 10 | — | Parse error | Mangled URL | No |

The `thint` float is passed to `confidence_lvl()` which maps it to a human-readable article locality description:

| `thint` | Description |
|---------|-------------|
| 0.0 | Full Local article page |
| 1.0 | Fake local micro-stub |
| 1.1 | External publication link |
| 2.0 | Op-Ed page |
| 4.0 | Video story page |
| 6.0 | Premium subscription ad |
| 7.0 | Research report page |
| 9.9 | Unknown page structure |

---

## Data Flow Between Depths

```python
# Depth 0 → sets:
self.yfn_crawl_data       # list of article dicts from Crawl4ai
self.yfn_jsdb[aurl_hash]  # { url, data, result } for the news feed page

# Depth 0 list_news_candidates → sets:
self.extracted_articles   # alias to yfn_jsdb[hash]['data']
self.articles_found       # count of articles in extracted_articles

# Depth 1 → populates:
self.ml_ingest            # { nlp_x: { symbol, urlhash, type, thint, uhint,
                          #            publisher, title, teaser, url } }
self.ml_brief             # [ title_str, ... ]  (unused)

# Depth 2 → updates:
self.ml_ingest[item_idx]['viable']  # 1 or 0

# Depth 3 BS4 → populates:
self.yfn_jsdb[url_hash]   # { url, data, result } for each article HTML session
self.articles_crawled     # { item_idx: BeautifulSoup_object }
self.sen_stats_df         # DataFrame: [ art, urlhash, positive, neutral, negative ]

# Depth 3 C4 → populates:
self.yfn_c4_result[url_hash]  # { url, data, result } for each Crawl4ai extraction
self.articles_crawled         # { item_idx: CrawlResult }
self.sen_stats_df             # DataFrame: [ art, urlhash, positive, neutral, negative ]
```

---

## Dependencies

| Module | Usage |
|--------|-------|
| `crawl4ai` | `AsyncWebCrawler`, `JsonCssExtractionStrategy`, `CrawlerRunConfig`, `CacheMode` |
| `bs4 (BeautifulSoup4)` | HTML parsing for Depth 3 article text extraction |
| `requests_html` | `HTMLSession` for BS4 network reads |
| `requests` | Live session initialization and cookie management |
| `hashlib` | SHA-256 URL hashing for cache keys and deduplication |
| `asyncio` | Async execution context for Crawl4ai |
| `pandas` | `sen_stats_df` sentiment aggregation DataFrame |
| `numpy` | Imported but not used directly in this module |
| `datastore_eng_LMDB` | `lmdb_io_eng` — dual KV cache manager |
| `ml_urlhinter` | `url_hinter` — URL type classifier |
| `ml_sentiment` | `ml_sentiment` — HuggingFace NLP pipeline |

---

## Integration: How It Is Invoked

`ml_yf_nlp_orchestrator.py::ml_nlpreader.nlp_read_one()` is the sole caller:

```python
# 1. Instantiate
yfn = yfnews_reader(1, news_symbol, global_args)

# 2. Set the news endpoint URL
yfn.form_endpoint(news_symbol)

# 3. Share the URL hinter
yfn.yfn_uh = url_hinter(1, self.args)

# 4. Run Depth 0 (async)
url_hash0 = await yfn.yahoofin_news_depth0(0)

# 5. Run Depth 0 report + Depth 1 evaluation (sync)
articles_found = yfn.list_news_candidates_depth0(symbol, 0, 1, url_hash0)
eval_state, bad_url_count = yfn.eval_news_feed_stories(symbol)

# Depth 2 + 3 are driven by the calling orchestrator or aop.py loop:
for item_idx, row in yfn.ml_ingest.items():
    thint = nlpreader.nlp_summary_report(yti, item_idx)  # Depth 2
    if thint == 0.0:
        tokens, words, results = yfn.artdata_BS4_depth3(item_idx, sent_ai, bs4_lmdb)  # Depth 3
```

---

## Error Handling Patterns

Each method uses a consistent `cmi_debug` label format for structured logging:

```
ml_yf_nlp_news_engine::method_name.#yti.idx_ASYNC
```

Errors are logged with `logging.error()`, warnings with `logging.info()`. Network failures at Depth 3 return `(0, 0, None)` — callers must check for `None` before proceeding to the NLP stage.

The `yfn_jsdb` and `yfn_c4_result` caches use `KeyError` as the cache-miss signal (no explicit sentinel value), so `try/except KeyError` is the standard cache lookup pattern throughout.

---

## Known Limitations and TODOs

1. **`artdata_BS4_depth3`** — TODO: rename to `ext_artdata_BS4` (noted in docstring)
2. **`artdata_C4_depth3`** — TODO: rename to `ext_artdata_C4` (noted in docstring)
3. **`ml_brief` list** — Populated but not consumed; reserved for a future Naive Bayes classifier
4. **ZSTD storage** — The current v1 architecture uses Base64 JSON encoding which adds ~33% overhead; v2 design (noted in `ml_sentiment.zstd_text_compressor`) would use raw msgpack binary packing
5. **Parallel Depth 3** — `articles_crawled{}` dict is populated but not used for parallelism yet; parallel async Depth 3 extraction is a planned future feature
6. **External articles (uhint=3)** — Skipped at Depth 3; not sent to the NLP pipeline
7. **New Yahoo Finance news zones** — `url_hinter.uhinter()` calls `sys.exit(1)` if it encounters an unknown URL path segment; schema maintenance required when Yahoo restructures their URL zones

---

## Version History

| Version | Date | Notes |
|---------|------|-------|
| 1.0.0 | 2026-06-18 | Initial README — comprehensive architecture documentation generated by automated codebase analysis |
