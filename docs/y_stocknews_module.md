# y_stocknews.py — Module Documentation

## Overview

`y_stocknews.py` defines the `yfnews_reader` class, a Yahoo Finance news scraper and multi-depth article pipeline for the Bespin quant analysis platform. It is responsible for fetching, classifying, and extracting structured data from Yahoo Finance news feeds for a given stock ticker symbol.

The module sits in the **news ingestion layer** of the Bespin pipeline — between raw HTML/JS page fetching and the downstream ML/NLP sentiment analysis engine.

---

## Role in the Bespin Pipeline

```
Yahoo Finance News URL
        │
        ▼
  yfnews_reader (y_stocknews.py)
  ┌──────────────────────────────┐
  │  Depth 0: scan_news_feed()   │  ← enumerate articles from news feed page
  │  Depth 1: eval_news_feed_    │  ← classify each article, build NLP candidate list
  │           stories()          │
  │  Depth 2: interpret_page()   │  ← per-article page type detection & metadata
  │  Depth 3: extract_article_   │  ← full text extraction + sentiment AI invocation
  │           data()             │
  └──────────────────────────────┘
        │
        ▼
  ml_ingest{} dict  →  ML/NLP sentiment pipeline (ml_sentiment.py)
```

The orchestrator for this pipeline is `ml_yf_nlp_orchestrator.py`, which instantiates `yfnews_reader` and drives calls through the depth stages.

---

## Dependencies

| Library | Purpose |
|---|---|
| `requests` | HTTP session management, cookie handling |
| `requests_html` | HTMLSession for basic HTML page fetching |
| `bs4` (BeautifulSoup) | HTML parsing and DOM traversal |
| `playwright` | JavaScript-rendered page fetching (Chromium) |
| `pandas` | Sentiment stats DataFrames |
| `numpy` | Numerical support |
| `hashlib` | URL → SHA-256 hash (cache keys) |
| `re` | Regex string cleaning (e.g. stripping `%` from captions) |
| `rich` | Terminal output formatting |
| `urllib.parse` | URL component parsing |

---

## Class: `yfnews_reader`

### Instantiation

```python
reader = yfnews_reader(yti, symbol, global_args)
```

| Parameter | Type | Description |
|---|---|---|
| `yti` | int | Unique instance identifier (used in log prefixes) |
| `symbol` | str | Stock ticker symbol (e.g. `"IBM"`) |
| `global_args` | dict | Runtime flags passed from the CLI orchestrator |

### Key `global_args` flags

| Key | Type | Effect |
|---|---|---|
| `bool_xray` | bool | Enable verbose debug output to terminal |

---

## Class Attributes (Global State)

| Attribute | Type | Description |
|---|---|---|
| `symbol` | str | Stock ticker for this instance |
| `yfqnews_url` | str | Active Yahoo Finance news URL (set by `form_endpoint()`) |
| `js_session` | HTMLSession | Shared HTTP session object |
| `js_resp0` / `js_resp2` | Response | HTML session response handles |
| `yfn_all_data` | dict | Raw JSON dataset (reserved) |
| `yfn_htmldata` | str | Full page HTML text in memory |
| `yfn_jsdata` | str | Full page text after JS rendering |
| `yfn_jsdb` | dict | **URL cache**: maps `SHA-256(url)` → response object |
| `ml_brief` | list | Flat list of article titles (Count Vectorizer input) |
| `ml_ingest` | dict | **NLP candidate database**: maps integer index → article dict |
| `extracted_articles` | list | Crawl4ai-extracted article objects (set during `scan_news_feed`) |
| `nsoup` | BeautifulSoup | Shared BS4 parse tree (reused across depth levels) |
| `sen_stats_df` | DataFrame | Aggregated per-article sentiment scores |
| `article_url` | str | URL of the article currently being processed |
| `url_netloc` | str | Netloc component of the current article URL |
| `yti` | int | Instance identifier |
| `cycle` | int | Thread loop counter |
| `nlp_x` | int | Global NLP article counter (incremented per article) |
| `get_counter` | int | Total HTTP GET requests made by this instance |

### `ml_ingest` Record Schema

Each entry in `ml_ingest` is a dict keyed by an integer index:

```python
{
    "symbol":    str,   # ticker
    "urlhash":   str,   # SHA-256 of article URL (cache key)
    "type":      int,   # article type code (mirrors uhint)
    "thint":     float, # confidence/type hint score
    "uhint":     int,   # URL hinter result code
    "url":       str,   # article URL (Yahoo Finance local)
    "teaser":    str,   # article headline/teaser text
    "publisher": str,   # news publisher name
    "title":     str,   # article title
    # Added by interpret_page():
    "exturl":    str,   # (optional) external/remote article URL
    "viable":    int,   # 1 = full text extractable, 0 = stub/limited
}
```

---

## URL Hinter Type Codes

The `ml_urlhinter` class (`yfn_uh`) classifies each article URL and returns a `uhint` code that drives all downstream branching:

| `uhint` | `thint` | Article Type |
|---|---|---|
| 0 | 0.0 | Real news — full article locally hosted on Yahoo Finance |
| 1 | 1.0 / 1.1 | Fake news stub — micro article linking out to external site |
| 2 | 4.0 | Video report — minimal supporting text |
| 3 | 1.1 | Remote article — externally hosted, not interpretable |
| 4 | 7.0 | Research report |
| 5 | 6.0 | Bulk Yahoo premium service |
| other | 9.9 | Unknown |

---

## Methods

### Session & Connection Setup

#### `init_dummy_session()`
Fires a GET to a fixed Yahoo Finance screener URL (`day_losers`) to prime the session and obtain live cookies without storing the response.

#### `init_live_session(id_url)`
GETs the given URL to refresh session cookies into `yahoo_headers`.

#### `update_headers(ch)`
Updates the `path` cookie in the active session. Called before fetching an article page so the cookie reflects the correct path.

#### `update_cookies()`
Pulls the Yahoo `A1` authentication cookie from `js_resp0` and injects it into the session cookie jar.

---

### URL Endpoint

#### `form_endpoint(symbol)`
Builds and stores the Yahoo Finance news URL for the given ticker:
```
https://finance.yahoo.com/quote/{SYMBOL}/news/
```
Result stored in `self.yfqnews_url`. This is the canonical starting URL for all downstream operations.

---

### Page Fetching

Three fetch engines are available, all writing into `yfn_jsdb` with a SHA-256 URL hash as the cache key.

#### `do_simple_get(url)` — Method 8
Basic HTML fetch via `HTMLSession.get()`. No JavaScript rendering. Fastest option. Stores raw HTML in `yfn_htmldata`. Returns the URL hash.

#### `ext_do_js_get(idx_x)` — Method 7
Fetches via `requests_html` and calls `.html.render()` to execute JavaScript (uses a Chromium headless browser under the hood). Stores rendered text in `yfn_jsdata`. Returns the URL hash.

#### `ext_pw_js_get(idx_x)` — Method 7.1
Fetches via **Playwright** Chromium. More reliable than `requests_html` rendering. Navigates to `yfqnews_url`, waits for `networkidle`, then captures both raw HTML (`page.content()`) and visible text (`document.body.innerText`). Creates a `MockResponse` wrapper to maintain cache compatibility. Returns the URL hash.

---

### Depth 0 — News Feed Scan

#### `scan_news_feed(symbol, depth, scan_type, bs4_obj_idx, hash_state)` — Method 9

Enumerates articles from the news feed page. Does **not** make any network requests — operates on data already in `yfn_jsdb`.

- `scan_type=1` (crawl4ai): reads `yfn_jsdb[hash_state]['data']` and stores the list of article dicts into `self.extracted_articles`.
- `scan_type=0` (legacy BS4): dead code path — structurally unreachable after a `return` statement. The BS4 code remains as commented reference.

If `bool_xray` is set, prints a numbered list of article titles found.

---

### Depth 1 — Story Evaluation

#### `eval_news_feed_stories(symbol)` — Method 10

Iterates over `self.extracted_articles` (populated by `scan_news_feed`) and:

1. Extracts `Title`, `Ext_url`, `Publisher` from each crawl4ai article dict.
2. Normalises the publisher name (strips `•`-delimited suffixes).
3. Resolves article URL to absolute form (prepends `https://finance.yahoo.com` for relative paths).
4. Calls `yfn_uh.uhinter()` to classify the URL type.
5. Maps `uhint` → `thint` confidence score.
6. Calls `yfn_uh.confidence_lvl()` for a human-readable classification label.
7. Hashes the article URL for use as cache/DB key.
8. Appends article title to `ml_brief` (flat text list for BoW vectorizer).
9. Inserts a full record dict into `ml_ingest[nlp_x]`.

Output: `ml_ingest` populated with all viable article candidates.

---

### Depth 2 — Page Interpretation

#### `interpret_page(item_idx, data_row)` — Method 11

Per-article page type detection and metadata extraction. Accepts an `ml_ingest` record dict (`data_row`) and `item_idx` as its integer key.

**Cache-first pattern**: checks `yfn_jsdb[cached_state]` first; if missing, performs a live fetch via `do_simple_get()` and retries the cache lookup.

Branches on `uhint`:

| `uhint` | Depth | Behaviour |
|---|---|---|
| 0 | 2.0 | Local full article — extracts author (`byline-attr-author`) and publish date (`byline-attr-time-style`); marks `viable=1` |
| 1 | 2.1 | Fake stub — locates `h1.cover-title`, author, date, and the external `<a href>` link; stores `exturl` in `data_row`; marks `viable=0` |
| 2 | 2.2 | Video stub — checks for minimal `<p>` text zone; marks `viable=0` |
| 3 | 2.2 | Remote article — no interpretation possible; marks `viable=0` |
| 4 | 2.2 | Research report — marks `viable=0` |
| other | 2.10 | Unknown state — returns error tuple |

Returns a tuple: `(uhint, thint, url_or_exturl)`.

---

### Depth 3 — Article Text Extraction

#### `extract_article_data(item_idx, sentiment_ai)` — Method 12

Full text extraction and ML invocation for a single article. Only called for articles that are locally hosted (i.e. `external=False`).

**Cache-first pattern**: identical to `interpret_page()` — checks `yfn_jsdb` first, then fetches live if missing.

For local articles:
1. Locates the article body (`body yf-1ir6o1g`), meta zone, stub news zone, and all `<p>` tags (`local_stub_news_p`).
2. Calls `sentiment_ai.compute_sentiment(symbol, item_idx, local_stub_news_p, urlhash)` — hands off to the HuggingFace Transformer pipeline.
3. Assembles a per-article sentiment summary row and appends it to `self.sen_stats_df`.

Returns `(total_tokens, total_words, total_scent)` from the sentiment pass.

For external/stub articles: logs a skip message and returns early — teaser text captured in Depth 1 is used instead.

---

### Utility / Debug

#### `share_hinter(hinst)` — Method 5
Injects the shared `ml_urlhinter` instance into `self.yfn_uh`. Must be called before any depth scan.

#### `dump_ml_ingest()` — Method 13
Prints a formatted table of all records in `ml_ingest{}` — symbol, URL hash, type/hint scores, and local/external URLs. Used for debug inspection.

---

## Operational Flow (Typical Call Sequence)

```python
reader = yfnews_reader(1, "IBM", args)

reader.share_hinter(url_hinter_instance)      # inject URL classifier
reader.form_endpoint("IBM")                   # set yfqnews_url
reader.init_live_session(reader.yfqnews_url)  # prime cookies

# Depth 0 — requires data already in yfn_jsdb (populated by crawl4ai upstream)
reader.scan_news_feed("IBM", 0, 1, idx, hash_key)

# Depth 1
reader.eval_news_feed_stories("IBM")          # populates ml_ingest{}

# Depth 2 — iterate over ml_ingest candidates
for idx, row in reader.ml_ingest.items():
    uhint, thint, url = reader.interpret_page(idx, row)

# Depth 3 — extract text and run sentiment
for idx in reader.ml_ingest:
    reader.extract_article_data(idx, sentiment_ai_instance)

# Inspect results
reader.dump_ml_ingest()
print(reader.sen_stats_df)
```

---

## Caching Strategy

All fetched pages are stored in `yfn_jsdb`, a plain Python dict acting as an in-process URL cache:

- **Key**: `SHA-256(url).hexdigest()` — 64-char hex string
- **Value**: raw `requests` / `requests_html` response object (or `MockResponse` for Playwright)

Every depth method follows a **cache-first, fetch-on-miss** pattern. This avoids redundant network requests when the same article URL is encountered at multiple depth levels.

> **Note**: The cache is instance-scoped and not persisted between runs. For cross-run persistence, the LMDB store (`datastore_eng_LMDB.py`) is used elsewhere in Bespin.

---

## Known Issues / Technical Debt

| Area | Note |
|---|---|
| **Dead code in `scan_news_feed()`** | The entire BS4 code path (scan_type=0) is structurally unreachable — a bare `return` exits the method before reaching it. Marked "Delete Me !!" in source. |
| **`init_dummy_session()` signature mismatch** | `extract_article_data()` calls `self.init_dummy_session(durl)` with an argument, but the method signature takes no parameters. |
| **`cy_soup` used before assignment** | In `extract_article_data()`, `cy_soup = self.yfn_jsdb[cached_state]` is called before the cache miss branch does a live fetch — will raise `KeyError` on a cold cache. |
| **BS4 CSS class names** | Yahoo Finance periodically changes its CSS class names (e.g. `body yf-1ir6o1g`, `byline yf-1k5w6kz`). These are hardcoded and require manual updates when the site redesigns. |
| **`escape()` applied to raw HTML** | `BeautifulSoup(escape(dataset), ...)` escapes HTML entities before parsing, which can corrupt tag structure. |
| **Duplicate class definition** | `ml_yf_nlp_news_engine.py` also defines a `yfnews_reader` class with crawl4ai integration. The orchestrator imports from that file; `y_stocknews.py` may be a parallel legacy version. |
