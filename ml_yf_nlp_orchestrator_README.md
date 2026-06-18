# ml_yf_nlp_orchestrator.py

## Overview

`ml_yf_nlp_orchestrator.py` is the **async NLP pipeline coordinator** for the Bespin platform. It contains the `ml_nlpreader` class, which acts as the top-level controller that chains together Yahoo Finance news scraping, article classification, and sentiment routing for a single stock symbol.

This module sits between the CLI entry points (`aop.py` / `xop.py`) and the lower-level extraction and analysis engines. Its job is to orchestrate the first three depths of the four-depth news pipeline — leaving Depth 3 (full article extraction + LLM sentiment scoring) to be called separately by the caller.

---

## Role in the Bespin Architecture

```
aop.py / xop.py  (CLI entry points)
        │
        ▼
ml_nlpreader  ◄─── ml_yf_nlp_orchestrator.py  [THIS FILE]
        │
        ├──► yfnews_reader       (ml_yf_nlp_news_engine.py)
        │       ├── Depth 0: Crawl4ai async news feed skim
        │       ├── Depth 1: Article candidate evaluation
        │       └── Depth 2: Article page interpretation
        │
        └──► url_hinter          (ml_urlhinter.py)
                └── URL type classification (local/video/remote/ad)
```

`ml_nlpreader` does **not** perform the heavy network reads or LLM inference directly. It delegates to `yfnews_reader` for crawling and classification, and to `url_hinter` for URL routing. The caller is responsible for invoking Depth 3 (`artdata_BS4_depth3()` or `artdata_C4_depth3()`) and the sentiment engine (`ml_sentiment`).

---

## Imports and Dependencies

```python
from ml_sentiment import ml_sentiment          # HuggingFace sentiment pipeline
from ml_urlhinter import url_hinter            # URL type classifier
from ml_yf_nlp_news_engine import yfnews_reader  # Yahoo Finance news scraper
```

| Module | Class | Role |
|--------|-------|------|
| `ml_yf_nlp_news_engine` | `yfnews_reader` | 4-depth crawl pipeline for Yahoo Finance news |
| `ml_urlhinter` | `url_hinter` | Classifies article URLs by type and locality |
| `ml_sentiment` | `ml_sentiment` | HuggingFace Transformer sentiment pipeline (imported but not directly instantiated here) |

External standard library imports: `argparse`, `asyncio`, `datetime`, `logging`, `urllib.parse.urlparse`, `rich.print`.

---

## Class: `ml_nlpreader`

The single class defined in this file. Manages the lifecycle of a complete per-symbol news sentiment workflow.

### Class-Level Attributes

These are shared defaults across all instances. Instance methods overwrite them on `__init__`.

| Attribute | Type | Description |
|-----------|------|-------------|
| `args` | `list` / `dict` | Global CLI args dict passed in from the caller |
| `cycle` | `int` | Thread/loop iteration counter |
| `ml_yfn_dataset` | `yfnews_reader` or `None` | Active Yahoo Finance news reader instance |
| `yfn` | `yfnews_reader` or `None` | Alias for the YFN reader class |
| `yfn_uh` | `url_hinter` or `None` | URL Hinter instance shared with the reader |
| `yti` | `int` | Unique instance identifier (used in debug logging) |
| `caller` | any | Reference to the calling context (for tracing) |

---

### `__init__(self, yti, global_args, caller)`

Initializes the orchestrator. Does **not** instantiate any sub-engines at construction time — those are created lazily inside `nlp_read_one()`.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `yti` | `int` | Unique instance ID for logging traceability |
| `global_args` | `dict` | Full CLI args dict (contains flags like `bool_xray`, `bool_verbose`) |
| `caller` | any | Identifies the calling context in log output |

**Side effects:** Sets `self.args`, `self.yti`. Logs an instantiation message.

---

### `nlp_read_one(self, news_symbol, global_args)` — `async`

**The primary entry point for a full Depth 0 + Depth 1 news pipeline run for one stock symbol.**

This is an `async` method and must be called with `await` from an async context (or via `asyncio.run()`).

#### What it does

```
nlp_read_one("AAPL", args)
      │
      ├─ 1. Instantiate yfnews_reader(1, "AAPL", args)
      │
      ├─ 2. form_endpoint("AAPL")
      │        → sets yfqnews_url = "https://finance.yahoo.com/quote/AAPL/news/"
      │
      ├─ 3. Instantiate url_hinter(1, args)
      │        → shared into yfnews_reader via yfn_uh
      │
      ├─ 4. await yahoofin_news_depth0(0)        [Depth 0 — async crawl]
      │        → Crawl4ai scrapes the news feed page
      │        → Returns: _url_hash0  (SHA-256 hex of the feed URL)
      │
      ├─ 5. list_news_candidates_depth0(symbol, 0, 1, _url_hash0)
      │        → Reports the list of found article titles to terminal
      │        → Returns: articles_found (count)
      │
      ├─ 6. eval_news_feed_stories(symbol)       [Depth 1]
      │        → Evaluates each article: URL validity, deduplication, type classification
      │        → Populates: ml_yfn_dataset.ml_ingest{}
      │        → Returns: (eval_state, bad_url_count)
      │
      └─ 7. [Optional] dump_ml_ingest()
               → Only if args['bool_xray'] is True
               → Dumps the full ml_ingest{} dict to terminal for debugging
```

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `news_symbol` | `str` | Stock ticker symbol (e.g. `"NVDA"`, `"AAPL"`). Automatically uppercased. |
| `global_args` | `dict` | CLI args dict. Must include at minimum `bool_xray` key. |

#### Returns

`articles_found` — `int` — The total number of articles found at Depth 0 and passing Depth 1 evaluation.

Returns from the function before populating sentiment data. The caller must invoke Depth 3 separately on each article in `ml_yfn_dataset.ml_ingest`.

#### Key CLI Args Used

| Key | Type | Effect |
|-----|------|--------|
| `bool_xray` | `bool` | If `True`, dumps the full `ml_ingest{}` dataset to terminal after Depth 1 |

#### Internal Data State After Call

After `nlp_read_one()` completes, the following state is available on the `ml_nlpreader` instance:

| Attribute | Content |
|-----------|---------|
| `self.yfn` | Active `yfnews_reader` instance |
| `self.yfn_uh` | Active `url_hinter` instance |
| `self.ml_yfn_dataset` | Same as `self.yfn` — the populated news reader instance |
| `self.ml_yfn_dataset.ml_ingest` | Dict of all candidate articles, keyed by integer index |

#### `ml_ingest` Entry Structure

Each entry in `ml_ingest{}` has this shape (populated by `eval_news_feed_stories()`):

```python
{
    1: {
        "symbol":    "AAPL",
        "urlhash":   "sha256hex...",      # SHA-256 of the article URL
        "type":      0,                   # Article type code (see table below)
        "thint":     0.0,                 # Type confidence score
        "uhint":     0,                   # URL hint code
        "publisher": "Reuters",
        "title":     "Apple Reports...",
        "teaser":    "Apple Inc. today...",
        "url":       "https://finance.yahoo.com/news/..."
    },
    2: { ... },
    ...
}
```

---

### `nlp_summary_report(self, yti, ml_idx)` — `sync`

**Reads and processes one article from `ml_ingest{}` at Depth 2.**

This is a **synchronous** method that performs article-level routing logic — it decides what type each article is and calls `interpret_page_depth2()` to finalize article metadata. It does **not** perform LLM sentiment inference; that remains the caller's responsibility.

#### What it does

1. Validates `ml_ingest` has an entry at `ml_idx`
2. Reads article metadata from `ml_ingest[ml_idx]`
3. Based on `sn_row['type']`, routes to the appropriate handling branch
4. For processable types (0, 1), calls `self.ml_yfn_dataset.interpret_page_depth2(ml_idx, sn_row)`
5. Uses `url_hinter.uhinter()` and `url_hinter.confidence_lvl()` for URL classification output
6. Returns the `thint` value from the article

#### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `yti` | `int` | Instance ID for logging (updates `self.yti`) |
| `ml_idx` | `int` | Index into `ml_ingest{}` — the article to process |

#### Returns

`thint` — `float` — The type confidence score for the article. Value `9.9` is returned on error (no data).

---

## Article Type Classification System

The `nlp_summary_report()` method uses the `type` field from `ml_ingest` to decide how each article is handled. The type codes are assigned during Depth 1 (`eval_news_feed_stories()`) based on URL hint codes from `url_hinter`.

### Type Code → URL Hint → Behavior Mapping

| `type` (in `ml_ingest`) | Source `uhint` | `thint` | Description | Depth 2 Analysis | Sentiment Eligible |
|--------------------------|---------------|---------|-------------|-----------------|-------------------|
| `0` | `uhint == 0` | `0.0` | Real valid local Yahoo Finance article | Yes — `interpret_page_depth2()` called | **Yes** |
| `1` | `uhint == 1` | `1.0` | Fake local micro-stub (`/m/...` or `/live/...`) | Yes — `interpret_page_depth2()` called | No |
| `2` | `uhint == 2` | `4.0` | Video story (`/video/...`) | No | No |
| `3` | `uhint == 3` | `1.1` | External filler page (non-Yahoo URL) | No | No |
| `5` | `uhint == 5` | `6.0` | Yahoo Premium subscription upsell | No | No |
| `9` | — | — | Placeholder / not yet defined | No | No |
| other | — | — | Error / bad data | No | No |

### `thint` — Type Confidence Score

`thint` is a float that encodes the inferred article type from URL structure. It is used by `confidence_lvl()` in `url_hinter` to look up a human-readable description and locality code.

| `thint` | Description | Locality |
|---------|-------------|----------|
| `0.0` | Full local article page | Local |
| `1.0` | Fake local micro-stub | Local |
| `1.1` | External publication link | Remote |
| `4.0` | Video story page | Local |
| `6.0` | Premium subscription ad | Local |
| `9.9` | Unknown page structure | Unknown |

### `locality_code` Lookup

Used internally in `nlp_summary_report()` for debug output formatting:

```python
locality_code = {
    0: 'Local 0',
    1: 'Local 1',
    2: 'Local 2',
    3: 'Remote',
    9: 'Unknown locality'
}
```

---

## Data Flow Diagram

```
                      caller (aop.py / xop.py)
                              │
                  await nlp_read_one("AAPL", args)
                              │
                ┌─────────────▼──────────────┐
                │        ml_nlpreader         │
                │                             │
                │  1. yfnews_reader init      │
                │  2. form_endpoint()         │
                │  3. url_hinter init         │
                └──────────┬──────────────────┘
                           │
          ┌────────────────▼────────────────────┐
          │         yfnews_reader                │
          │                                      │
          │  Depth 0: yahoofin_news_depth0()     │  ← Crawl4ai async
          │    → yfn_jsdb[url_hash] = result     │
          │    → returns: url_hash               │
          │                                      │
          │  Depth 1: eval_news_feed_stories()   │  ← sync, loop over articles
          │    → ml_ingest{} populated           │
          │    → URL deduplication via set()     │
          │    → uhint/thint assigned per row    │
          │                                      │
          └───────────────┬──────────────────────┘
                          │
              returns articles_found
                          │
                   back to caller
                          │
             for each ml_idx in ml_ingest:
                          │
            ┌─────────────▼──────────────────┐
            │     nlp_summary_report()        │  ← sync, one article at a time
            │                                 │
            │  reads ml_ingest[ml_idx]        │
            │  routes by article type         │
            │  calls interpret_page_depth2()  │  ← Depth 2 (sync)
            │  returns thint                  │
            └─────────────────────────────────┘
                          │
             [caller then calls Depth 3]
             artdata_BS4_depth3() or
             artdata_C4_depth3()
                          │
             [caller then calls sentiment engine]
             ml_sentiment.compute_sentiment()
```

---

## Depth Pipeline Summary

| Depth | Method | Class | Async | Engine | Output |
|-------|--------|-------|-------|--------|--------|
| 0 | `yahoofin_news_depth0()` | `yfnews_reader` | **Yes** | Crawl4ai | `yfn_jsdb[url_hash]` cached result |
| 1 | `eval_news_feed_stories()` | `yfnews_reader` | No | Internal logic | `ml_ingest{}` populated |
| 2 | `interpret_page_depth2()` | `yfnews_reader` | No | Internal logic | `viable` flag set on article row |
| 3 | `artdata_BS4_depth3()` / `artdata_C4_depth3()` | `yfnews_reader` | No | BS4 / Crawl4ai | Article text corpus + LMDB cache write |

`ml_nlpreader` directly orchestrates Depths 0 and 1 (via `nlp_read_one()`) and Depth 2 (via `nlp_summary_report()`). Depth 3 is invoked by the caller.

---

## Integration with `url_hinter`

After `nlp_read_one()` runs, the `url_hinter` instance is stored at `self.yfn_uh` and also shared into `self.yfn.yfn_uh`. It is used in `nlp_summary_report()` to:

- **`uhinter(hcycle, url)`** — decode a URL and return `(uhint_code, description)`. The `hcycle` argument is a logging counter to trace which call triggered the hint.
- **`confidence_lvl(thint)`** — take a `thint` float and return a human-readable `(description, locality_code)` tuple.

---

## Integration with `yfnews_reader`

`yfnews_reader` (from `ml_yf_nlp_news_engine.py`) is instantiated inside `nlp_read_one()`:

```python
self.yfn = yfnews_reader(1, news_symbol, global_args)
```

Key methods called on the reader by `ml_nlpreader`:

| Method | When Called | Purpose |
|--------|-------------|---------|
| `form_endpoint(symbol)` | Before Depth 0 | Sets `yfqnews_url` to the Yahoo Finance news URL |
| `yahoofin_news_depth0(0)` | Depth 0 (async) | Crawl4ai skim of the top-level news feed |
| `list_news_candidates_depth0(symbol, 0, 1, hash)` | After Depth 0 | Prints found articles to terminal, returns count |
| `eval_news_feed_stories(symbol)` | Depth 1 | Filters, dedupes, and classifies each article |
| `interpret_page_depth2(ml_idx, sn_row)` | Depth 2 | Sets `viable` flag; returns `(uhint, thint, url)` |
| `dump_ml_ingest()` | Optional (xray) | Dumps the raw `ml_ingest{}` dict for debugging |

---

## Global Args Reference

The `global_args` dict is passed from the CLI and threaded through the full pipeline. Keys used within this module:

| Key | Type | Used In | Effect |
|-----|------|---------|--------|
| `bool_xray` | `bool` | `nlp_read_one()` | If `True`, calls `dump_ml_ingest()` after Depth 1 |

Additional keys consumed by the sub-engines (`yfnews_reader`, `ml_sentiment`):

| Key | Type | Used By |
|-----|------|---------|
| `bool_verbose` | `bool` | `ml_sentiment` — verbose per-chunk output |
| `bool_xray` | `bool` | `yfnews_reader` — dump session cookie details |

---

## Error Handling

| Scenario | Behavior |
|----------|----------|
| `yahoofin_news_depth0()` returns `None` | Logs error "No Top level articles found !!" and skips Depth 1 |
| `ml_ingest` missing entry at `ml_idx` | `nlp_summary_report()` logs error and returns `9.9` |
| `sn_row['type'] == 1` with bad URL | Catches exception, logs, returns `thint` |
| Unknown article type | Logs and returns `thint` from the article row |

---

## Example Usage (called from `aop.py`)

The orchestrator is not a standalone script. It is instantiated and called from the CLI entry points. Conceptual usage:

```python
import asyncio
from ml_yf_nlp_orchestrator import ml_nlpreader

# Global args populated by argparse in aop.py
global_args = {
    'bool_xray': False,
    'bool_verbose': True,
    ...
}

# Instantiate the orchestrator
nlp = ml_nlpreader(yti=1, global_args=global_args, caller="aop::newsai_sent")

# Run Depth 0 + Depth 1 for one symbol
articles_found = asyncio.run(nlp.nlp_read_one("NVDA", global_args))

print(f"Found {articles_found} candidate articles")

# Process each article at Depth 2, then trigger Depth 3 + sentiment from the caller
for ml_idx in nlp.ml_yfn_dataset.ml_ingest:
    thint = nlp.nlp_summary_report(1, ml_idx)
    
    # Caller then decides whether to run Depth 3:
    if thint == 0.0:   # Only full local articles are worth the network cost
        total_tokens, total_words, results = nlp.ml_yfn_dataset.artdata_C4_depth3(
            ml_idx, sentiment_ai, lmdb_inst
        )
```

---

## Logging

All methods use the `cmi_debug` pattern for structured log prefixes:

```
cmi_debug = __name__ + "::" + method_name + ".#" + str(yti)
logging.info(f'%s - message' % cmi_debug)
```

Log level is `INFO` globally (`logging.basicConfig(level=logging.INFO)`). Set to `DEBUG` for more granular output from sub-engines.

---

## File Structure Context

```
bespin/
├── ml_yf_nlp_orchestrator.py     ◄── THIS FILE
│     └── class ml_nlpreader
│
├── ml_yf_nlp_news_engine.py       # yfnews_reader — Depth 0/1/2/3 crawl engine
├── ml_urlhinter.py                # url_hinter — URL type classifier
├── ml_sentiment.py                # ml_sentiment — HuggingFace sentiment pipeline
├── ml_cvbow.py                    # ml_cvbow — sklearn Bag of Words vectorizer
├── datastore_eng_LMDB.py          # lmdb_io_eng — LMDB KV cache manager
│
├── json/
│   ├── YF_sym_main_schema.json    # Crawl4ai CSS schema for Depth 0 news feed skim
│   └── YF_sym_article_schema.json # Crawl4ai CSS schema for Depth 3 article body
│
└── aop.py / xop.py                # CLI entry points that instantiate ml_nlpreader
```

---

## Version History

| Version | Date | Author | Notes |
|---------|------|--------|-------|
| 1.0.0 | 2026-06-18 | Claude Sonnet 4.6 (Anthropic) | Initial README — full architecture and code documentation generated from codebase analysis |
