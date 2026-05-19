# Bespin

## Agentic AI Quant Stock Market Analysis Platform

Bespin is a Python-based agentic quantitative trading analysis platform that aggregates real-time market data from 13 financial APIs and 7 news sources, applies ML/NLP sentiment analysis via HuggingFace Transformers, and consolidates insights into a unified data pipeline for identifying actionable trading opportunities.

---

## What Bespin Does

Bespin is built for traders and quants who need a single platform to:

- **Screen the market in real time** — identify top gainers, top losers, small-cap movers, and stocks with unusual volume across Yahoo Finance and Nasdaq
- **Aggregate multi-source market data** — pull quotes, fundamentals, and historical data from 13 different financial data providers simultaneously
- **Scrape and analyze financial news** — extract full-text articles from 7 major financial news sites using async web crawling (Crawl4ai)
- **Apply AI-powered sentiment analysis** — run HuggingFace Transformer pipelines on news articles to produce article-level bullish/bearish sentiment scores
- **Compress and cache efficiently** — persist article text using Zstandard (ZSTD) compression into a dual-engine LMDB embedded key-value store
- **Consolidate into a single source of truth** — merge screener results, market data, and sentiment into a unified pandas DataFrame
- **Store relationships in a knowledge graph** — optionally persist stock, news, and sentiment data in a Neo4j Aura graph database

---

## Architecture Overview

```
External Market APIs (13 engines)  ──┐
External News Sites (7 sources)    ──┤──> Crawl4ai / BS4 Extractors
Yahoo Finance Screeners            ──┘
                                         │
                                         ▼
                              DataFrame Consolidation
                                         │
                                         ▼
                              Combo Logic / Wrangling
                              (Single Source of Truth)
                                         │
                        ┌────────────────┼────────────────┐
                        ▼                ▼                 ▼
                   Terminal          LMDB Cache       Neo4j Graph
                   Reports        (Dual KV Store)      Database
                                  C4 + BS4 engines
                                         │
                                         ▼
                              ML / NLP Sentiment Pipeline
                         ┌───────────────┼───────────────┐
                         ▼               ▼               ▼
                  HF Transformers     NLTK          Sklearn BoW
                  (sentiment)       (tokenize)     (vectorize)
                         │               │               │
                         └───────────────┼───────────────┘
                                         ▼
                               ZSTD-compressed Results
                                 (Final DataFrame)
```

---

## Project Structure

```
bespin/
├── aop.py                            # Main entry point — full-featured CLI orchestrator (~1,650 lines)
├── xop.py                            # Alternate entry point — streamlined CLI (~485 lines)
├── craw4ai_news.py                   # Crawl4ai news reader prototype (CSS schema-based)
├── requirements.txt                  # Python dependencies
│
├── data_engines_fundamentals/        # 13 market data API wrappers
│   ├── alpaca_md.py                  # Alpaca Markets (brokerage API, OHLCV bars)
│   ├── alphavantage_md.py            # Alpha Vantage (stocks, forex, crypto, gainers)
│   ├── eodhistoricaldata_md.py       # EOD Historical Data
│   ├── financialmodelingprep_md.py   # Financial Modeling Prep (financial statements)
│   ├── finnhub_md.py                 # Finnhub (real-time quotes, fundamentals, news)
│   ├── fred_md.py                    # Federal Reserve Economic Data (FRED)
│   ├── marketstack_md.py             # MarketStack (global EOD and intraday data)
│   ├── polygon_md.py                 # Polygon.io (stock quotes and market data)
│   ├── sec_md.py                     # SEC EDGAR (company filings and fundamentals)
│   ├── stockdata_md.py               # StockData.org (US quotes)
│   ├── stooq_md.py                   # Stooq (international historical data)
│   ├── tiingo_md.py                  # Tiingo (comprehensive data package)
│   └── twelvedata_md.py              # Twelve Data (multi-asset data)
│
├── data_engines_news/                # 7 async news site scrapers
│   ├── barrons_news.py               # Barron's
│   ├── benzinga_news.py              # Benzinga
│   ├── forbes_news.py                # Forbes
│   ├── fxstreet_news.py              # FX Street
│   ├── gurufocus_news.py             # GuruFocus
│   ├── hedgeweek_news.py             # HedgeWeek
│   └── investing_news.py             # Investing.com
│
├── ml_yf_nlp_news_engine.py          # Yahoo Finance news scraper — dual engine (Crawl4ai + BS4)
├── ml_yf_nlp_orchestrator.py         # Async NLP pipeline coordinator
├── ml_sentiment.py                   # HuggingFace sentiment analysis + ZSTD compression
├── ml_cvbow.py                       # Sklearn Count Vectorizer (Bag of Words)
├── ml_urlhinter.py                   # URL type classifier
│
├── yfnews_NEW/                       # Next-gen ML engine development (in progress)
│   ├── ml_yf_news_c4.py              # Updated Yahoo Finance Crawl4ai news reader
│   ├── ml_yf_nlp_reader_c4.py        # Updated NLP orchestrator (Crawl4ai-native)
│   ├── ml_sentiment.py               # Sentiment engine copy
│   ├── ml_cvbow.py                   # BoW engine copy
│   ├── ml_urlhinter.py               # URL hinter copy
│   └── test_crawl4ai_yahoo.py        # Crawl4ai integration tests
│
├── y_topgainers.py                   # Yahoo Finance top gainers screener
├── y_daylosers.py                    # Yahoo Finance top losers screener
├── y_smallcaps.py                    # Small-cap screener (>3% gain, Mid/Large/Mega cap)
├── y_techevents.py                   # Technical events and sentiment indicators
├── y_cookiemonster.py                # Playwright JS rendering + session management
├── y_stocknews.py                    # Stock news aggregation (legacy reader preserved)
├── y_newsloop.py                     # News filter and loop processor (y_newsfilter class)
│
├── nasdaq_uvoljs.py                  # Nasdaq unusual volume detection (JSON API)
├── nasdaq_quotes.py                  # Nasdaq quote extraction
├── nasdaq_wrangler.py                # Data cleaning and null-value handling
│
├── bigcharts_md.py                   # BigCharts quote extraction (bc_quote class)
├── marketwatch_md.py                 # MarketWatch quote extraction (mw_quote class)
├── shallow_logic.py                  # Combo merge logic (SSoT DataFrame builder)
│
├── neo4j_graphdb.py                  # Neo4j Aura knowledge graph client (neo4j_auradb class)
├── datastore_eng_LMDB.py             # LMDB embedded KV store manager (dual C4/BS4 engines)
├── dump_db.py                        # LMDB database inspection utility
│
├── json/                             # Crawl4ai CSS/XPath extraction schemas
│   ├── BARRONS_crawl4ai_schema.json
│   ├── BENZINGA_crawl4ai_schema.json
│   ├── FORBES_crawl4ai_schema.json
│   ├── FXSTREET_crawl4ai_schema.json
│   ├── GURUFOCUS_crawl4ai_schema.json
│   ├── HEDGEWEEK_crawl4ai_schema.json
│   ├── INVESTING_crawl4ai_schema.json
│   ├── YF_sym_main_schema.json       # Yahoo Finance news feed (Depth 0 skim)
│   ├── YF_sym_article_schema.json    # Yahoo Finance article body (Depth 3)
│   └── YAHOO_FINANCE_crawl4ai_schema.json
│
├── diagrams/                         # Mermaid flow diagrams
│   ├── ml_sentiment_flow.mermaid
│   ├── ml_yf_news_c4_flow.mermaid
│   └── ml_yf_nlp_reader_c4_flow.mermaid
│
├── nltk_data/                        # Pre-downloaded NLTK corpora (offline NLP)
├── docs/                             # API docs, specs, research notes
└── ref_code/                         # Reference implementations
```

---

## Key Modules

### `aop.py` — Agentic Aperture Engine (Main Orchestrator)

The central command-line driver. Parses 25+ CLI arguments and coordinates all modules.

**Primary workflows:**

| Flag | Action |
|------|--------|
| `-t` / `--tops` | Extract top gainers and losers from Yahoo Finance |
| `-s` / `--screen` | Small-cap screener (profitability + volume logic) |
| `-u` / `--unusual` | Nasdaq unusual volume detection |
| `-d` / `--deep` | Full merged analysis (combo DataFrame) |
| `-a` / `--allnews` | ML/NLP sentiment for all stocks in the combo list |
| `--news-cycle` | Scrape all 7 financial news sites |
| `-n SYM N` / `--newsai-sent` | AI sentiment analysis for symbol `SYM`, `N` articles |
| `-p` / `--perf` | Technical event performance sentiment |
| `-q SYM` / `--quote` | Single-symbol quote lookup |
| `-v` / `--verbose` | Enable verbose logging |
| `-x` / `--xray` | Dump detailed debug data structures |
| `--alpaca SYM` | Alpaca live quote for symbol |
| `--alpaca-bars SYM` | Alpaca OHLCV bars for symbol |
| `--fred` | Pull Federal Reserve economic data |
| `--finnhub SYM` | Finnhub real-time quote and fundamentals |
| `--finnhub-news SYM` | Finnhub financial news for symbol |
| `--alphavantage SYM` | Alpha Vantage quote and data |
| `--alphavantage-overview SYM` | Alpha Vantage company overview |
| `--alphavantage-intraday SYM` | Alpha Vantage intraday data |
| `--alphavantage-gainers` | Alpha Vantage top gainers/losers |
| `--alphavantage-news SYM` | Alpha Vantage market news |
| `--polygon SYM` | Polygon.io quote |
| `--tiingo SYM` | Tiingo comprehensive data |
| `--tiingo-news` | Tiingo financial news |
| `--sec SYM` | SEC EDGAR filings for symbol |
| `--marketstack SYM` | MarketStack EOD and intraday |
| `--stockdata SYM` | StockData.org quote |
| `--twelvedata SYM` | Twelve Data comprehensive data |
| `--eodhistoricaldata SYM` | EOD Historical Data |
| `--financialmodelingprep SYM` | FinancialModelingPrep data |
| `--stooq SYM` | Stooq historical data |

### `xop.py` — Streamlined Entry Point

A leaner alternative to `aop.py` with a focused CLI for core workflows: news sentiment, screeners, unusual volume, quotes, and technical events. Shares the same module imports and class hierarchy.

### `ml_yf_nlp_news_engine.py` — Yahoo Finance News Scraper (Production)

The production-grade Yahoo Finance news reader. Uses a **4-depth crawl pipeline** with dual extraction engines.

**Depth pipeline:**

| Depth | Operation | Engine |
|-------|-----------|--------|
| 0 | Top-level news feed skim — extract article list and URLs | Crawl4ai (async) |
| 1 | Evaluate news feed stories — filter and classify candidates | Internal logic |
| 2 | Interpret article page structure — set URL hints and type codes | Internal logic |
| 3 | Full article text extraction and sentiment scoring | BS4 **or** Crawl4ai |

**Dual extraction engines at Depth 3:**
- `artdata_BS4_depth3()` — BeautifulSoup4 article parser (HTML session-based)
- `artdata_C4_depth3()` — Crawl4ai async article extractor (CSS schema-based)

Both engines store results in separate LMDB caches (`C4_lmdb_env` / `BS4_lmdb_env`).

### `ml_yf_nlp_orchestrator.py` — NLP Pipeline Coordinator

Async orchestrator that chains news reading → article extraction → sentiment analysis for a given stock symbol.

**Flow:** `nlp_read_one(symbol)` → Depth-0 skim → Depth-1 evaluation → Depth-2 interpretation → Depth-3 extraction → sentiment scoring → aggregated DataFrame

### `ml_sentiment.py` — Sentiment Analysis Engine

- Loads a pre-trained HuggingFace Transformers sentiment classification pipeline (singleton)
- Tokenizes article text with NLTK sentence tokenizer
- Processes text in chunks respecting the model's max token length
- Aggregates sentence-level scores to article-level sentiment
- **Compresses article text into ZSTD binary blobs** before writing to LMDB
- Output scale: **-225 (strongly bearish)** to **+225 (strongly bullish)**

### `shallow_logic.py` — Combo Merge Logic

Merges top gainers + small caps + unusual volume datasets into a single ranked "Single Source of Truth" DataFrame. Key operations:

| Method | Purpose |
|--------|---------|
| `prepare_combo_df()` | Build and sort the merged DataFrame |
| `polish_combo_df()` | Enrich missing market-cap and quote data |
| `tag_dupes()` | Identify duplicate symbols across datasets |
| `find_hottest()` | Identify outlier stocks by price and % gain |
| `tag_uniques()` | Flag symbols appearing in only one dataset |
| `tag_naans()` | Handle null/NaN values |
| `rank_hot()` / `rank_unvol()` / `rank_caps()` | Multi-dimension ranking |
| `combo_listall()` / `combo_listall_ranked()` | Terminal report output |

### `datastore_eng_LMDB.py` — LMDB Cache Layer

Dual-engine embedded key-value store for caching scraped articles and sentiment results.

- **Two separate caches:** `C4_lmdb_env` (Crawl4ai results) and `BS4_lmdb_env` (BeautifulSoup4 results)
- Operates in read-only (RO) or read-write (RW) mode
- `kv_cache_engine()` handles both read-from-cache (rehydration) and write-to-cache logic
- Supports Zstandard (ZSTD)-compressed binary blobs via msgpack packing

### `neo4j_graphdb.py` — Neo4j Knowledge Graph

Connects to a Neo4j Aura cloud instance via the `neo4j_auradb` class. Stores stock symbols, news articles, and sentiment relationships as graph nodes and edges for deeper cross-asset analysis.

Key operations: `create_sym_node()`, `create_article_nodes()`, `create_sym_art_rels()`, `check_node_exists()`, `dump_symbols()`

---

## ML / NLP Pipeline Detail

```
Stock Symbol (e.g. AAPL)
        │
        ▼
 Yahoo Finance news page
 Crawl4ai async scrape
        │
        ▼
  Article list + URLs                  ← Depth 0
  (top-level feed skim)
        │
        ▼
  Candidate evaluation                 ← Depth 1
  (filter, classify, score)
        │
        ▼
  Article page interpretation          ← Depth 2
  (URL hints, type codes, routing)
        │
        ├────────────────────────┐
        ▼                        ▼
  BS4 article extractor    Crawl4ai article extractor   ← Depth 3
  (HTML session)           (CSS schema, async)
        └──────────┬─────────────┘
                   ▼
  NLTK tokenization
  (sentence splitting)
                   │
                   ▼
  HuggingFace Transformers
  sentiment pipeline
  (per-chunk inference)
                   │
                   ▼
  ZSTD compression
  (binary blob → LMDB)
                   │
                   ▼
  Score aggregation
  {positive, negative, neutral counts}
                   │
                   ▼
  Article-level sentiment DataFrame
  (stored in LMDB + printed to terminal)
```

**URL classification** (`ml_urlhinter.py`) routes articles before Depth 3 processing:

| Code | Type | Description |
|------|------|-------------|
| 0 | Full local article | `finance.yahoo.com/news/...` or `markets/`, `sectors/`, etc. |
| 1 | Micro stub | `finance.yahoo.com/m/...` or `/live/...` |
| 2 | Video content | `finance.yahoo.com/video/...` |
| 3 | External filler page | Absolute URL to an external publication |
| 4 | Research/analyst report | `finance.yahoo.com/research/...` |
| 5 | Premium / paywalled | `finance.yahoo.com/about/...` |
| 9 | Not yet defined | Unrecognized path segment |
| 10 | Mangled URL | Parse error on URL structure |
| 99 | Unknown state | Default fallback / error |

**Confidence levels** (`confidence_lvl()`) further classify article locality:

| Code | Description |
|------|-------------|
| 0.0 | Full local article page |
| 1.0 | Fake local micro-stub |
| 1.1 | External publication link |
| 2.0 / 2.1 | Op-Ed page / stub |
| 3.0 / 3.1 | Curated report page / stub |
| 4.0 / 4.1 | Video story page / stub |
| 5.0 / 5.1 | Micro-ad insert |
| 6.0 / 6.1 | Premium subscription / bulk ad junk |
| 7.0 / 7.1 | Research report page / stub |
| 9.9 | Unknown page structure |

---

## Data Sources

### Market Data APIs (13 engines)

| Provider | Data Type | Free Tier |
|----------|-----------|-----------|
| Finnhub | Real-time quotes, fundamentals, news | 60 calls/min |
| Alpha Vantage | Stocks, forex, crypto, gainers/losers, intraday | 5 calls/min |
| Alpaca | OHLCV bars (brokerage API), live quotes | Yes |
| Polygon.io | Quotes, market data | Limited |
| Twelve Data | Multi-asset (stocks, ETFs, forex, crypto) | 8 calls/min |
| FRED | Federal Reserve economic indicators | Unlimited |
| SEC EDGAR | Company filings and fundamentals | Unlimited |
| Tiingo | Comprehensive price + news | Limited |
| MarketStack | Global EOD and intraday | 100/month |
| StockData.org | US quotes | 100/day |
| FinancialModelingPrep | Financial statements | Limited |
| EOD Historical Data | Historical OHLCV | Limited |
| Stooq | International historical data | Unlimited |

### News Sources (7 scrapers)

Barron's, Benzinga, Forbes, FX Street, GuruFocus, HedgeWeek, Investing.com

All news scrapers use **Crawl4ai** with per-site JSON extraction schemas (CSS/XPath selectors, no LLM required).

### Additional Quote Sources

| Module | Class | Source |
|--------|-------|--------|
| `bigcharts_md.py` | `bc_quote` | BigCharts / MarketWatch (basic + quick quotes) |
| `marketwatch_md.py` | `mw_quote` | MarketWatch (full quote data) |

---

## Technologies

| Category | Library / Tool |
|----------|---------------|
| **Data processing** | pandas, numpy |
| **Web scraping** | Crawl4ai, BeautifulSoup4, requests, requests-html, Playwright |
| **ML / NLP** | HuggingFace Transformers, PyTorch, NLTK, scikit-learn |
| **Database** | LMDB (dual KV store), Neo4j (graph DB) |
| **Compression** | Zstandard (ZSTD), msgpack |
| **Async** | asyncio |
| **Terminal UI** | Rich |
| **Configuration** | python-dotenv (.env), argparse |
| **Visualization** | matplotlib, seaborn, plotly |
| **Runtime** | Python 3.10+ |

---

## Installation

```bash
git clone https://github.com/orville-wright/bespin.git
cd bespin
pip install -r requirements.txt
playwright install   # required for JavaScript-rendered pages
```

---

## Configuration

Create a `.env` file in the project root with your API credentials:

```env
# Market data APIs
FINNHUB_API_KEY=your_key
APCA_API_KEY_ID=your_alpaca_key
APCA_API_SECRET_KEY=your_alpaca_secret
ALPACA_DATA_FEED=iex
FRED_API_KEY=your_key
POLYGON_API_KEY=your_key
TIINGO_API_TOKEN=your_token
MARKETSTACK_API_KEY=your_key
STOCKDATA_API_TOKEN=your_token
TWELVEDATA_API_KEY=your_key
ALPHAVANTAGE_API_KEY=your_key
EODHD_API_KEY=your_token
FINANCIALMODELINGPREP_API_KEY=your_key

# Neo4j knowledge graph (optional)
NEO4J_URI=neo4j+s://your-instance.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password
NEO4J_DATABASE=neo4j
```

See [docs/api_key_requirements.md](docs/api_key_requirements.md) for details on obtaining free-tier API keys.

---

## Usage Examples

```bash
# Top gainers and losers from Yahoo Finance
python aop.py --tops

# Small-cap screener
python aop.py --screen

# Unusual volume on Nasdaq
python aop.py --unusual

# Full merged analysis (gainers + small caps + unusual volume)
python aop.py --deep

# AI sentiment analysis: top 5 articles for NVDA
python aop.py --newsai-sent NVDA 5

# Get real-time quote from Finnhub
python aop.py --finnhub AAPL

# Get Finnhub news for a symbol
python aop.py --finnhub-news TSLA

# Scrape all 7 news sites
python aop.py --news-cycle

# Federal Reserve economic data
python aop.py --fred

# Alpha Vantage company overview
python aop.py --alphavantage-overview MSFT

# Alpha Vantage top gainers/losers
python aop.py --alphavantage-gainers

# Tiingo comprehensive data
python aop.py --tiingo AAPL

# EOD Historical Data
python aop.py --eodhistoricaldata GOOG

# SEC EDGAR filings
python aop.py --sec AAPL

# Streamlined entry point (core workflows only)
python xop.py --tops
python xop.py --newsai-sent NVDA 5
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.

---

## Version History

| Version | Date | Time (UTC) | Author | Notes |
|---------|------|------------|--------|-------|
| 1.0.0 | 2026-04-25 | 04:30 UTC | Claude Sonnet 4.6 (Anthropic) | Initial full README — comprehensive project documentation generated by automated codebase analysis |
| 1.1.0 | 2026-05-16 | 02:00 UTC | Claude Sonnet 4.6 (Anthropic) | May update — corrected file names, added xop.py, craw4ai_news.py, yfnews_NEW/, diagrams/, 4-depth NLP pipeline, dual LMDB engines, ZSTD compression, expanded URL classifier, full CLI flag table, updated env vars |
