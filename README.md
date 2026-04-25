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
- **Consolidate into a single source of truth** — merge screener results, market data, and sentiment into a unified pandas DataFrame
- **Store and cache efficiently** — persist results in an LMDB embedded key-value store and optionally in a Neo4j knowledge graph

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
                   Reports           (KV Store)        Database
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
                                Aggregated Results
                                 (Final DataFrame)
```

---

## Project Structure

```
bespin/
├── aop.py                            # Main entry point — CLI orchestrator (~4,300 lines)
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
├── ml_yf_nlp_news_engine.py          # Yahoo Finance news scraper (Crawl4ai + BS4)
├── ml_yf_nlp_orchestrator.py         # Async NLP pipeline coordinator
├── ml_sentiment.py                   # HuggingFace sentiment analysis engine
├── ml_cvbow.py                       # Sklearn Count Vectorizer (Bag of Words)
├── ml_urlhinter.py                   # URL type classifier (12 article types)
│
├── y_topgainers.py                   # Yahoo Finance top gainers screener
├── y_daylosers.py                    # Yahoo Finance top losers screener
├── y_smallcaps.py                    # Small-cap screener (>3% gain, >$299M cap)
├── y_techevents.py                   # Technical events and sentiment indicators
├── y_cookiemonster.py                # Playwright JS rendering + session management
├── y_stocknews.py                    # Stock news aggregation
├── y_newsloop.py                     # News loop processor
│
├── nasdaq_uvoljs.py                  # Nasdaq unusual volume detection (JSON API)
├── nasdaq_quotes.py                  # Nasdaq quote extraction
├── nasdaq_wrangler.py                # Data cleaning and null-value handling
│
├── bigcharts_md.py                   # BigCharts/MarketWatch quote extraction
├── marketwatch_md.py                 # MarketWatch data extraction
├── shallow_logic.py                  # Combo merge logic (SSoT DataFrame builder)
│
├── db_graph.py                       # Neo4j Aura knowledge graph client
├── datastore_eng_LMDB.py             # LMDB embedded KV store manager
├── dump_db.py                        # LMDB database inspection utility
│
├── json/                             # Crawl4ai CSS/XPath extraction schemas
│   ├── BENZINGA_crawl4ai_schema.json
│   ├── BARRONS_crawl4ai_schema.json
│   ├── FORBES_crawl4ai_schema.json
│   └── ... (10 schema files total)
│
├── nltk_data/                        # Pre-downloaded NLTK corpora (offline NLP)
├── docs/                             # API docs, specs, research notes
└── ref_code/                         # Reference implementations
```

---

## Key Modules

### `aop.py` — Agentic Aperture Engine (Main Orchestrator)

The central command-line driver. Parses 20+ CLI arguments and coordinates all modules.

**Primary workflows:**

| Flag | Action |
|------|--------|
| `-t` / `--tops` | Extract top gainers and losers from Yahoo Finance |
| `-s` / `--screen` | Small-cap screener (profitability + volume logic) |
| `-u` / `--unusual` | Nasdaq unusual volume detection |
| `-d` / `--deep` | Full merged analysis (combo DataFrame) |
| `--news-cycle` | Scrape all 7 financial news sites |
| `-n SYM N` / `--newsai-sent` | AI sentiment analysis for symbol `SYM`, `N` articles |
| `-q SYM` / `--quote` | Single-symbol quote lookup |
| `--fred` | Pull Federal Reserve economic data |
| `--finnhub SYM` | Finnhub real-time quote and fundamentals |
| `--alphavantage SYM` | Alpha Vantage data |

### `ml_yf_nlp_orchestrator.py` — NLP Pipeline Coordinator

Async orchestrator that chains news reading → article extraction → sentiment analysis for a given stock symbol.

**Flow:** `nlp_read_one(symbol)` → depth-0 article list scrape → depth-3 full article parse → sentiment scoring → aggregated DataFrame

### `ml_sentiment.py` — Sentiment Analysis Engine

- Loads a pre-trained HuggingFace Transformers sentiment classification pipeline
- Tokenizes article text with NLTK
- Processes each sentence independently through the model
- Aggregates sentence-level scores to article-level sentiment
- Output scale: **-225 (strongly bearish)** to **+225 (strongly bullish)**

### `shallow_logic.py` — Combo Merge Logic

Merges top gainers + small caps + unusual volume datasets into a single ranked "Single Source of Truth" DataFrame. Handles deduplication, missing market-cap imputation, and outlier identification (hottest stocks by price and % gain).

### `datastore_eng_LMDB.py` — LMDB Cache Layer

Embedded key-value store for caching scraped articles and sentiment results. Operates in read-only (RO) or read-write (RW) mode. Default max size: 1 GB.

### `db_graph.py` — Neo4j Knowledge Graph

Connects to a Neo4j Aura cloud instance to store stock, news, and sentiment as a relationship graph for deeper cross-asset analysis.

---

## ML / NLP Pipeline Detail

```
Stock Symbol (e.g. AAPL)
        │
        ▼
 Yahoo Finance news page
 (Crawl4ai async scrape)
        │
        ▼
  Article list (Depth 0)
        │
        ▼
  Full article text (Depth 3)
  [Crawl4ai or BeautifulSoup4]
        │
        ▼
  NLTK tokenization
  (sentence splitting)
        │
        ▼
  HuggingFace Transformers
  sentiment pipeline
  (per-sentence inference)
        │
        ▼
  Score aggregation
  {positive, negative, neutral counts}
        │
        ▼
  Article-level sentiment DataFrame
  (stored in LMDB + printed to terminal)
```

**URL classification** (`ml_urlhinter.py`) filters articles before processing:

| Type | Description |
|------|-------------|
| 0 | Full local article (finance.yahoo.com/news/...) |
| 1 | Micro stub |
| 2 | Video content |
| 3 | External filler page |
| 4 | Research/analyst report |
| 5 | Premium / paywalled content |

---

## Data Sources

### Market Data APIs (13 engines)

| Provider | Data Type | Free Tier |
|----------|-----------|-----------|
| Finnhub | Real-time quotes, fundamentals, news | 60 calls/min |
| Alpha Vantage | Stocks, forex, crypto, gainers/losers | 5 calls/min |
| Alpaca | OHLCV bars (brokerage API) | Yes |
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

---

## Technologies

| Category | Library / Tool |
|----------|---------------|
| **Data processing** | pandas, numpy |
| **Web scraping** | Crawl4ai, BeautifulSoup4, requests, Playwright |
| **ML / NLP** | HuggingFace Transformers, PyTorch, NLTK, scikit-learn |
| **Database** | LMDB (KV store), Neo4j (graph DB) |
| **Async** | asyncio |
| **Terminal UI** | Rich |
| **Configuration** | python-dotenv (.env), argparse |
| **Visualization** | matplotlib, seaborn, plotly |
| **Runtime** | Python 3.10+ |

---

## Installation

```bash
git clone https://github.com/<your-org>/bespin.git
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
ALPACA_API-KEY=your_key
ALPACA_SEC-KEY=your_secret
FRED_API_KEY=your_key
POLYGON_API_KEY=your_key
TIINGO_API_TOKEN=your_token
MARKETSTACK_API_KEY=your_key
STOCKDATA_API_TOKEN=your_token
TWELVEDATA_API_KEY=your_key
EOD_HISTORICAL_DATA_API_KEY=your_key

# Neo4j knowledge graph (optional)
NEO4J_URI=neo4j+s://your-instance.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password
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

# Scrape all 7 news sites
python aop.py --news-cycle

# Federal Reserve economic data
python aop.py --fred
```

---

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.

---

## Version History

| Version | Date | Time (UTC) | Author | Notes |
|---------|------|------------|--------|-------|
| 1.0.0 | 2026-04-25 | 04:30 UTC | Claude Sonnet 4.6 (Anthropic) | Initial full README — comprehensive project documentation generated by automated codebase analysis |
