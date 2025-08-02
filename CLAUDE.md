# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Bespin is an agentic quantitative finance project focused on multi-source financial data extraction, analysis, and ML/NLP-powered sentiment analysis. The codebase is a comprehensive Python-based toolkit that aggregates financial data from multiple sources and performs advanced machine learning analysis on financial news and market data.

## Architecture

### Main Entry Point
- `aop.py`: Primary orchestrator script (1,632 lines) that coordinates all data extraction and analysis modules
- Supports 30+ command-line arguments for different data collection workflows
- Handles threading for background data collection with 10-second update cycles

### Data Source Architecture

**Yahoo Finance Modules (`y_` prefix):**
- `y_topgainers.py`: Top gaining stocks extraction via JavaScript data scraping
- `y_daylosers.py`: Day losing stocks data extraction
- `y_smallcaps.py`: Small cap stock screening (market cap > $299M, +5% gains)
- `y_cookiemonster.py`: Yahoo session/cookie management and anti-bot evasion
- `y_generalnews.py`: General financial news extraction
- `y_stocknews.py`: Stock-specific news analysis
- `y_techevents.py`: Technical event sentiment analysis (Bull/Bear/Neutral indicators)

**NASDAQ Data Modules (`nasdaq_` prefix):**
- `nasdaq_quotes.py`: Live market data quotes via native JSON API (5-min delayed)
- `nasdaq_uvoljs.py`: Unusual volume analysis (up/down volume detection)
- `nasdaq_wrangler.py`: NASDAQ data processing and refactoring layer

**Financial Data Engines (`data_engines_fundamentals/`):**
- `alpaca_md.py`: Alpaca trading API integration (real-time quotes, OHLCV bars)
- `polygon_md.py`: Polygon.io market data with company profiles
- `tiingo_md.py`: Tiingo comprehensive data (quotes, fundamentals, news)
- `sec_md.py`: SEC EDGAR filings integration
- `fred_md.py`: Federal Reserve economic data (yield curves, indicators)
- `alphavantage_md.py`: Alpha Vantage quotes, company overviews, intraday data
- `finnhub_md.py`: Finnhub real-time quotes and company profiles
- `marketstack_md.py`: Marketstack EOD and historical data
- `stockdata_md.py`: StockData.org quotes and EOD data
- `twelvedata_md.py`: Twelve Data comprehensive market data
- `eodhistoricaldata_md.py`: EOD Historical Data API integration
- `financialmodelingprep_md.py`: FinancialModelingPrep data and company profiles
- `stooq_md.py`: Stooq historical market data

**News Data Engines (`data_engines_news/`):**
- `barrons_news.py`: Barron's financial news via Crawl4AI
- `benzinga_news.py`: Benzinga news extraction
- `forbes_news.py`: Forbes financial news
- `fxstreet_news.py`: FXStreet news analysis
- `investing_news.py`: Investing.com news extraction
- `hedgeweek_news.py`: Hedge fund industry news
- `gurufocus_news.py`: GuruFocus investment news

### Advanced ML/NLP Pipeline (`ml_` prefix)
- `ml_sentiment.py`: Core sentiment analysis engine with technical scoring (-200 to +200 scale)
- `ml_cvbow.py`: Count Vectorizer and Bag of Words implementation
- `ml_yf_nlp_reader_c4.py`: Yahoo Finance NLP reader using Crawl4AI
- `ml_yf_news_c4.py`: Yahoo Finance news processing with Crawl4AI
- `ml_urlhinter.py`: URL content analysis and classification

**Enhanced NLP Features:**
- Dual scraping engines: Crawl4AI and BeautifulSoup4 with load balancing
- Advanced sentiment classification using transformers and NLTK
- URL hash-based article deduplication
- Multi-threaded article processing with performance metrics
- Human vs AI reading time comparisons

### Data Infrastructure
- `db_graph.py`: Neo4j graph database operations with environment-based authentication
- `shallow_logic.py`: Combined logic and data processing utilities
- `bigcharts_md.py`: BigCharts/MarketWatch data extraction (15-min delayed quotes)
- `marketwatch_md.py`: MarketWatch data integration

## Development Commands

### Running the Main Application
```bash
# Basic usage - run from project root
python3 aop.py [options]

# Market Data Workflows:
python3 aop.py -t                              # Show top gainers/losers from Yahoo Finance
python3 aop.py -s                              # Small cap screener (>$299M market cap, +5% gains)
python3 aop.py -u                              # Unusual volume analysis (up/down volume)
python3 aop.py -d -s -u                        # Deep analysis combining all data sources
python3 aop.py --news-cycle                    # Full news cycle extraction from all engines

# Quote and Price Data:
python3 aop.py -q SYMBOL                       # Multi-source quote (NASDAQ, BigCharts)
python3 aop.py --alpaca SYMBOL                 # Alpaca real-time quote
python3 aop.py --alpaca-bars SYMBOL            # Alpaca OHLCV bars (1-minute, last 20)
python3 aop.py --polygon SYMBOL                # Polygon.io comprehensive data
python3 aop.py --tiingo SYMBOL                 # Tiingo quote and fundamentals
python3 aop.py --alphavantage SYMBOL           # Alpha Vantage global quote
python3 aop.py --finnhub SYMBOL                # Finnhub real-time quote and profile

# Advanced Data Sources:
python3 aop.py --sec SYMBOL                    # SEC EDGAR filings search
python3 aop.py --fred                          # Federal Reserve economic snapshot
python3 aop.py --tiingo-news                   # Tiingo financial news feed
python3 aop.py --alphavantage-gainers          # Alpha Vantage top gainers/losers
python3 aop.py --alphavantage-news SYMBOL      # Alpha Vantage market news

# ML/NLP Sentiment Analysis:
python3 aop.py -n SYMBOL COUNT                 # AI sentiment analysis for one stock (limit articles)
python3 aop.py -a                              # AI sentiment analysis for all discovered stocks
python3 aop.py -p                              # Technical event performance sentiment

# Debug and Analysis:
python3 aop.py -v                              # Verbose logging output
python3 aop.py -x                              # Dump detailed debug data structures
```

### Environment Setup
- Requires Python 3.x with virtual environment
- Neo4j database connection requires `.env` file with NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD
- Multiple API integrations require environment variables for API keys:
  - `ALPACA_API_KEY`, `ALPACA_SECRET_KEY` (Alpaca trading)
  - `POLYGON_API_KEY` (Polygon.io market data)
  - `TIINGO_API_KEY` (Tiingo financial data)
  - `ALPHA_VANTAGE_API_KEY` (Alpha Vantage data)
  - `FINNHUB_API_KEY` (Finnhub market data)
  - `FRED_API_KEY` (Federal Reserve economic data)
  - Additional keys for 10+ other financial data providers

### Installation
```bash
# Install dependencies
pip install -r requirements.txt

# Install NLTK data (for sentiment analysis)
python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords')"
```

## Key Dependencies
- **Web Scraping**: requests_html, beautifulsoup4, requests, crawl4ai, pyppeteer
- **Data Processing**: pandas (2.2.3), numpy (2.2.6), matplotlib, seaborn
- **ML/NLP**: scikit-learn (1.6.1), nltk, transformers, torch (2.7.0+cpu)
- **Financial APIs**: 13+ dedicated API clients for different data providers
- **Database**: neo4j (graph database for knowledge storage)
- **UI/Output**: rich (formatted console output), plotly (data visualization)
- **Development**: jupyter, ipython (notebook support)
- **Async Processing**: asyncio, threading (multi-threaded data collection)

## Data Flow Architecture

### 1. Data Extraction Layer
- **13 Financial Data Engines**: Located in `data_engines_fundamentals/` 
- **7 News Extraction Engines**: Located in `data_engines_news/`
- **Yahoo Finance Scrapers**: JavaScript-based extraction with anti-bot evasion
- **NASDAQ API Integration**: Native JSON API access with session management
- **Multi-source Quote Collection**: 3+ methods per symbol (NASDAQ, Alpaca, BigCharts)

### 2. Data Processing Pipeline
- **Raw Data Ingestion**: Multiple formats (JSON, HTML, CSV, API responses)
- **DataFrame Transformation**: Standardization into pandas DataFrames
- **Data Validation & Cleaning**: Asset class detection, data type casting
- **Deduplication**: URL hash-based article deduplication across news sources

### 3. Advanced ML/NLP Analysis
- **Dual Scraping Engines**: Crawl4AI and BeautifulSoup4 with load balancing
- **Sentiment Analysis**: Transformers-based classification with -200 to +200 scoring
- **Technical Indicators**: Bull/Bear/Neutral sentiment mapping
- **Performance Metrics**: Human vs AI reading time comparisons
- **Multi-threaded Processing**: Concurrent article analysis with performance tracking

### 4. Knowledge Graph Storage
- **Neo4j Integration**: Graph database for relationship mapping
- **Entity Creation**: Stock symbols, articles, sentiment data as nodes
- **Relationship Mapping**: Symbol-to-article relationships with metadata
- **Historical Tracking**: Time-series sentiment evolution

### 5. Output & Visualization
- **Rich Console Formatting**: Colored, structured terminal output
- **Recommendation Engine**: "Single Source of Truth" combining all data sources
- **Statistical Analysis**: Aggregated sentiment metrics and market insights
- **Debug & Verbose Modes**: Detailed data structure dumps and logging

## Advanced Features

### Multi-source Data Fusion
- **Combo Logic**: `shallow_logic.py` combines data from multiple sources
- **Anomaly Detection**: Identifies stocks appearing in multiple categories (gainers + unusual volume)
- **Recommendation System**: Lowest price, highest gain, hottest stock identification
- **Market Overview**: Price/percent averages across all monitored stocks

### Anti-Bot Evasion & Load Balancing
- **Cookie Management**: `y_cookiemonster.py` handles session management
- **Request Randomization**: Load balancing between scraping engines
- **Rate Limiting**: Intelligent delays to avoid detection
- **User Agent Rotation**: Dynamic header management

### Performance Optimization
- **Threading Architecture**: Background workers for continuous data collection
- **Caching**: URL hash-based caching and deduplication
- **Chunked Processing**: Sentence/paragraph-level NLP analysis
- **Resource Management**: Memory-efficient DataFrame operations

## Module Dependencies
- **Core Orchestrator**: `aop.py` coordinates all modules (1,632 lines)
- **ML Pipeline**: `ml_sentiment.py` → `ml_cvbow.py` → `ml_urlhinter.py`
- **Data Wrangling**: `nasdaq_wrangler.py` handles quote data processing
- **Session Management**: `y_cookiemonster.py` provides anti-bot capabilities
- **Database Layer**: `db_graph.py` centralizes Neo4j operations
- **Utility Layer**: `shallow_logic.py` provides data fusion and analysis

## Performance Characteristics
- **Real-time Processing**: Sub-second quote retrieval from multiple sources
- **Scalable Analysis**: Handles 100+ news articles per sentiment analysis cycle
- **Memory Efficient**: Streaming data processing with DataFrame optimization
- **API Rate Management**: Intelligent throttling across 13+ data providers

## Important Notes

### Security & Best Practices
- Financial data APIs have rate limits and require proper key management
- Web scrapers are brittle and may break when target sites update anti-bot measures
- Neo4j database requires proper authentication and network configuration
- Never commit API keys or credentials to the repository

### Data Quality Considerations
- Yahoo Finance data may have inconsistencies for technical indicators
- Quote delays vary by source: NASDAQ (5min), BigCharts (15min), Alpaca (real-time)
- News sentiment analysis requires balanced data sources to avoid bias
- Volume and price data should be cross-validated across multiple sources

### Development Notes
- Always activate Python virtual environment before development
- NLTK data must be downloaded separately for sentiment analysis
- Crawl4AI requires specific setup for JavaScript rendering
- Neo4j must be running and accessible before using graph database features

### Architecture Principles
- Modular design allows independent module development and testing
- Dual scraping engines provide redundancy and load distribution
- Knowledge graph storage enables complex relationship queries
- Multi-threaded processing maximizes data throughput while respecting rate limits