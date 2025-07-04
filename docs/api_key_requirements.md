# Free Stock Market Data Services - API Key Requirements

## Summary

This document lists all the free stock market data services researched and indicates which ones require API keys for access. Python extractors have been created for the key API-based services.

## Services Requiring API Keys (Python Extractors Created)

### 1. Finnhub (finnhub_md.py)
- **API Key Required**: YES
- **Environment Variable**: `FINNHUB_API_KEY`
- **Free Tier**: 60 calls/minute
- **Signup**: https://finnhub.io
- **Features**: Real-time quotes, historical data, company fundamentals, news
- **Authentication**: Header-based (`X-Finnhub-Token`)

### 2. Marketstack (marketstack_md.py)
- **API Key Required**: YES
- **Environment Variable**: `MARKETSTACK_API_KEY`
- **Free Tier**: 100 requests/month
- **Signup**: https://marketstack.com
- **Features**: Global stock data, end-of-day prices, intraday data (paid plans)
- **Authentication**: URL parameter (`access_key`)

### 3. StockData.org (stockdata_md.py)
- **API Key Required**: YES
- **Environment Variable**: `STOCKDATA_API_TOKEN`
- **Free Tier**: 100 requests/day
- **Signup**: https://stockdata.org
- **Features**: US stock quotes, historical data, company profiles, news
- **Authentication**: URL parameter (`api_token`)

### 4. Twelve Data (twelvedata_md.py)
- **API Key Required**: YES
- **Environment Variable**: `TWELVEDATA_API_KEY`
- **Free Tier**: 8 requests/minute, 800 requests/day
- **Signup**: https://twelvedata.com
- **Features**: Stocks, forex, crypto, fundamentals
- **Authentication**: URL parameter (`apikey`) or header

### 5. IEX Cloud (DISCONTINUED)
- **Status**: Shut down August 31, 2024
- **Successor**: viaNexus (https://vianexus.com)
- **API Key Required**: YES (for successor)
- **Note**: Original IEX Cloud no longer available

### 6. FinancialModelingPrep
- **API Key Required**: YES
- **Free Tier**: 250 calls/day
- **Signup**: https://financialmodelingprep.com
- **Features**: Real-time quotes, fundamentals, financial statements
- **Status**: Extractor not created (can be added if needed)

### 7. EOD Historical Data
- **API Key Required**: YES
- **Free Tier**: 20 calls/day
- **Signup**: https://eodhistoricaldata.com
- **Features**: Historical data, intraday bars, fundamentals
- **Status**: Extractor not created (limited free quota)

### 8. Nasdaq Data Link (formerly Quandl)
- **API Key Required**: YES (for API access)
- **Free Tier**: Available for non-premium datasets
- **Signup**: https://data.nasdaq.com
- **Features**: Historical data, specialized datasets
- **Status**: Extractor not created (primarily historical/specialized data)

### 9. Tradier API
- **API Key Required**: YES
- **Free Tier**: Available with free Tradier account
- **Signup**: https://tradier.com
- **Features**: Real-time quotes, options data, streaming
- **Status**: Extractor not created (OAuth complexity)

## Services NOT Requiring API Keys (No Extractors Created)

### 1. Google Finance
- **API Key Required**: NO
- **Access Method**: Google Sheets GOOGLEFINANCE() function or web scraping
- **Features**: Delayed quotes, historical prices
- **Limitations**: No official API, rate limits unclear

### 2. MSN Money
- **API Key Required**: NO
- **Access Method**: Web scraping undocumented endpoints
- **Features**: Delayed quotes, charts
- **Limitations**: Unofficial, may break without notice

### 3. Investing.com
- **API Key Required**: NO
- **Access Method**: Web scraping
- **Features**: Real-time quotes, charts, global coverage
- **Limitations**: No official API, anti-scraping measures

### 4. FreeRealTime.com
- **API Key Required**: NO
- **Access Method**: Web scraping
- **Features**: Real-time US stock quotes
- **Limitations**: US equities only, scraping required

### 5. CNBC
- **API Key Required**: NO
- **Access Method**: Web scraping quote pages
- **Features**: Delayed quotes, market data
- **Limitations**: Delayed data, scraping required

### 6. FinancialContent.com
- **API Key Required**: NO
- **Access Method**: Simple URL endpoints (by Nasdaq)
- **Features**: Stock quotes, charts
- **Limitations**: May change without notice

### 7. Stooq
- **API Key Required**: NO
- **Access Method**: CSV downloads, web scraping
- **Features**: Historical data, global coverage
- **Limitations**: No real-time data, manual downloads

### 8. World Trading Data
- **API Key Required**: Unclear (service availability varies)
- **Status**: Service reliability questionable
- **Features**: Global stocks, crypto
- **Limitations**: Uncertain service status

### 9. Yahoo Finance CSV/JSON Endpoints
- **API Key Required**: NO
- **Access Method**: Hidden API endpoints
- **Features**: Historical data downloads
- **Limitations**: Unofficial, may break, rate limited

### 10. TradingView
- **API Key Required**: NO for widgets
- **Access Method**: Widgets, unofficial endpoints
- **Features**: Charts, delayed data
- **Limitations**: Widget-based, not a traditional API

## Existing Extractors (Already in Project)

The following extractors were already present in the project:

1. **alpaca_md.py** - Alpaca Markets (API key required)
2. **fred_md.py** - Federal Reserve Economic Data (API key required)
3. **nasdaq_quotes.py** - NASDAQ web scraping (no API key)
4. **polygon_md.py** - Polygon.io (API key required)
5. **sec_md.py** - SEC EDGAR (no API key required)
6. **tiingo_md.py** - Tiingo (API key required)

## Environment Variables Setup

To use the new extractors, add these environment variables to your `.env` file:

```bash
# New extractors
FINNHUB_API_KEY=your_finnhub_api_key_here
MARKETSTACK_API_KEY=your_marketstack_api_key_here
STOCKDATA_API_TOKEN=your_stockdata_api_token_here
TWELVEDATA_API_KEY=your_twelvedata_api_key_here

# Existing extractors
ALPACA_API-KEY=your_alpaca_api_key_here
ALPACA_SEC-KEY=your_alpaca_secret_key_here
FRED_API_KEY=your_fred_api_key_here
POLYGON_API_KEY=your_polygon_api_key_here
TIINGO_API_TOKEN=your_tiingo_api_token_here
```

## Usage Recommendations

1. **For Free Real-time Data**: Finnhub (60 calls/min) or Twelve Data (8 calls/min)
2. **For Historical Data**: StockData.org (100/day) or Marketstack (100/month)
3. **For High Volume**: Consider paid tiers or multiple free accounts
4. **For No API Key**: Use existing nasdaq_quotes.py or sec_md.py

## Rate Limiting Considerations

- Finnhub: 60 calls/minute (most generous)
- Twelve Data: 8 calls/minute, 800/day
- StockData.org: 100 calls/day
- Marketstack: 100 calls/month (very limited)

Plan your usage accordingly and consider implementing rate limiting in your applications.