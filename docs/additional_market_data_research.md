# Additional Market Data Services Research & Extractors

## Summary

This document provides comprehensive research findings for 11 additional market data services and details which extractors were successfully created.

## Services Researched

### 1. Morningstar Developer API (developer.morningstar.com)
- **Status**: ❌ No Extractor Created
- **API Key Required**: YES (Enterprise Only)
- **Access**: Sales team contact required, no self-service signup
- **Free Tier**: None available
- **Pricing**: Custom enterprise pricing only
- **Recommendation**: Not suitable for individual developers or small projects
- **Reason for No Extractor**: Enterprise-only access with sales-gated authentication

### 2. EOD Historical Data (eodhistoricaldata.com)
- **Status**: ✅ Extractor Created (`eodhistoricaldata_md.py`)
- **API Key Required**: YES
- **Environment Variable**: `EODHISTORICALDATA_API_TOKEN`
- **Free Tier**: 20 API calls/day
- **Features**: End-of-day data, real-time quotes (15-20 min delay), fundamentals, technical indicators
- **Demo Token**: "demo" works for AAPL.US, TSLA.US, VTI.US, AMZN.US, BTC-USD.CC, EURUSD.FOREX
- **Coverage**: 150,000+ tickers from 60+ global exchanges
- **Pricing**: $19.99/month and up

### 3. FinancialModelingPrep (financialmodelingprep.com)
- **Status**: ✅ Extractor Created (`financialmodelingprep_md.py`)
- **API Key Required**: YES
- **Environment Variable**: `FINANCIALMODELINGPREP_API_KEY`
- **Free Tier**: 250 API calls/day
- **Features**: Real-time quotes, historical data, fundamentals, financial statements
- **Coverage**: Global markets, 70,000+ stocks
- **Pricing**: $19/month (Starter), $49/month (Premium), $99/month (Ultimate)
- **Trusted by**: RBC Capital Markets, Franklin Templeton, Intel, KPMG

### 4. Stooq (stooq.com)
- **Status**: ✅ Extractor Created (`stooq_md.py`)
- **API Key Required**: NO
- **Access Method**: URL-based CSV downloads (unofficial API patterns)
- **Free Tier**: Full access (with reasonable usage)
- **Features**: Historical data (30+ years), global coverage, indices, crypto, forex
- **Coverage**: 21,332 global securities, 1,980 currency pairs, 132 cryptocurrencies
- **Limitations**: No official API, requires respectful usage with delays

### 5. Tradier API (tradier.com)
- **Status**: ❌ No Extractor Created
- **API Key Required**: YES (OAuth 2.0)
- **Access**: Requires Tradier Brokerage account for real-time data
- **Free Tier**: Available with brokerage account
- **Features**: US stocks/options, real-time quotes, options Greeks
- **Limitations**: 
  - Complex OAuth authentication (24-hour token expiry)
  - Personal use only unless Tradier Partner
  - US markets only
  - Requires brokerage account for real-time data
- **Reason for No Extractor**: Complex OAuth setup and brokerage account requirement

### 6. IEX Cloud (iexcloud.io)
- **Status**: ❌ Service Discontinued
- **Shutdown Date**: August 31, 2024
- **Successor**: viaNexus (https://vianexus.com)
- **Note**: Original IEX Cloud no longer available
- **Alternative**: Consider viaNexus for similar functionality

### 7. Intrinio (intrinio.com)
- **Status**: ❌ No Extractor Created
- **API Key Required**: YES
- **Free Tier**: Trial only (no permanent free tier)
- **Features**: High-quality normalized financial data
- **Pricing**: Enterprise/custom pricing
- **Target Market**: Financial institutions and enterprises
- **Reason for No Extractor**: No free tier, enterprise-focused pricing

### 8. FreeRealTime.com
- **Status**: ❌ No Extractor Created
- **API Key Required**: NO
- **Access Method**: Web scraping required
- **Features**: Real-time US stock quotes
- **Limitations**: 
  - No official API
  - US equities only
  - Requires web scraping implementation
  - Potential anti-scraping measures
- **Reason for No Extractor**: No API, scraping complexity and reliability concerns

### 9. FinancialContent.com
- **Status**: ❌ No Extractor Created
- **API Key Required**: NO
- **Access Method**: URL endpoints (by Nasdaq)
- **Features**: Stock quotes and charts
- **Limitations**: 
  - Undocumented API
  - May change without notice
  - Limited reliability guarantees
- **Reason for No Extractor**: Undocumented endpoints with potential reliability issues

### 10. TradingView (tradingview.com)
- **Status**: ❌ No Extractor Created
- **API Key Required**: NO for widgets
- **Access Method**: Widgets, unofficial endpoints
- **Features**: Charts, delayed data
- **Limitations**: 
  - No official public API
  - Widget-based access
  - Terms of service restrictions on data extraction
- **Reason for No Extractor**: No official API, TOS restrictions

### 11. WorldTradingData (worldtradingdata.com)
- **Status**: ❌ No Extractor Created
- **Service Status**: Questionable reliability
- **Features**: Global stocks, crypto
- **Limitations**: 
  - Service availability varies
  - Uncertain operational status
  - Documentation quality concerns
- **Reason for No Extractor**: Unreliable service status

## Extractors Successfully Created

### 1. eodhistoricaldata_md.py
**Features:**
- End-of-day historical data
- Real-time quotes (15-20 min delay)
- Fundamental data (10 API calls per request)
- Intraday data (5 API calls per request)
- Technical indicators
- Dividends and splits
- Global exchange support

**Key Methods:**
```python
get_eod_data(symbol, exchange='US', date_from=None, date_to=None, period='d')
get_realtime_data(symbols, exchange='US')
get_fundamentals(symbol, exchange='US')
get_intraday_data(symbol, exchange='US', interval='5m', date=None)
get_technical_indicators(symbol, exchange='US', function='sma', period=50)
```

### 2. financialmodelingprep_md.py
**Features:**
- Real-time stock quotes
- Historical price data
- Company profiles
- Financial statements (income, balance sheet, cash flow)
- Key financial metrics and ratios
- Market gainers/losers

**Key Methods:**
```python
get_quote(symbols)  # Max 3 symbols for batch requests
get_historical_data(symbol, date_from=None, date_to=None, timeseries=None)
get_company_profile(symbol)
get_income_statement(symbol, period='annual', limit=5)
get_balance_sheet(symbol, period='annual', limit=5)
get_cash_flow(symbol, period='annual', limit=5)
```

### 3. stooq_md.py
**Features:**
- Global historical data (30+ years)
- Current quotes via CSV download
- Index data (^DJI, ^GSPC, etc.)
- Cryptocurrency data
- Forex data
- Multiple market support

**Key Methods:**
```python
get_historical_data(symbol, market='US', interval='d', days_back=365)
get_current_quote(symbol, market='US', include_headers=True)
get_index_data(index_symbol, interval='d', days_back=365)
get_crypto_data(crypto_symbol, interval='d', days_back=365)
get_forex_data(base_currency, quote_currency, interval='d', days_back=365)
```

## Environment Variables Setup

Add these to your `.env` file for the new extractors:

```bash
# EOD Historical Data
EODHISTORICALDATA_API_TOKEN=your_eod_api_token_here

# FinancialModelingPrep
FINANCIALMODELINGPREP_API_KEY=your_fmp_api_key_here

# Note: Stooq doesn't require an API key
```

## Usage Recommendations

### For Free Historical Data:
- **Stooq**: Best for bulk historical data (no API key required)
- **EOD Historical Data**: Good for recent data with demo token

### For Real-time Quotes:
- **FinancialModelingPrep**: 250 calls/day (good free tier)
- **EOD Historical Data**: 20 calls/day (limited but quality data)

### For Fundamental Analysis:
- **FinancialModelingPrep**: Excellent for financial statements and ratios
- **EOD Historical Data**: Good for basic fundamentals

### For Global Markets:
- **EOD Historical Data**: 60+ exchanges worldwide
- **Stooq**: Global coverage including emerging markets

## Rate Limiting Summary

| Service | Free Tier Limit | API Calls per Request |
|---------|----------------|----------------------|
| EOD Historical Data | 20/day | 1 (EOD), 5 (intraday), 10 (fundamentals) |
| FinancialModelingPrep | 250/day | 1 per symbol |
| Stooq | No official limit | 1 per request (be respectful) |

## Integration Notes

All extractors follow the same pattern as existing extractors:
- Similar class structure and method naming
- Consistent error handling and logging
- Pandas DataFrame outputs
- Environment variable management via python-dotenv
- Example usage in `main()` function

## Limitations and Considerations

1. **Rate Limits**: All services have usage restrictions - implement appropriate delays
2. **Data Quality**: Free tiers may have delayed data or limited historical depth
3. **Reliability**: Web scraping services (like Stooq) may be less reliable than official APIs
4. **Terms of Service**: Always review and comply with each service's terms of use
5. **Commercial Use**: Some services restrict commercial usage on free tiers

## Next Steps

1. Test extractors with your specific requirements
2. Consider upgrading to paid tiers for production use
3. Implement rate limiting and error retry logic
4. Monitor service availability and update extractors as needed
5. Consider data caching to minimize API calls