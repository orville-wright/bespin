# Yahoo Finance News Scraper - crawl4ai Version

This directory contains the converted Yahoo Finance news scraper that uses **crawl4ai** instead of BeautifulSoup4.

## Files Converted

### Main Scripts
- `ml_yahoofinews_crawl4ai.py` - Main Yahoo Finance news scraper (converted from `ml_yahoofinews.py`)
- `ml_nlpreader_crawl4ai.py` - NLP reader and coordinator (converted from `ml_nlpreader.py`)
- `YAHOO_FINANCE_crawl4ai_schema.json` - JSON schema for crawl4ai extraction

### Supporting Scripts (Unchanged)
- `ml_sentiment.py` - Sentiment analysis using transformers
- `ml_urlhinter.py` - URL classification and hinting
- `ml_cvbow.py` - Count vectorizer for bag-of-words analysis

### Test Script
- `test_crawl4ai_yahoo.py` - Comprehensive test script

## Key Changes from BeautifulSoup to crawl4ai

### 1. Data Extraction Method
**Before (BeautifulSoup):**
```python
self.nsoup = BeautifulSoup(html_content, "html.parser")
articles = self.nsoup.find_all("section", class_="yf-1ce4p3e")
```

**After (crawl4ai):**
```python
extraction_strategy = JsonCssExtractionStrategy(schema)
result = await crawler.arun(url, config=config)
self.extracted_articles = json.loads(result.extracted_content)
```

### 2. CSS Selector Mapping
The BeautifulSoup CSS selectors were mapped to crawl4ai JSON schema:

| Element | BeautifulSoup Selector | crawl4ai Schema |
|---------|----------------------|-----------------|
| News Container | `section.yf-1ce4p3e` | `baseSelector: "section.yf-1ce4p3e li"` |
| Article Title | `h3.text` | `selector: "h3", type: "text"` |
| Article URL | `a['href']` | `selector: "h3 a", type: "attribute", attribute: "href"` |
| Publisher | `.publishing.yf-1weyqlp` | `selector: ".publishing.yf-1weyqlp", type: "text"` |
| Author | `.byline-attr-author.yf-1k5w6kz` | `selector: ".byline-attr-author.yf-1k5w6kz", type: "text"` |

### 3. Async Architecture
- All crawl4ai operations are async
- Added async wrappers for compatibility with existing synchronous code
- Maintained backward compatibility through sync wrapper methods

### 4. Error Handling
- Enhanced error handling for network timeouts
- Better logging for debugging crawl4ai operations
- Fallback mechanisms for failed extractions

## Critical CSS Selectors Preserved

The following CSS selectors from the original code were carefully preserved:

1. **News Feed Container**: `section.yf-1ce4p3e` - Main container for news articles
2. **Article List Items**: `li` elements within the news section
3. **Article Titles**: `h3` tags containing article headlines
4. **Article URLs**: `a` tags with `href` attributes
5. **Publisher Info**: `.publishing.yf-1weyqlp` - News agency information
6. **Author Info**: `.byline-attr-author.yf-1k5w6kz` - Article author
7. **Publication Date**: `.byline-attr-time-style time` - When article was published
8. **Article Body**: `.body.yf-1ir6o1g p` - Full article content (for detailed analysis)

## Usage

### Basic Usage
```python
from ml_nlpreader_crawl4ai import ml_nlpreader

# Initialize with configuration
global_args = {
    'bool_xray': True,      # Enable debug output
    'bool_verbose': True,   # Enable verbose logging
    'bool_news': True       # Enable news processing
}

nlp_reader = ml_nlpreader(1, global_args)

# Run analysis for a symbol
results = nlp_reader.run_full_analysis("AAPL")
```

### Advanced Usage
```python
from ml_yahoofinews_crawl4ai import yfnews_reader
from ml_urlhinter import url_hinter
import asyncio

async def extract_news():
    # Create instances
    yfn_reader = yfnews_reader(1, "TSLA", global_args)
    uh = url_hinter(1, global_args)
    yfn_reader.share_hinter(uh)
    
    # Form endpoint
    yfn_reader.form_endpoint("TSLA")
    
    # Extract news using crawl4ai
    hash_state = await yfn_reader.crawl4ai_extract_news(0)
    
    if hash_state:
        # Process the data
        yfn_reader.scan_news_feed("TSLA", 0, 1, 0, hash_state)
        yfn_reader.eval_news_feed_stories("TSLA")
        
        return yfn_reader.ml_ingest
    
    return None

# Run async extraction
results = asyncio.run(extract_news())
```

## Testing

Run the test script to verify the conversion:

```bash
python test_crawl4ai_yahoo.py
```

The test script includes:
1. JSON schema validation
2. Direct crawl4ai extraction test
3. Full workflow integration test
4. Sentiment analysis validation

## Dependencies

Make sure you have the following packages installed:

```bash
pip install crawl4ai
pip install requests-html
pip install beautifulsoup4
pip install pandas
pip install numpy
pip install nltk
pip install transformers
pip install playwright
pip install rich
```

## Configuration

The JSON schema file (`YAHOO_FINANCE_crawl4ai_schema.json`) controls what data is extracted. You can modify it to extract additional fields or change selectors if Yahoo Finance updates their HTML structure.

## Performance Notes

- crawl4ai is generally faster than BeautifulSoup for large-scale scraping
- Async operations allow for better concurrency
- JSON schema-based extraction is more robust than manual HTML parsing
- Built-in JavaScript rendering handles dynamic content better

## Troubleshooting

1. **No articles extracted**: Check if Yahoo Finance has changed their HTML structure
2. **Timeout errors**: Increase timeout values in crawl4ai configuration
3. **JavaScript errors**: Ensure Playwright is properly installed
4. **Schema errors**: Validate the JSON schema file

## Compatibility

The converted scripts maintain full compatibility with the original API:
- All class methods have the same signatures
- Data structures remain unchanged
- Error handling is preserved
- Logging output is consistent

This ensures that any existing code using the original BeautifulSoup version will work with minimal modifications.
