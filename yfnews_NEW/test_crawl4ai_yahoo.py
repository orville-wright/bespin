#!/usr/bin/env python3
"""
Test script for crawl4ai-based Yahoo Finance news scraper
Demonstrates the conversion from BeautifulSoup to crawl4ai
"""

import sys
import asyncio
import logging
from pathlib import Path

# Add current directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from ml_yf_nlp_reader_c4 import ml_nlpreader
from ml_sentiment import ml_sentiment

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """
    Main test function
    """
    print("="*80)
    print("Yahoo Finance News Scraper - crawl4ai Version")
    print("="*80)
    
    # Configuration
    global_args = {
        'bool_xray': True,      # Enable debug output
        'bool_verbose': True,   # Enable verbose logging
        'bool_news': True       # Enable news processing
    }
    
    # Test symbol
    test_symbol = "AAPL"  # Apple Inc.
    
    print(f"\nTesting with symbol: {test_symbol}")
    print("-" * 40)
    
    try:
        # Initialize the NLP reader
        nlp_reader = ml_nlpreader(1, global_args)
        
        # Run full analysis workflow
        results = nlp_reader.run_full_analysis(test_symbol)
        
        print("\n" + "="*80)
        print("TEST COMPLETED SUCCESSFULLY")
        print("="*80)
        
        if results:
            print(f"\nResults for {test_symbol}:")
            for symbol, data in results.items():
                print(f"  - Processed {data['articles_processed']} articles")
                print(f"  - Total tokens: {data['total_tokens']}")
                print(f"  - Sentiment distribution: {data['sentiment_counts']}")
        else:
            print("No results generated - check logs for issues")
            
    except Exception as e:
        print(f"\nERROR: {e}")
        logging.error(f"Test failed: {e}", exc_info=True)
        return 1
    
    return 0

async def async_test():
    """
    Async test function for direct crawl4ai testing
    """
    from ml_yf_news_c4 import yfnews_reader
    from ml_urlhinter import url_hinter
    
    print("\n" + "="*60)
    print("DIRECT CRAWL4AI TEST")
    print("="*60)
    
    global_args = {
        'bool_xray': True,
        'bool_verbose': True
    }
    
    test_symbol = "TSLA"
    
    try:
        # Create instances
        yfn_reader = yfnews_reader(1, test_symbol, global_args)
        uh = url_hinter(1, global_args)
        yfn_reader.share_hinter(uh)
        
        # Form endpoint
        yfn_reader.form_endpoint(test_symbol)
        
        # Extract news using crawl4ai
        hash_state = await yfn_reader.crawl4ai_extract_news(0)
        
        if hash_state:
            # Process the data
            yfn_reader.scan_news_feed(test_symbol, 0, 1, 0, hash_state)
            yfn_reader.eval_news_feed_stories(test_symbol)
            
            print(f"\nExtracted {len(yfn_reader.ml_ingest)} articles")
            
            # Show some results
            for idx, article in list(yfn_reader.ml_ingest.items())[:3]:  # Show first 3
                print(f"\nArticle {idx}:")
                print(f"  Title: {article.get('title', 'N/A')}")
                print(f"  URL: {article.get('url', 'N/A')}")
                print(f"  Type: {article.get('type', 'N/A')}")
                print(f"  Publisher: {article.get('publisher', 'N/A')}")
        else:
            print("Failed to extract news data")
            
    except Exception as e:
        print(f"Async test failed: {e}")
        logging.error(f"Async test failed: {e}", exc_info=True)

def test_schema_loading():
    """
    Test JSON schema loading
    """
    import json
    from pathlib import Path
    
    print("\n" + "="*60)
    print("JSON SCHEMA TEST")
    print("="*60)
    
    schema_file = Path(__file__).parent / "YAHOO_FINANCE_crawl4ai_schema.json"
    
    try:
        with open(schema_file, 'r') as f:
            schema = json.load(f)
        
        print(f"Schema loaded successfully from: {schema_file}")
        print(f"Schema name: {schema.get('name')}")
        print(f"Base selector: {schema.get('baseSelector')}")
        print(f"Number of fields: {len(schema.get('fields', []))}")
        
        print("\nFields:")
        for field in schema.get('fields', []):
            print(f"  - {field.get('name')}: {field.get('selector')} ({field.get('type')})")
            
    except Exception as e:
        print(f"Schema test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("Starting Yahoo Finance crawl4ai conversion tests...")
    
    # Test 1: Schema loading
    if not test_schema_loading():
        sys.exit(1)
    
    # Test 2: Direct crawl4ai test
    print("\nRunning async crawl4ai test...")
    asyncio.run(async_test())
    
    # Test 3: Full workflow test
    print("\nRunning full workflow test...")
    exit_code = main()
    
    sys.exit(exit_code)
