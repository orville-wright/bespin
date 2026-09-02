#!/usr/bin/env python3
"""
finviz_momentum.py -- loader for the Finviz top-%-gainers screen.

All logic lives in loader.py. This file exists only to pin the screener's
identity so it cannot be mistyped on the command line.

Usage:
    uv run python collector/finviz_momentum.py path\\to\\scraped.csv
    uv run python collector/finviz_momentum.py path\\to\\scraped.csv --dry-run
"""

import sys

from loader import main

SCREENER_NAME = "finviz_momentum"
SCREENER_VERSION = "v1"
RATIONALE = "Top %-gainers with volume confirmation"

if __name__ == "__main__":
    sys.exit(main(
        screener_name=SCREENER_NAME,
        screener_version=SCREENER_VERSION,
        rationale=RATIONALE,
    ))