# collector/finviz_technical_small.py
"""
finviz_technical_small.py -- ETL CSV supabase table loader
upserts data into tabel: screened_candidate_targets
Extarcts data from Finviz.com data predefined screener

All logic lives in loader.py. This file exists only to pin the screener's
identity so it cannot be mistyped on the command line and for FastAPI server support.

usage: finviz_technical_small.py [-h] [--dry-run] [--env-file ENV_FILE] csv_path

Examples:
    python collector/finviz_momentum.py path\\to\\scraped.csv
    uv run collector/finviz_momentum.py path\\to\\scraped.csv --dry-run
    

positional arguments resolved from loader.py:
  csv_path             path to the scraped CSV

options:
  -h, --help           show this help message and exit
  --dry-run            validate and resolve session, but do not upsert
  --env-file ENV_FILE  explicit .env path (default: search upward)
"""

import sys
from loader import main

if __name__ == "__main__":
    sys.exit(main(
        screener_name="finviz_technical_small",
        screener_version="v1",
        rationale="Small-cap technical screen, price >$5, volume >10x avg",
    ))