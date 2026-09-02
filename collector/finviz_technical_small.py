# collector/finviz_technical_small.py
import sys
from loader import main

if __name__ == "__main__":
    sys.exit(main(
        screener_name="finviz_technical_small",
        screener_version="v1",
        rationale="Small-cap technical screen, price >$5, volume >10x avg",
    ))