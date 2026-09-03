@echo off
cd /d "C:\Users\dbrace\code\bespin"
python collector\finviz_technical_small.py "C:\Users\dbrace\code\bespin\archive\finviz_bespin_test_screener_1_20260903_020034_v1.csv" > "C:\Users\dbrace\code\bespin\archive\etl_run_20260903_020034.log" 2>&1
echo BESPIN_ETL_DONE>>"C:\Users\dbrace\code\bespin\archive\etl_run_20260903_020034.log"
