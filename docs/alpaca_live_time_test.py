
import os
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
from alpaca.trading.client import TradingClient
from session import get_target_session, describe, ET


DATA_BASE_URL = "https://data.alpaca.markets/v2/"
PAPER_TRADING_BASE_URL = "https://paper-api.alpaca.markets/v2/"
LIVE_TRADING_BASE_URL = "https://api.alpaca.markets/v2/"
VALID_FEEDS = {"iex", "sip", "delayed_sip", "boats", "overnight", "otc"}
ALPACA_ENDPOINT = "https://paper-api.alpaca.markets/v2"

    
client = TradingClient(ALPACA_API_KEY, ALPACA_SEC_KEY, paper=True)

print(describe(client))

# sanity: the resolved session must not be in the past, ET-wise
target = get_target_session(client)
today_et = datetime.now(timezone.utc).astimezone(ET).date()
assert target >= today_et, f"resolved {target}, which is before today ET ({today_et})"

# simulate Chris: same instant, nothing about the call changes
now = datetime.now(timezone.utc)
assert get_target_session(client, now) == target

# rolling window: 14 consecutive days should never raise and never go backwards
prev = None
for d in range(14):
    t = get_target_session(client, now + timedelta(days=d))
    assert prev is None or t >= prev, f"went backwards at day {d}"
    prev = t
print("live calendar: OK")