from datetime import datetime, timezone, timedelta
from alpaca.trading.client import TradingClient
from session import get_target_session, describe, ET

client = TradingClient(API_KEY, API_SECRET, paper=True)

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