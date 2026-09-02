
import sys
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

ALPACA_API_KEY = os.getenv('ALPACA_API_KEY')

ALPACA_SEC_KEY = os.getenv('ALPACA_SEC_KEY')

 
# ------------------------------------------------------------------
# 0. Load .env explicitly and loudly
# ------------------------------------------------------------------
try:
    from dotenv import find_dotenv, load_dotenv
except ImportError:
    print("ERROR: python-dotenv not installed.  uv add python-dotenv", file=sys.stderr)
    sys.exit(2)
 
# raise_error_if_not_found=True -- the default silently returns "" and
# load_dotenv() then does nothing at all.
try:
    env_path = find_dotenv(raise_error_if_not_found=True, usecwd=False)
except OSError:
    print("ERROR: no .env found walking up from this file.", file=sys.stderr)
    sys.exit(2)
 
load_dotenv(env_path)
print(f"loaded .env from: {env_path}")

# ------------------------------------------------------------------
# 1. Resolve credentials, naming the exact variable that is missing
# ------------------------------------------------------------------
def require(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        # Show what ALPACA_* keys DO exist, so a name typo is obvious.
        available = sorted(k for k in os.environ if "ALPACA" in k.upper())
        print(f"ERROR: {name} is missing or empty.", file=sys.stderr)
        print(f"       ALPACA_* vars actually present: {available or '(none)'}",
              file=sys.stderr)
        sys.exit(2)
    return val.strip()
 
 
ALPACA_API_KEY = require("ALPACA_API_KEY")
ALPACA_SEC_KEY = require("ALPACA_SEC_KEY")
 
print(f"api key:  {ALPACA_API_KEY[:4]}...{ALPACA_API_KEY[-2:]} "
      f"(len {len(ALPACA_API_KEY)})")
print(f"sec key:  ...{ALPACA_SEC_KEY[-2:]} (len {len(ALPACA_SEC_KEY)})")
 
# Paper keys start PK, live keys start AK. Mismatching key type against
# the paper= flag is the next most common failure after missing vars.
IS_PAPER = ALPACA_API_KEY.startswith("PK")
print(f"key type: {'paper' if IS_PAPER else 'live'}  ->  paper={IS_PAPER}")
 
 
# ------------------------------------------------------------------
# 2. Live calendar checks
# ------------------------------------------------------------------
from alpaca.trading.client import TradingClient          # noqa: E402
from session import ET, describe, get_target_session     # noqa: E402
 
client = TradingClient(ALPACA_API_KEY, ALPACA_SEC_KEY, paper=IS_PAPER)
 
results = []
 
 
def check(name, expected, actual, passed):
    results.append((name, str(expected), str(actual), "PASS" if passed else "*** FAIL ***"))
 
 
now = datetime.now(timezone.utc)
 
# -- L01 reachable ---------------------------------------------------
try:
    target = get_target_session(client, now)
    check("L01 calendar reachable", "ok", "ok", True)
except Exception as e:
    check("L01 calendar reachable", "ok", f"ERROR: {e}", False)
    target = None
 
if target:
    # -- L02 not in the past -----------------------------------------
    today_et = now.astimezone(ET).date()
    check("L02 target not before today ET", f">= {today_et}", target, target >= today_et)
 
    # -- L03 deterministic for a fixed instant -----------------------
    check("L03 same instant -> same session", target,
          get_target_session(client, now), get_target_session(client, now) == target)
 
    # -- L04 rolling window never raises, never goes backwards -------
    prev, ok, detail = None, True, ""
    for d in range(14):
        try:
            t = get_target_session(client, now + timedelta(days=d))
        except Exception as e:
            ok, detail = False, f"day {d}: {e}"
            break
        if prev is not None and t < prev:
            ok, detail = False, f"day {d}: {t} < {prev}"
            break
        prev = t
    check("L04 14-day walk monotonic", "monotonic", detail or "monotonic", ok)
 
    # -- L05 rollover: just after today's close ----------------------
    from session import fetch_calendar
    cal = fetch_calendar(client, today_et - timedelta(days=1),
                         today_et + timedelta(days=10))
    nxt = next((s for s in cal if s.date >= target), None)
    if nxt:
        after_close = (nxt.close.replace(tzinfo=ET) + timedelta(minutes=1)) \
            .astimezone(timezone.utc)
        rolled = get_target_session(client, after_close)
        check("L05 rolls forward after close", f"> {target}", rolled, rolled > target)
 
# ------------------------------------------------------------------
print("\n" + "=" * 74)
print(f"{'CHECK':<40}{'EXPECTED':<16}{'ACTUAL':<12}{'STATUS'}")
print("=" * 74)
for n, e, a, s in results:
    print(f"{n:<40}{e[:15]:<16}{a[:11]:<12}{s}")
print("=" * 74)
nf = sum(1 for r in results if r[3] != "PASS")
print(f"{len(results) - nf}/{len(results)} passed" + ("" if nf == 0 else f"  --  {nf} FAILED"))
 
if target:
    print("\n" + describe(client, now))
 
sys.exit(1 if nf else 0)