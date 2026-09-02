"""
Paste-into-interpreter smoke test: screener CSV -> Supabase row payloads.
No file I/O, no network. Validates parse, null handling, canonicalisation,
JSON-serialisability, and the check constraints Postgres will enforce.
"""

import io
import json
import math
import re
from datetime import datetime, timezone, date

import pandas as pd

# ------------------------------------------------------------------
# 1. The CSV, inline
# ------------------------------------------------------------------
CSV = """num,ticker,beta,atr,sma20_pct,sma50_pct,sma200_pct,high_52w_pct,low_52w_pct,rsi,price,change_pct,change_from_open_pct,gap_pct,volume
1,MOVE,1.22,1.79,18.94,-6.99,5.12,-52.06,272.50,56.58,12.81,16.99,26.96,-7.85,810443
2,MGRT,106.56,5.20,10.72,20.44,104.49,-36.48,3406.17,66.13,107.99,10.12,10.03,0.08,37034
3,ARCT,2.47,1.22,49.52,97.37,112.72,-32.87,195.00,81.15,16.23,9.93,9.11,0.75,1368244
4,SLN,1.44,0.91,9.00,25.98,94.99,-16.13,251.79,65.81,14.74,8.94,9.27,-0.30,1134889
5,LMB,1.41,2.83,1.05,-29.13,-41.89,-61.04,9.94,36.76,44.79,8.66,8.45,0.19,885299
6,CBIO,1.23,1.20,5.42,7.77,16.78,-34.59,105.62,55.92,17.93,8.01,9.13,-1.02,788171
7,NVAX,2.45,0.52,18.47,19.22,19.01,-15.46,63.23,71.68,10.12,8.00,10.00,-1.81,10040575
8,CLYM,0.10,1.25,-3.85,6.71,74.11,-19.15,862.34,49.77,14.82,7.94,6.77,1.09,1972572
9,CNXC,0.46,1.86,22.47,29.57,5.24,-44.22,68.83,75.49,32.28,7.24,7.78,-0.50,2391548
10,SDGR,1.69,0.95,10.45,22.22,38.42,-9.47,90.41,65.72,20.84,6.87,8.26,-1.28,1508067
11,AGBK,,0.27,6.79,-1.00,-14.03,-45.70,12.18,57.88,6.63,6.76,5.74,0.97,302445
12,GMRS,,0.83,-0.50,-3.99,-0.41,-18.84,26.47,48.85,12.97,6.75,5.79,0.91,835474
13,BRAI,,0.46,5.30,2.35,-17.03,-82.33,39.25,54.54,5.83,6.39,6.00,0.36,55350
14,VIR,1.62,0.58,18.82,20.50,36.88,-0.26,144.84,71.63,11.63,6.31,7.69,-1.28,2778728
15,OBE,0.19,0.59,10.04,22.68,34.36,-16.86,122.98,62.40,12.13,5.94,1.68,4.19,843209
16,FORTY,1.22,3.42,13.69,10.35,13.17,-25.59,28.58,64.08,129.00,5.74,2.09,3.57,96
17,AGRO,-0.04,0.52,17.69,19.41,15.05,-24.35,74.46,69.07,12.02,5.62,4.80,0.79,1387377
18,VSTM,0.32,0.42,9.55,28.61,21.73,-32.33,121.87,67.24,7.61,5.40,5.84,-0.42,2079540
19,FTH,-0.28,2.94,10.90,19.18,62.48,-8.94,363.22,62.34,34.51,5.31,3.20,2.04,276726
20,IPI,1.26,1.68,9.68,14.86,16.51,-19.15,80.49,70.43,40.70,5.25,4.44,0.78,388926
21,TBN,-2.03,1.52,8.56,16.76,24.80,-23.71,103.73,70.43,39.83,5.15,-1.17,6.39,339395
22,SGRY,1.92,0.76,0.32,-5.36,-1.14,-37.71,27.96,49.38,14.60,5.11,5.11,0.00,3962461
23,MNPR,1.50,7.05,2.37,9.17,57.68,-5.81,258.77,57.31,117.57,5.04,6.68,-1.54,95185
"""

# ------------------------------------------------------------------
# 2. Config that the real script would supply
# ------------------------------------------------------------------
SCREENER_NAME    = "finviz_momentum"
SCREENER_VERSION = "v1"
COLLECTOR_NAME   = "orville-sfo"
RATIONALE        = "Top %-gainers with volume confirmation"
TARGET_SESSION   = date(2026, 8, 28)          # ET-derived in production

KEY_EXCLUDE      = ("num", "ticker")
SYMBOL_RE        = re.compile(r"^[A-Z]{1,5}(\.[A-Z]{1,2})?$")   # mirrors the DB check


# ------------------------------------------------------------------
# 3. Helpers (identical to the production ones)
# ------------------------------------------------------------------
def to_canonical(sym: str) -> str:
    """Alpaca convention: uppercase, dot separator for share classes."""
    return sym.strip().upper().replace("-", ".")


def clean(v):
    """NaN -> None so it serialises to JSON null, not bare NaN."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return v


# ------------------------------------------------------------------
# 4. Parse
# ------------------------------------------------------------------
df = pd.read_csv(
    io.StringIO(CSV),
    dtype={"ticker": "string"},
    keep_default_na=False,
    na_values=[""],
)

# ------------------------------------------------------------------
# 5. Transform to Supabase payloads
# ------------------------------------------------------------------
now_utc = datetime.now(timezone.utc).isoformat()

rows = [
    {
        "symbol":           to_canonical(r["ticker"]),
        "screener_name":    SCREENER_NAME,
        "screener_version": SCREENER_VERSION,
        "collector":        COLLECTOR_NAME,
        "target_session":   TARGET_SESSION.isoformat(),
        "rationale":        RATIONALE,
        "screened_at":      now_utc,
        "metrics": {
            "source_rank": int(r["num"]),
            **{k: clean(v) for k, v in r.items() if k not in KEY_EXCLUDE},
        },
    }
    for r in df.to_dict(orient="records")
]

# ------------------------------------------------------------------
# 6. Assertions
# ------------------------------------------------------------------
results = []


def check(name, expected, actual, passed):
    results.append((name, str(expected), str(actual), "PASS" if passed else "*** FAIL ***"))


check("C01 row count", 23, len(df), len(df) == 23)

check("C02 beta dtype is float64", "float64", df["beta"].dtype, df["beta"].dtype == "float64")

n_beta_null = int(df["beta"].isna().sum())
check("C03 beta nulls", 3, n_beta_null, n_beta_null == 3)

n_other_null = int(df.drop(columns=["beta"]).isna().sum().sum())
check("C04 nulls elsewhere", 0, n_other_null, n_other_null == 0)

n_dupes = int(df["ticker"].duplicated().sum())
check("C05 duplicate tickers", 0, n_dupes, n_dupes == 0)

bad_syms = [r["symbol"] for r in rows if not SYMBOL_RE.match(r["symbol"])]
check("C06 symbols pass DB check regex", "none", bad_syms or "none", not bad_syms)

check("C07 collector whitelisted", "yes", COLLECTOR_NAME,
      COLLECTOR_NAME in ("wilbur-akl", "orville-sfo"))

# JSON round-trip: the real failure mode NaN would cause
try:
    blob = json.dumps(rows, allow_nan=False)
    json.loads(blob)
    check("C08 JSON serialisable (allow_nan=False)", "ok", "ok", True)
except (ValueError, TypeError) as e:
    check("C08 JSON serialisable (allow_nan=False)", "ok", f"ERROR: {e}", False)

agbk = next(r for r in rows if r["symbol"] == "AGBK")
check("C09 AGBK beta is None", None, agbk["metrics"]["beta"], agbk["metrics"]["beta"] is None)

check("C10 AGBK beta -> JSON null", "null",
      json.dumps(agbk["metrics"]["beta"]), json.dumps(agbk["metrics"]["beta"]) == "null")

# numpy scalars would break the SDK's serialiser
leaked = {type(v).__name__ for r in rows for v in r["metrics"].values()
          if v is not None and type(v).__module__ == "numpy"}
check("C11 no numpy scalars leaked", "none", leaked or "none", not leaked)

move = next(r for r in rows if r["symbol"] == "MOVE")
check("C12 metrics key count", 14, len(move["metrics"]), len(move["metrics"]) == 14)

check("C13 source_rank promoted", 1, move["metrics"].get("source_rank"),
      move["metrics"].get("source_rank") == 1)

check("C14 'num'/'ticker' not in metrics", "absent",
      [k for k in move["metrics"] if k in KEY_EXCLUDE] or "absent",
      not any(k in move["metrics"] for k in KEY_EXCLUDE))

check("C15 volume is int", "int", type(move["metrics"]["volume"]).__name__,
      isinstance(move["metrics"]["volume"], int))

check("C16 screened_at is UTC", "+00:00", now_utc[-6:], now_utc.endswith("+00:00"))

rsi_min, rsi_max = df["rsi"].min(), df["rsi"].max()
check("C17 RSI within 0-100", "0-100", f"{rsi_min:.2f}-{rsi_max:.2f}",
      0 <= rsi_min and rsi_max <= 100)

# ------------------------------------------------------------------
# 7. Report
# ------------------------------------------------------------------
print("\n" + "=" * 78)
print(f"{'CHECK':<42}{'EXPECTED':<12}{'ACTUAL':<12}{'STATUS'}")
print("=" * 78)
for name, exp, act, status in results:
    print(f"{name:<42}{exp[:11]:<12}{act[:11]:<12}{status}")
print("=" * 78)
n_fail = sum(1 for r in results if r[3] != "PASS")
print(f"{len(results) - n_fail}/{len(results)} passed"
      + ("" if n_fail == 0 else f"  --  {n_fail} FAILED"))

print("\n--- dtypes ---")
print(df.dtypes.to_string())

print("\n--- sample payload (MOVE) ---")
print(json.dumps(move, indent=2))

print("\n--- sample payload (AGBK, null beta) ---")
print(json.dumps(agbk, indent=2))

print(f"\n{len(rows)} rows ready for upsert "
      f"(on_conflict='symbol,screener_name,target_session')")
