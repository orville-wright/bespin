"""
Supabase wire test for screened_candidate_targets -- httpx only.

Identical coverage to wire_test.py but talks to PostgREST directly, so it
needs no `supabase` SDK and therefore no `realtime`/`websockets` stack.

Requires:  httpx, pandas  (both already in the project)
Env vars:  SUPABASE_URL, SUPABASE_SERVICE_KEY

Run:       python wire_test_httpx.py
"""

import io
import math
import os
import sys
from datetime import datetime, timezone, date

import httpx
import pandas as pd

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
TABLE            = "screened_candidate_targets"
VIEW             = "v_daily_conviction"
CONFLICT         = "symbol,screener_name,target_session"
SENTINEL_SESSION = date(2099, 1, 1)
SCREENER_NAME    = "wire_test"
SCREENER_VERSION = "v0"
COLLECTOR_NAME   = "orville-sfo"
RATIONALE        = "wire test - safe to delete"
KEY_EXCLUDE      = ("num", "ticker")

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

results = []


def check(name, expected, actual, passed):
    results.append((name, str(expected), str(actual), "PASS" if passed else "*** FAIL ***"))


def to_canonical(sym: str) -> str:
    return sym.strip().upper().replace("-", ".")


def clean(v):
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    return v


def build_rows(session: date, screener: str, collector: str):
    df = pd.read_csv(
        io.StringIO(CSV),
        dtype={"ticker": "string"},
        keep_default_na=False,
        na_values=[""],
    )
    now_utc = datetime.now(timezone.utc).isoformat()
    return [
        {
            "symbol":           to_canonical(r["ticker"]),
            "screener_name":    screener,
            "screener_version": SCREENER_VERSION,
            "collector":        collector,
            "target_session":   session.isoformat(),
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
# Thin PostgREST wrapper
# ------------------------------------------------------------------
class Rest:
    """Minimal PostgREST client. Only what this test needs."""

    def __init__(self, url: str, key: str):
        self.base = url.rstrip("/") + "/rest/v1"
        self.client = httpx.Client(
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    def upsert(self, table, rows, on_conflict, merge=True):
        resolution = "merge-duplicates" if merge else "ignore-duplicates"
        r = self.client.post(
            f"{self.base}/{table}",
            params={"on_conflict": on_conflict},
            json=rows,
            headers={"Prefer": f"resolution={resolution},return=minimal"},
        )
        r.raise_for_status()          # 4xx/5xx -> httpx.HTTPStatusError
        return r

    def select(self, table, columns="*", **filters):
        params = {"select": columns, **filters}
        r = self.client.get(f"{self.base}/{table}", params=params)
        r.raise_for_status()
        return r.json()

    def count(self, table, **filters):
        r = self.client.get(
            f"{self.base}/{table}",
            params={"select": "id", **filters},
            headers={"Prefer": "count=exact", "Range-Unit": "items", "Range": "0-0"},
        )
        r.raise_for_status()
        # Content-Range looks like "0-0/46"
        return int(r.headers["content-range"].split("/")[-1])

    def delete(self, table, **filters):
        r = self.client.delete(f"{self.base}/{table}", params=filters)
        r.raise_for_status()
        return r

    def close(self):
        self.client.close()


# ------------------------------------------------------------------
def main() -> int:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        print("ERROR: set SUPABASE_URL and SUPABASE_SERVICE_KEY", file=sys.stderr)
        return 2

    db = Rest(url, key)
    sess = SENTINEL_SESSION.isoformat()
    eq_sess = {"target_session": f"eq.{sess}"}
    rows = build_rows(SENTINEL_SESSION, SCREENER_NAME, COLLECTOR_NAME)

    try:
        db.delete(TABLE, **eq_sess)               # pre-clean

        # -- W01 insert ------------------------------------------
        try:
            db.upsert(TABLE, rows, CONFLICT)
            check("W01 insert 23 rows", "ok", "ok", True)
        except Exception as e:
            check("W01 insert 23 rows", "ok", f"ERROR: {e}", False)
            raise

        # -- W02 read back ---------------------------------------
        data = db.select(TABLE, "symbol,metrics,screened_at,collector",
                         order="symbol", **eq_sess)
        check("W02 row count after insert", 23, len(data), len(data) == 23)

        # -- W03 null beta survived ------------------------------
        agbk = next((r for r in data if r["symbol"] == "AGBK"), None)
        got = agbk["metrics"]["beta"] if agbk else "MISSING"
        check("W03 AGBK beta null after round-trip", None, got,
              agbk is not None and got is None)

        # -- W04 float precision ---------------------------------
        move = next((r for r in data if r["symbol"] == "MOVE"), None)
        rsi = move["metrics"]["rsi"] if move else None
        check("W04 float precision (MOVE rsi)", 56.58, rsi, rsi == 56.58)

        # -- W05 int stayed int ----------------------------------
        vol = move["metrics"]["volume"] if move else None
        check("W05 volume is int not float", "int", type(vol).__name__,
              isinstance(vol, int) and not isinstance(vol, bool))

        # -- W06 timestamptz UTC ---------------------------------
        ts = move["screened_at"] if move else ""
        check("W06 screened_at UTC", "+00:00 or Z", ts[-6:],
              ts.endswith("+00:00") or ts.endswith("Z"))

        # -- W07 upsert idempotent -------------------------------
        db.upsert(TABLE, rows, CONFLICT)
        n = db.count(TABLE, **eq_sess)
        check("W07 re-run does not duplicate", 23, n, n == 23)

        # -- W08 second screener, same symbols -------------------
        rows_b = build_rows(SENTINEL_SESSION, "wire_test_b", "wilbur-akl")
        db.upsert(TABLE, rows_b, CONFLICT)
        n = db.count(TABLE, **eq_sess)
        check("W08 two screeners coexist", 46, n, n == 46)

        # -- W09 constraints reject at API layer -----------------
        bad_cases = [
            ("hyphen symbol",     {"symbol": "BRK-B"}),
            ("lowercase symbol",  {"symbol": "aapl"}),
            ("unknown collector", {"collector": "dave"}),
        ]
        outcomes = []
        for label, override in bad_cases:
            payload = {**rows[0], **override}
            try:
                db.upsert(TABLE, [payload], CONFLICT)
                outcomes.append(f"{label}=ACCEPTED")
            except httpx.HTTPStatusError:
                outcomes.append(f"{label}=rejected")
        ok = all(o.endswith("rejected") for o in outcomes)
        check("W09 bad rows rejected by API", "all rejected",
              "all rejected" if ok else "; ".join(outcomes), ok)

        # -- W10 view readable -----------------------------------
        try:
            view = db.select(VIEW, "*", **eq_sess)
            v = next((x for x in view if x["symbol"] == "MOVE"), None)
            ok = (v is not None and v["screener_count"] == 2
                  and v["cross_confirmed"] is True)
            check("W10 view: MOVE cross-confirmed", "2 screeners, true",
                  f"{v['screener_count']}, {v['cross_confirmed']}" if v else "not found",
                  ok)
        except Exception as e:
            check("W10 view: MOVE cross-confirmed", "2 screeners, true",
                  f"ERROR: {e}", False)

    finally:
        try:
            db.delete(TABLE, **eq_sess)
            n = db.count(TABLE, **eq_sess)
            check("W11 cleanup", 0, n, n == 0)
        except Exception as e:
            check("W11 cleanup", 0, f"ERROR: {e}", False)
        db.close()

    # -- report --------------------------------------------------
    print("\n" + "=" * 78)
    print(f"{'CHECK':<42}{'EXPECTED':<14}{'ACTUAL':<12}{'STATUS'}")
    print("=" * 78)
    for name, exp, act, status in results:
        print(f"{name:<42}{exp[:13]:<14}{act[:11]:<12}{status}")
    print("=" * 78)
    n_fail = sum(1 for r in results if r[3] != "PASS")
    print(f"{len(results) - n_fail}/{len(results)} passed"
          + ("" if n_fail == 0 else f"  --  {n_fail} FAILED"))
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
