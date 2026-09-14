"""
Paste-into-interpreter smoke test: collector/loader.py's activate_screener() / load_csv()
/ run() contract -- the exact seam that broke when loader.py grew multi-screener support
(activate_screener() mutating a module-level `s1` dict) while api/bespin_scr_api_svr.py's
_run_loader() kept calling loader.run() without a `collector` kwarg and without ever
calling activate_screener() first.

No network, no Supabase, no Alpaca calendar -- this only exercises preflight (parsing +
screener-config resolution), which is where the break was and is fully offline.

Run: uv run docs/loader_screener_contract_smoke_test.py
"""

import inspect
import io
import sys
import tempfile
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_COLLECTOR_DIR = _REPO_ROOT / "collector"
if str(_COLLECTOR_DIR) not in sys.path:
    sys.path.insert(0, str(_COLLECTOR_DIR))

import loader  # noqa: E402

results = []


def check(name, expected, actual, passed):
    results.append((name, str(expected), str(actual), "PASS" if passed else "*** FAIL ***"))


# ------------------------------------------------------------------
# 1. run()'s signature must not require a caller-supplied `collector` --
#    that mismatch is exactly what made every /upsertpath, /upsertstream,
#    and /dryrun request raise TypeError.
# ------------------------------------------------------------------
run_params = inspect.signature(loader.run).parameters
check("R01 run() has no 'collector' param", "absent", "present" if "collector" in run_params else "absent",
      "collector" not in run_params)
check("R02 run() still takes screener_name", "present", "present" if "screener_name" in run_params else "absent",
      "screener_name" in run_params)

# ------------------------------------------------------------------
# 2. activate_screener() returns a fresh dict, not a name for a shared global --
#    two different screeners resolved back-to-back must not cross-contaminate.
# ------------------------------------------------------------------
cfg_a = loader.activate_screener("fvz_test_scr_1")
cfg_b = loader.activate_screener("tdv-1_dtechs_100m_2b_up10pct")

check("S01 activate_screener returns a dict", "dict", type(cfg_a).__name__, isinstance(cfg_a, dict))
check("S02 screener A collector", "orville-sfo", cfg_a.get("collector"), cfg_a.get("collector") == "orville-sfo")
check("S03 screener B collector", "ai-orville-sfo", cfg_b.get("collector"), cfg_b.get("collector") == "ai-orville-sfo")
check("S04 resolving B did not mutate A", "orville-sfo", cfg_a.get("collector"), cfg_a.get("collector") == "orville-sfo")
check("S05 unrecognised screener name doesn't raise", "dict", type(loader.activate_screener("no-such-screener")).__name__,
      isinstance(loader.activate_screener("no-such-screener"), dict))

no_s1_global = not hasattr(loader, "s1")
check("S06 module-level 's1' global removed", "removed", "removed" if no_s1_global else "still present", no_s1_global)

# ------------------------------------------------------------------
# 3. load_csv() takes the screener config explicitly and validates
#    against *that* config's columns, not a shared global's.
# ------------------------------------------------------------------
CSV = """num,symbol,beta,atr,sma20_pct,sma50_pct,sma200_pct,high_52w_pct,low_52w_pct,rsi,price,change_pct,change_from_open_pct,gap_pct,volume
1,NVAX,2.45,0.52,18.47,19.22,19.01,-15.46,63.23,71.68,10.12,8.00,10.00,-1.81,10040575
2,move-a,,1.79,18.94,-6.99,5.12,-52.06,272.50,56.58,12.81,16.99,26.96,-7.85,810443
"""

with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as tmp:
    tmp.write(CSV)
    tmp_path = Path(tmp.name)

try:
    rows, warnings = loader.load_csv(tmp_path, cfg_a)
    check("L01 rows parsed", 2, len(rows), len(rows) == 2)
    check("L02 no warnings for exact column match", 0, len(warnings), len(warnings) == 0)
    check("L03 symbol canonicalised", "MOVE.A", rows[1]["symbol"], rows[1]["symbol"] == "MOVE.A")
    check("L04 empty beta -> None", None, rows[1]["metrics"].get("beta"), rows[1]["metrics"].get("beta") is None)

    try:
        loader.load_csv(tmp_path, cfg_b)
        check("L05 wrong screener config rejects mismatched columns", "LoaderError raised", "no error raised", False)
    except loader.LoaderError as e:
        check("L05 wrong screener config rejects mismatched columns", "LoaderError raised", "LoaderError raised",
              e.stage == "preflight")
finally:
    tmp_path.unlink(missing_ok=True)

# ------------------------------------------------------------------
# 4. Report
# ------------------------------------------------------------------
print("\n" + "=" * 78)
print(f"{'CHECK':<52}{'EXPECTED':<18}{'STATUS'}")
print("=" * 78)
for name, exp, act, status in results:
    print(f"{name:<52}{exp[:17]:<18}{status}  (got: {act})")
print("=" * 78)
n_fail = sum(1 for r in results if r[3] != "PASS")
print(f"{len(results) - n_fail}/{len(results)} passed" + ("" if n_fail == 0 else f"  --  {n_fail} FAILED"))

if n_fail:
    sys.exit(1)
