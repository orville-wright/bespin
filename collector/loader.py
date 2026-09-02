#!/usr/bin/env python3
"""
loader.py -- shared logic for all Bespin screener loaders.

Not invoked directly. Each screener has a thin wrapper in this directory
that pins its identity and calls run().  See finviz_momentum.py.

Contract:
  stdout  ->  exactly one JSON object, nothing else. Claude parses this.
  stderr  ->  all human-readable progress and diagnostics.
  exit 0  ->  success.  exit 1 -> failure (JSON still emitted, ok=false).

Pipeline:
  1. preflight  parse + validate the CSV, no network
  2. session    resolve target_session via Alpaca calendar
  3. archive    write the CSV aside before touching the database
  4. upsert     push to Supabase via PostgREST

Failing early keeps the error pointed at the CSV rather than at the wire.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

import httpx

# session.py lives at the repo root, one level up from collector/
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from session import SESSION_LOGIC_VERSION, get_target_session  # noqa: E402

# ------------------------------------------------------------------
# Contract -- the CSV shape every screener generator must produce
# ------------------------------------------------------------------
REQUIRED_COLUMNS = [
    "num", "ticker", "beta", "atr",
    "sma20_pct", "sma50_pct", "sma200_pct",
    "high_52w_pct", "low_52w_pct",
    "rsi", "price", "change_pct", "change_from_open_pct", "gap_pct",
    "volume",
]

INT_FIELDS = {"num", "volume"}
KEY_EXCLUDE = ("num", "ticker")

VALID_COLLECTORS = ("wilbur-akl", "orville-sfo")

TABLE = "screened_candidate_targets"
CONFLICT = "symbol,screener_name,target_session"
CHUNK_SIZE = 500

ARCHIVE_DIR = Path(__file__).resolve().parent / "archive"


class LoaderError(Exception):
    """Fatal, with a stage label for the JSON envelope."""

    def __init__(self, stage: str, message: str, detail=None):
        super().__init__(message)
        self.stage = stage
        self.detail = detail or {}


def log(msg: str) -> None:
    """Human output. stderr only -- stdout is reserved for the JSON."""
    print(msg, file=sys.stderr)


# ------------------------------------------------------------------
# 1. Preflight
# ------------------------------------------------------------------
def to_canonical(symbol: str) -> str:
    """Alpaca convention: uppercase, dot separator for share classes."""
    return symbol.strip().upper().replace("-", ".")


def load_csv(path: Path) -> tuple[list[dict], list[str]]:
    """Parse and validate. Returns (rows, warnings). Raises on fatal issues."""
    if not path.exists():
        raise LoaderError("preflight", f"CSV not found: {path}")

    warnings: list[str] = []

    # utf-8-sig strips a BOM if a Windows tool added one.
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        raw = list(reader)

    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing:
        raise LoaderError(
            "preflight",
            f"CSV is missing required columns: {missing}",
            {"header_found": header, "required": REQUIRED_COLUMNS},
        )

    extra = [c for c in header if c not in REQUIRED_COLUMNS]
    if extra:
        warnings.append(f"unrecognised columns carried into metrics: {extra}")

    if not raw:
        raise LoaderError("preflight", "CSV has a header but no data rows")

    rows, seen = [], {}
    for i, r in enumerate(raw, start=2):          # line 1 is the header
        ticker = (r.get("ticker") or "").strip()
        if not ticker:
            raise LoaderError("preflight", f"line {i}: empty ticker")

        symbol = to_canonical(ticker)
        if not symbol.replace(".", "").isalpha():
            raise LoaderError(
                "preflight",
                f"line {i}: ticker {ticker!r} -> {symbol!r} is not canonical "
                f"(expected letters and an optional dot)",
            )
        if symbol in seen:
            raise LoaderError(
                "preflight",
                f"line {i}: duplicate ticker {symbol} (first seen line {seen[symbol]})",
            )
        seen[symbol] = i

        metrics: dict = {}
        for k, v in r.items():
            if k in KEY_EXCLUDE or k is None:
                continue
            metrics[k] = _parse_number(v, k, i)

        rank = _parse_number(r.get("num"), "num", i)
        if rank is None:
            raise LoaderError("preflight", f"line {i}: empty num")

        rows.append({"symbol": symbol, "source_rank": rank, "metrics": metrics})

    log(f"preflight: {len(rows)} rows, {len(seen)} unique symbols, "
        f"{len(warnings)} warning(s)")
    return rows, warnings


def _parse_number(value, field: str, line: int):
    """Empty string -> None (JSON null). Never produces NaN."""
    if value is None:
        return None
    v = value.strip()
    if v == "":
        return None
    try:
        return int(v) if field in INT_FIELDS else float(v)
    except ValueError:
        raise LoaderError(
            "preflight",
            f"line {line}: field {field!r} has non-numeric value {v!r}",
        ) from None


# ------------------------------------------------------------------
# 2. Archive -- written before any database call
# ------------------------------------------------------------------
def archive_csv(src: Path, target_session: date, screener_name: str) -> Path:
    dest_dir = ARCHIVE_DIR / target_session.isoformat()
    dest_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%H%M%S")
    dest = dest_dir / f"{screener_name}_{stamp}.csv"
    shutil.copy2(src, dest)
    log(f"archived: {dest}")
    return dest


# ------------------------------------------------------------------
# 3. PostgREST
# ------------------------------------------------------------------
class Rest:
    """Minimal PostgREST client. No SDK, no websockets, no realtime."""

    def __init__(self, url: str, key: str, timeout: float = 30.0):
        self.base = url.rstrip("/") + "/rest/v1"
        self.client = httpx.Client(
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
            },
            timeout=timeout,
        )

    def upsert(self, table: str, rows: list[dict], on_conflict: str,
               retries: int = 3) -> None:
        for attempt in range(1, retries + 1):
            try:
                r = self.client.post(
                    f"{self.base}/{table}",
                    params={"on_conflict": on_conflict},
                    json=rows,
                    headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
                )
                r.raise_for_status()
                return
            except httpx.HTTPStatusError as e:
                # 4xx means the payload is wrong. Retrying will not help.
                if e.response.status_code < 500:
                    raise LoaderError(
                        "upsert",
                        f"rejected by PostgREST ({e.response.status_code})",
                        {"body": e.response.text[:600]},
                    ) from None
                if attempt == retries:
                    raise LoaderError(
                        "upsert", f"server error after {retries} attempts",
                        {"body": e.response.text[:600]},
                    ) from None
            except (httpx.TimeoutException, httpx.TransportError) as e:
                if attempt == retries:
                    raise LoaderError(
                        "upsert", f"network failure after {retries} attempts: {e}"
                    ) from None
            wait = 2 ** (attempt - 1)
            log(f"  upsert attempt {attempt} failed, retrying in {wait}s")
            time.sleep(wait)

    def close(self) -> None:
        self.client.close()


# ------------------------------------------------------------------
# 4. Orchestration
# ------------------------------------------------------------------
def run(*, screener_name: str, screener_version: str, rationale: str,
        csv_path: Path, dry_run: bool = False) -> dict:
    started = time.monotonic()
    result: dict = {
        "ok": False,
        "screener_name": screener_name,
        "screener_version": screener_version,
        "csv_path": str(csv_path),
        "dry_run": dry_run,
        "warnings": [],
    }

    try:
        # ---- credentials --------------------------------------
        collector = _require("BESPIN_COLLECTOR")
        if collector not in VALID_COLLECTORS:
            raise LoaderError(
                "config",
                f"BESPIN_COLLECTOR={collector!r} is not one of {VALID_COLLECTORS}",
            )
        result["collector"] = collector

        # ---- 1. preflight (no network) ------------------------
        rows, warnings = load_csv(csv_path)
        result["warnings"] = warnings
        result["rows_read"] = len(rows)

        # ---- 2. session ---------------------------------------
        try:
            from alpaca.trading.client import TradingClient
        except ImportError:
            raise LoaderError(
                "config",
                "alpaca-py is not installed. Run: uv sync --only-group collector",
            ) from None

        api_key = _require("ALPACA_API_KEY")
        sec_key = _require("ALPACA_SEC_KEY")
        client = TradingClient(api_key, sec_key, paper=api_key.startswith("PK"))

        now_utc = datetime.now(timezone.utc)
        try:
            target_session = get_target_session(client, now_utc)
        except Exception as e:
            raise LoaderError("session", f"could not resolve target_session: {e}") from None

        result["target_session"] = target_session.isoformat()
        result["screened_at"] = now_utc.isoformat()
        log(f"session: target_session={target_session} "
            f"(now_utc={now_utc.isoformat(timespec='seconds')})")

        # ---- 3. archive ---------------------------------------
        result["archived_to"] = str(archive_csv(csv_path, target_session, screener_name))

        # ---- build payload ------------------------------------
        payload = [
            {
                "symbol": r["symbol"],
                "screener_name": screener_name,
                "screener_version": screener_version,
                "collector": collector,
                "target_session": target_session.isoformat(),
                "rationale": rationale,
                "screened_at": now_utc.isoformat(),
                "metrics": {
                    "source_rank": r["source_rank"],
                    "session_logic": SESSION_LOGIC_VERSION,
                    "bespin_version": os.environ.get("BESPIN_VERSION", "unknown"),
                    **r["metrics"],
                },
            }
            for r in rows
        ]
        result["symbols"] = [r["symbol"] for r in rows]

        # ---- 4. upsert ----------------------------------------
        if dry_run:
            log("dry run: skipping upsert")
            result["rows_upserted"] = 0
        else:
            db = Rest(_require("SUPABASE_URL"), _require("SUPABASE_SERVICE_KEY"))
            try:
                for i in range(0, len(payload), CHUNK_SIZE):
                    db.upsert(TABLE, payload[i:i + CHUNK_SIZE], CONFLICT)
            finally:
                db.close()
            result["rows_upserted"] = len(payload)
            log(f"upserted {len(payload)} rows into {TABLE}")

        result["ok"] = True

    except LoaderError as e:
        result.update(stage=e.stage, error=str(e), detail=e.detail)
        log(f"FAILED [{e.stage}]: {e}")
    except Exception as e:                                    # noqa: BLE001
        result.update(stage="unexpected", error=f"{type(e).__name__}: {e}")
        log(f"FAILED [unexpected]: {type(e).__name__}: {e}")

    result["duration_ms"] = round((time.monotonic() - started) * 1000)
    return result


def _require(name: str) -> str:
    val = os.environ.get(name)
    if not val or not val.strip():
        raise LoaderError("config", f"environment variable {name} is missing or empty")
    return val.strip()


# ------------------------------------------------------------------
# Entry point used by the thin wrappers
# ------------------------------------------------------------------
def main(*, screener_name: str, screener_version: str, rationale: str) -> int:
    parser = argparse.ArgumentParser(
        description=f"Load a {screener_name} CSV into {TABLE}."
    )
    parser.add_argument("csv_path", type=Path, help="path to the scraped CSV")
    parser.add_argument("--dry-run", action="store_true",
                        help="validate and resolve session, but do not upsert")
    parser.add_argument("--env-file", type=Path, default=None,
                        help="explicit .env path (default: search upward)")
    args = parser.parse_args()

    _load_env(args.env_file)

    result = run(
        screener_name=screener_name,
        screener_version=screener_version,
        rationale=rationale,
        csv_path=args.csv_path,
        dry_run=args.dry_run,
    )

    # The one and only thing on stdout.
    print(json.dumps(result, indent=2))
    return 0 if result["ok"] else 1


def _load_env(explicit: Path | None) -> None:
    try:
        from dotenv import find_dotenv, load_dotenv
    except ImportError:
        log("note: python-dotenv not installed; relying on the ambient environment")
        return
    if explicit:
        load_dotenv(explicit)
        log(f"loaded env: {explicit}")
        return
    found = find_dotenv(usecwd=False)          # walks up from this file
    if found:
        load_dotenv(found)
        log(f"loaded env: {found}")
    else:
        log("note: no .env found; relying on the ambient environment")