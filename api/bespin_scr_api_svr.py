#!/usr/bin/env python3
"""
bespin_scr_api_svr.py -- Bespin Screener API server (FastAPI)

Wraps collector/loader.py behind an HTTP API so screener CSVs can be
upserted (or dry-run validated) without shelling out to the CLI wrappers
(e.g. collector/finviz_technical_small.py).

Does not modify collector/loader.py or collector/finviz_technical_small.py --
this module only imports loader.py's public run() function and reuses its
module-level constants (ARCHIVE_DIR, env var names, etc).

Endpoints:
  POST /upsertpath    -- validate + upsert a server-accessible CSV path
  POST /upsertstream  -- validate + upsert a browser-uploaded CSV file
  POST /dryrun        -- same as /upsertpath but with loader's --dry-run behavior
  GET  /status    -- server health/info
  GET  /listarc   -- recursive listing of collector/archive/
  POST /shutdown  -- stop the server process

Usage:
    uv run api/bespin_scr_api_svr.py
    uv run api/bespin_scr_api_svr.py --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import subprocess
import sys
import tempfile
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from pydantic import BaseModel, ConfigDict, Field

# collector/ has no __init__.py -- it's a script directory, not a package --
# so it must be added to sys.path explicitly before `import loader` works.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_COLLECTOR_DIR = _REPO_ROOT / "collector"
if str(_COLLECTOR_DIR) not in sys.path:
    sys.path.insert(0, str(_COLLECTOR_DIR))

import loader  # noqa: E402  -- collector/loader.py, unmodified

SERVER_STARTED_MONOTONIC = time.monotonic()
SERVER_STARTED_AT = datetime.now(timezone.utc)
SERVER_STARTED_AT_LOCAL = datetime.now().astimezone()

DEFAULT_SCREENER_NAME = "finviz_technical_small"
DEFAULT_SCREENER_VERSION = "v1"
DEFAULT_RATIONALE = "Small-cap technical screen, price >$5, volume >10x avg"

_UPSERT_COUNTER_LOCK = threading.Lock()
UPSERT_TRANSACTION_COUNTERS = {
    "attempts": 0,
    "path_failures": 0,
    "stream_failures": 0,
    "path_success": 0,
    "stream_success": 0,
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Same env resolution loader.py's CLI entry point uses: walk upward from
    # loader.py's own location looking for a .env file.
    loader._load_env(None)
    _reset_upsert_transaction_counters()
    hostname, _local_ip = get_host_info()
    print(f"INFO:     Local hostname: {hostname}")
    print(f"INFO:     Start time: {SERVER_STARTED_AT_LOCAL.isoformat()}")
    yield


app = FastAPI(
    title="Bespin Candidate Screener API",
    description="HTTP front-end for collector/loader.py screener upserts.",
    version="1.0.0",
    lifespan=lifespan,
)


class ScreenerRequest(BaseModel):
    """Shared parameters for path-based API requests."""

    model_config = ConfigDict(extra="forbid")

    csv_path: str = Field(
        ...,
        description="Full path to the scraped screener CSV file.",
        examples=[r"C:\Users\dbrace\code\bespin\collector\archive\2026-09-03\finviz_technical_small_143000.csv"],
    )
    screener_name: str = Field(
        DEFAULT_SCREENER_NAME,
        description="Screener identity, e.g. 'finviz_technical_small'.",
        examples=["finviz_technical_small"],
    )
    screener_version: str = Field(
        DEFAULT_SCREENER_VERSION,
        description="Screener version tag, e.g. 'v1'.",
        examples=["v1"],
    )
    rationale: str = Field(
        DEFAULT_RATIONALE,
        description="Human-readable rationale for the screen.",
        examples=["Small-cap technical screen, price >$5, volume >10x avg"],
    )


def _run_loader(req: ScreenerRequest, *, dry_run: bool, source_mode: str | None = None) -> dict:
    try:
        result = loader.run(
            screener_name=req.screener_name,
            screener_version=req.screener_version,
            rationale=req.rationale,
            csv_path=Path(req.csv_path),
            dry_run=dry_run,
        )
    except Exception:
        if source_mode:
            _record_upsert_transaction(source_mode, False)
        raise
    # Mirrors loader.py's CLI contract: exactly one JSON object printed.
    print( f"\nINFO:     Supabase UPSERT Data Package:{json.dumps(result, indent=2)}" )
    if source_mode:
        _record_upsert_transaction(source_mode, bool(result.get("ok")))
    return result


@app.post("/upsertpath")
def upsertpath(req: ScreenerRequest) -> dict:
    """Validate a server-accessible CSV path, archive it, and upsert to Supabase."""
    return _run_loader(req, dry_run=False, source_mode="path")


@app.post("/upsertstream")
async def upsertstream(
    request: Request,
    csv_file: UploadFile = File(..., description="Browser-uploaded CSV file."),
    screener_name: str = Form(DEFAULT_SCREENER_NAME),
    screener_version: str = Form(DEFAULT_SCREENER_VERSION),
    rationale: str = Form(DEFAULT_RATIONALE),
) -> dict:
    """Validate a multipart CSV upload, archive it, and upsert to Supabase."""
    temp_path: Path | None = None
    try:
        await _validate_stream_form(request)
        filename = csv_file.filename or "uploaded.csv"
        csv_bytes = await csv_file.read()
        if not csv_bytes:
            raise HTTPException(status_code=400, detail="uploaded CSV file is empty")
        req = ScreenerRequest(
            csv_path="",
            screener_name=screener_name,
            screener_version=screener_version,
            rationale=rationale,
        )
        suffix = Path(filename).suffix or ".csv"
        with tempfile.NamedTemporaryFile(
            mode="wb",
            delete=False,
            prefix="bespin_upsert_",
            suffix=suffix,
        ) as tmp:
            tmp.write(csv_bytes)
            temp_path = Path(tmp.name)
        req.csv_path = str(temp_path)
        return _run_loader(req, dry_run=False, source_mode="stream")
    except HTTPException:
        _record_upsert_transaction("stream", False)
        raise
    finally:
        await csv_file.close()
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


@app.post("/dryrun")
def dryrun(req: ScreenerRequest) -> dict:
    """Same as /upsertpath but skips the Supabase write (loader.py --dry-run)."""
    return _run_loader(req, dry_run=True)


@app.get("/status")
def status() -> dict:
    """Current server status/info."""
    
    hostname, local_ip = get_host_info()

    return {
        "status": "running",
        "server": "bespin_scr_api_svr",
        "hostname": hostname,
        "local_IP": local_ip,
        "pid": os.getpid(),
        "started_at": SERVER_STARTED_AT.isoformat(),
        "uptime_seconds": round(time.monotonic() - SERVER_STARTED_MONOTONIC, 1),
        "now_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.system(),
        "platform_release": platform.release(),
        "python_version": platform.python_version(),
        "archive_dir": str(loader.ARCHIVE_DIR),
        "archive_dir_exists": loader.ARCHIVE_DIR.exists(),
        "collector_env": os.environ.get(loader.ENV_COLLECTOR),
        "bespin_version": os.environ.get(loader.ENV_BESPIN_VERSION, "unknown"),
        "upsert_transactions": _upsert_transaction_status(),
    }


@app.get("/listarc")
def listarc() -> dict:
    """Recursively list every file under collector/archive/."""
    archive_dir = loader.ARCHIVE_DIR
    if not archive_dir.exists():
        raise HTTPException(
            status_code=404,
            detail=f"archive directory not found: {archive_dir}",
        )

    system = platform.system()
    if system == "Windows":
        cmd = ["cmd", "/c", "dir", "/A-D", "/S", "/B"]
    else:
        cmd = ["find", ".", "-type", "f"]

    proc = subprocess.run(
        cmd,
        cwd=str(archive_dir),
        capture_output=True,
        text=True,
        timeout=30,
    )
    if proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=f"listing command failed (exit {proc.returncode}): {proc.stderr.strip()}",
        )

    files = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
    return {
        "archive_dir": str(archive_dir),
        "platform": system,
        "count": len(files),
        "files": files,
    }


@app.post("/shutdown")
def shutdown() -> dict:
    """Stop and exit the FastAPI server."""

    def _delayed_exit() -> None:
        # Give the HTTP response a moment to flush before the process dies.
        time.sleep(0.5)
        os._exit(0)

    threading.Thread(target=_delayed_exit, daemon=True).start()
    return {"message": "Bespin Screener API server is shutting down", "pid": os.getpid()}

def get_host_info():
    hostname = socket.gethostname()
    try:
        # Doesn't actually open a connection, just picks the outbound-facing interface
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        local_ip = socket.gethostbyname(hostname)
    return hostname, local_ip


async def _validate_stream_form(request: Request) -> None:
    form = await request.form()
    allowed_fields = {"csv_file", "screener_name", "screener_version", "rationale"}
    field_names = {name for name, _value in form.multi_items()}
    unknown_fields = sorted(field_names - allowed_fields)
    if "csv_path" in field_names:
        raise HTTPException(
            status_code=400,
            detail="/upsertstream accepts a CSV file upload only; do not include csv_path",
        )
    if unknown_fields:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported multipart form field(s): {unknown_fields}",
        )

    files = [
        value
        for _name, value in form.multi_items()
        if hasattr(value, "filename") and hasattr(value, "read")
    ]
    if len(files) != 1:
        raise HTTPException(
            status_code=400,
            detail=f"/upsertstream requires exactly one uploaded CSV file; received {len(files)}",
        )


def _record_upsert_transaction(source_mode: str, ok: bool) -> None:
    with _UPSERT_COUNTER_LOCK:
        UPSERT_TRANSACTION_COUNTERS["attempts"] += 1
        if source_mode == "path":
            key = "path_success" if ok else "path_failures"
        elif source_mode == "stream":
            key = "stream_success" if ok else "stream_failures"
        else:
            raise ValueError(f"unsupported upsert source mode: {source_mode}")
        UPSERT_TRANSACTION_COUNTERS[key] += 1
        snapshot = UPSERT_TRANSACTION_COUNTERS.copy()
    _log_upsert_transaction_status(snapshot)


def _reset_upsert_transaction_counters() -> None:
    with _UPSERT_COUNTER_LOCK:
        for key in UPSERT_TRANSACTION_COUNTERS:
            UPSERT_TRANSACTION_COUNTERS[key] = 0


def _upsert_transaction_status() -> dict:
    with _UPSERT_COUNTER_LOCK:
        snapshot = UPSERT_TRANSACTION_COUNTERS.copy()
    return {
        "attempts": snapshot["attempts"],
        "failures": {
            "path": snapshot["path_failures"],
            "stream": snapshot["stream_failures"],
        },
        "success": {
            "path": snapshot["path_success"],
            "stream": snapshot["stream_success"],
        },
    }


def _log_upsert_transaction_status(snapshot: dict) -> None:
    print(
        "INFO:     Transaction attemtps: "
        f"{snapshot['attempts']} / "
        "Failures "
        f"[Path: {snapshot['path_failures']} / Stream: {snapshot['stream_failures']}] / "
        "Success: "
        f"[Path: {snapshot['path_success']} / Stream: {snapshot['stream_success']}]"
    )

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bespin Screener API server")
    parser.add_argument("--host", default="127.0.0.1", help="bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="bind port (default: 8000)")
    return parser.parse_args()


if __name__ == "__main__":
    import uvicorn

    args = _parse_args()
    uvicorn.run(app, host=args.host, port=args.port)
