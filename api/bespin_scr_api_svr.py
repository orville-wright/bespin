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
  POST /upsert    -- validate + upsert a screener CSV into Supabase
  POST /dryrun    -- same as /upsert but with loader's --dry-run behavior
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
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# collector/ has no __init__.py -- it's a script directory, not a package --
# so it must be added to sys.path explicitly before `import loader` works.
_REPO_ROOT = Path(__file__).resolve().parents[1]
_COLLECTOR_DIR = _REPO_ROOT / "collector"
if str(_COLLECTOR_DIR) not in sys.path:
    sys.path.insert(0, str(_COLLECTOR_DIR))

import loader  # noqa: E402  -- collector/loader.py, unmodified

SERVER_STARTED_MONOTONIC = time.monotonic()
SERVER_STARTED_AT = datetime.now(timezone.utc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Same env resolution loader.py's CLI entry point uses: walk upward from
    # loader.py's own location looking for a .env file.
    loader._load_env(None)
    yield


app = FastAPI(
    title="Bespin Candidate Screener API",
    description="HTTP front-end for collector/loader.py screener upserts.",
    version="1.0.0",
    lifespan=lifespan,
)


class ScreenerRequest(BaseModel):
    """Shared parameters for /upsert and /dryrun."""

    csv_path: str = Field(
        ...,
        description="Full path to the scraped screener CSV file.",
        examples=[r"C:\Users\dbrace\code\bespin\collector\archive\2026-09-03\finviz_technical_small_143000.csv"],
    )
    screener_name: str = Field(
        ...,
        description="Screener identity, e.g. 'finviz_technical_small'.",
        examples=["finviz_technical_small"],
    )
    screener_version: str = Field(
        ...,
        description="Screener version tag, e.g. 'v1'.",
        examples=["v1"],
    )
    rationale: str = Field(
        ...,
        description="Human-readable rationale for the screen.",
        examples=["Small-cap technical screen, price >$5, volume >10x avg"],
    )


def _run_loader(req: ScreenerRequest, *, dry_run: bool) -> dict:
    result = loader.run(
        screener_name=req.screener_name,
        screener_version=req.screener_version,
        rationale=req.rationale,
        csv_path=Path(req.csv_path),
        dry_run=dry_run,
    )
    # Mirrors loader.py's CLI contract: exactly one JSON object printed.
    print(json.dumps(result, indent=2))
    return result


@app.post("/upsert")
def upsert(req: ScreenerRequest) -> dict:
    """Validate the CSV, resolve the session, archive it, and upsert to Supabase."""
    return _run_loader(req, dry_run=False)


@app.post("/dryrun")
def dryrun(req: ScreenerRequest) -> dict:
    """Same as /upsert but skips the Supabase write (loader.py --dry-run)."""
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

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bespin Screener API server")
    parser.add_argument("--host", default="127.0.0.1", help="bind host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="bind port (default: 8000)")
    return parser.parse_args()


if __name__ == "__main__":
    import uvicorn

    args = _parse_args()
    uvicorn.run(app, host=args.host, port=args.port)
