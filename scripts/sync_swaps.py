"""CLI helper: render swap sections from Supabase into Google Sheets.

This is intentionally tiny so it can be used later with Cloud Run / cron.

Env vars supported:
  - SHEET_URL
  - SUPABASE_URL
  - SUPABASE_KEY
  - GCP_SERVICE_ACCOUNT_JSON (path to service account json)

Example:
  export SHEET_URL=...
  export SUPABASE_URL=...
  export SUPABASE_KEY=...
  export GCP_SERVICE_ACCOUNT_JSON=/path/to/sa.json
  python scripts/sync_swaps.py
"""

from __future__ import annotations

import json
import os
import sys

import gspread
from google.oauth2.service_account import Credentials
from supabase import create_client


def _die(msg: str) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(2)


def main() -> None:
    sheet_url = os.getenv("SHEET_URL")
    sb_url = os.getenv("SUPABASE_URL")
    sb_key = os.getenv("SUPABASE_KEY")
    sa_path = os.getenv("GCP_SERVICE_ACCOUNT_JSON")

    if not sheet_url:
        _die("Missing SHEET_URL env var")
    if not (sb_url and sb_key):
        _die("Missing SUPABASE_URL / SUPABASE_KEY env vars")
    if not sa_path:
        _die("Missing GCP_SERVICE_ACCOUNT_JSON env var")
    if not os.path.exists(sa_path):
        _die(f"Service account json not found: {sa_path}")

    with open(sa_path, "r", encoding="utf-8") as f:
        creds_dict = json.load(f)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(credentials)
    ss = gc.open_by_url(sheet_url)

    sb = create_client(sb_url, sb_key)

    # Local import so running this file doesn't require Streamlit.
    from oa_app.jobs.sync_swaps_to_sheets import sync_swaps_to_sheets

    res = sync_swaps_to_sheets(ss, sb)
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
