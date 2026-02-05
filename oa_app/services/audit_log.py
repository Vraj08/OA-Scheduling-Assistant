"""Audit logging.

Primary storage:
  - Supabase (Postgres) when configured (preferred)

Fallback storage:
  - Google Sheets worksheet
"""

from __future__ import annotations

from datetime import datetime

import gspread
import streamlit as st

from ..config import AUDIT_SHEET
from ..integrations.gspread_io import with_backoff
from ..integrations.supabase_io import get_supabase, supabase_enabled, with_retry

_HEADERS = [["Timestamp", "Actor", "Action", "Campus", "Day", "Start", "End", "Details"]]


def _use_db() -> bool:
    if str(st.secrets.get("USE_SHEETS_AUDIT", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    return supabase_enabled()


def ensure_audit_sheet(ss: gspread.Spreadsheet) -> gspread.Worksheet:
    """Get the audit sheet, creating it if missing."""
    try:
        return with_backoff(ss.worksheet, AUDIT_SHEET)
    except gspread.WorksheetNotFound:
        ws = with_backoff(ss.add_worksheet, title=AUDIT_SHEET, rows=2000, cols=10)
        with_backoff(ws.update, range_name="A1:H1", values=_HEADERS)
        return ws


def append_audit(
    ss: gspread.Spreadsheet,
    *,
    actor: str,
    action: str,
    campus: str,
    day: str,
    start: str,
    end: str,
    details: str,
) -> None:
    """Append an audit log row. Best-effort; let caller catch exceptions."""
    ts = datetime.now().isoformat(timespec="seconds")

    if _use_db():
        sb = get_supabase()
        payload = {
            "at": ts,                   # timestamptz
            "actor": actor,
            "action": action,
            "campus": campus,
            "day": day,
            "start_time": start,
            "end_time": end,
            "details": details,
        }

        with_retry(lambda: sb.table("audit_log").insert(payload).execute())
        return

    ws = ensure_audit_sheet(ss)
    with_backoff(ws.append_row, [ts, actor, action, campus, day, start, end, details], value_input_option="RAW")
