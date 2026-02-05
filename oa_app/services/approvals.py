"""Approval queue.

Primary storage:
  - Supabase (Postgres) when configured (preferred for concurrency)

Fallback storage:
  - A dedicated worksheet (legacy mode)

Only approvers (e.g., Vraj / Kat) should be able to view/approve/reject.
"""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import gspread
import streamlit as st

from ..config import APPROVAL_SHEET
from ..core.quotas import bump_ws_version
from ..integrations.gspread_io import with_backoff
from ..integrations.supabase_io import get_supabase, supabase_enabled, with_retry


_HEADERS = [[
    "ID",
    "Created",
    "Requester",
    "Action",
    "Campus",
    "Day",
    "Start",
    "End",
    "Details",
    "Status",
    "ReviewedBy",
    "ReviewedAt",
    "ReviewNote",
]]


def _use_db() -> bool:
    # Allow forcing legacy Sheets mode if needed.
    if str(st.secrets.get("USE_SHEETS_APPROVALS", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    return supabase_enabled()


def ensure_approval_sheet(ss: gspread.Spreadsheet) -> gspread.Worksheet:
    """Get the approval sheet, creating it if missing."""
    try:
        return with_backoff(ss.worksheet, APPROVAL_SHEET)
    except gspread.WorksheetNotFound:
        ws = with_backoff(ss.add_worksheet, title=APPROVAL_SHEET, rows=2000, cols=20)
        with_backoff(ws.update, range_name="A1:M1", values=_HEADERS)
        return ws


def submit_request(
    ss: gspread.Spreadsheet,
    *,
    requester: str,
    action: str,
    campus: str,
    day: str,
    start: str,
    end: str,
    details: str,
) -> str:
    """Append a PENDING approval request. Returns request id."""
    rid = uuid4().hex[:10]
    now = datetime.now().isoformat(timespec="seconds")

    if _use_db():
        sb = get_supabase()
        payload = {
            "id": rid,
            "created_at": now,          # timestamptz
            "requester": requester,
            "action": action,
            "campus": campus,
            "day": day,
            "start_time": start,
            "end_time": end,
            "details": details,
            "status": "PENDING",
            "reviewed_by": None,
            "reviewed_at": None,
            "review_note": "",
            "error_message": "",
        }

        # Table name: approvals
        with_retry(lambda: sb.table("approvals").insert(payload).execute())
        return rid

    # Legacy Sheets
    ws = ensure_approval_sheet(ss)
    with_backoff(
        ws.append_row,
        [rid, now, requester, action, campus, day, start, end, details, "PENDING", "", "", ""],
        value_input_option="RAW",
    )
    bump_ws_version(ws)
    return rid


def read_requests(
    ss: gspread.Spreadsheet,
    *,
    max_rows: int = 500,
) -> list[dict]:
    """Read the approval table (best-effort, returns a list of dict rows).

    Each dict includes `_row` (1-based row index in the sheet).
    """
    if _use_db():
        sb = get_supabase()
        # newest first
        resp = with_retry(lambda: sb.table("approvals").select("*").order("created_at", desc=True).limit(int(max_rows)).execute())
        data = getattr(resp, "data", None) or []
        out: list[dict] = []
        for row in data:
            out.append(
                {
                    "ID": row.get("id", ""),
                    
                    "Requester": row.get("requester", ""),
                    "Action": row.get("action", ""),
                    "Campus": row.get("campus", ""),
                    "Day": row.get("day", ""),
                    "Created": row.get("created_at", ""),
                    "Start": row.get("start_time", ""),
                    "End": row.get("end_time", ""),
                    "Details": row.get("details", ""),
                    "Status": row.get("status", ""),
                    "ReviewedBy": row.get("reviewed_by", ""),
                    "ReviewedAt": row.get("reviewed_at", ""),
                    "ReviewNote": row.get("review_note", ""),
                    "ErrorMessage": row.get("error_message", ""),
                    "_row": 0,
                }
            )
        return out

    # Legacy Sheets
    ws = ensure_approval_sheet(ss)
    values = with_backoff(ws.get, f"A1:M{max_rows}") or []
    if not values or len(values) < 2:
        return []
    headers = [str(h).strip() for h in values[0]]
    out: list[dict] = []
    for i, row in enumerate(values[1:], start=2):
        if not any(str(x).strip() for x in row):
            continue
        d = {headers[j]: (row[j] if j < len(row) else "") for j in range(len(headers))}
        d["_row"] = i
        out.append(d)
    return out


def set_status(
    ss: gspread.Spreadsheet,
    *,
    row: int = 0,
    req_id: str = "",
    status: str,
    reviewed_by: str,
    note: str = "",
    error_message: str = "",
) -> None:
    """Update status/reviewer columns for a request.

    - Supabase mode: uses req_id
    - Sheets mode: uses row
    """
    now = datetime.now().isoformat(timespec="seconds")

    if _use_db():
        if not req_id:
            raise ValueError("set_status requires req_id in Supabase mode")
        sb = get_supabase()
        payload = {
            "status": status,
            "reviewed_by": reviewed_by,
            "reviewed_at": now,
            "review_note": note,
            "error_message": error_message,
        }
        with_retry(lambda: sb.table("approvals").update(payload).eq("id", req_id).execute())
        return

    # Sheets
    if not row:
        raise ValueError("set_status requires row in Sheets mode")
    ws = ensure_approval_sheet(ss)
    with_backoff(ws.update, range_name=f"J{row}:M{row}", values=[[status, reviewed_by, now, note]])
    bump_ws_version(ws)
