"""Roster loading.

Keeps roster-sheet reading separate from the Streamlit page.
"""

from __future__ import annotations

from typing import List

import streamlit as st

from ..config import ROSTER_NAME_COLUMN_HEADER, ROSTER_SHEET
from ..integrations.gspread_io import open_spreadsheet, with_backoff


def _pick_name_col(headers: list[str]) -> int:
    """Return the 0-based index of the likely name column."""
    target = (ROSTER_NAME_COLUMN_HEADER or "").strip().lower()
    norm = [(h or "").strip().lower() for h in headers]

    # Exact header match
    if target and target in norm:
        return norm.index(target)

    # Substring header match (e.g., "Name (OAs/GOAs)")
    if target:
        for i, h in enumerate(norm):
            if target in h:
                return i

    # Common fallbacks
    for cand in ["name", "oa name", "employee name", "full name", "student name", "oa/goa name", "oas/goas"]:
        # exact
        if cand in norm:
            return norm.index(cand)
        # substring
        for i, h in enumerate(norm):
            if cand in h:
                return i

    return 0


@st.cache_data(show_spinner=False)
def load_roster(sheet_url: str) -> List[str]:
    """Load roster names from the roster worksheet.

    Primary path: `get_all_records()` using `ROSTER_NAME_COLUMN_HEADER`.
    Fallback: read values and take the best "name" column if headers differ.
    """
    ss = open_spreadsheet(sheet_url)

    try:
        ws = with_backoff(ss.worksheet, ROSTER_SHEET)
    except Exception:
        return []

    out: List[str] = []

    # --- Primary path (fast, structured) ---
    try:
        values = with_backoff(ws.get_all_records)
        for row in values or []:
            name = row.get(ROSTER_NAME_COLUMN_HEADER, "")
            if isinstance(name, str):
                name = name.strip()
                if name:
                    out.append(name)
        if out:
            return out
    except Exception:
        pass

    # --- Fallback path (robust) ---
    try:
        grid = with_backoff(ws.get_all_values)
    except Exception:
        return []

    if not grid:
        return []

    headers = grid[0] if grid else []
    col = _pick_name_col(headers)

    for r in grid[1:]:
        if col < len(r):
            name = (r[col] or "").strip()
            if name:
                out.append(name)

    return out
