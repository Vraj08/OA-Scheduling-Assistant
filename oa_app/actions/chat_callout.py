# oa_app/chat_callout.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional
import re
import gspread
import streamlit as st

from ..core.utils import fmt_time
from .chat_add import (
    _ensure_dt,
    _is_half_hour_boundary_dt,
    _range_to_slots,
    _is_blankish,
    _day_cols_from_first_row,
    _header_day_cols,
    _find_day_col_anywhere,
    _find_day_col_fuzzy,
)
from ..ui.schedule_query import _read_grid, _RANGE_RE, _parse_time_cell, _TIME_CELL_RE
from ..core.quotas import bump_ws_version, seed_batch_get_cache
from ..config import ONCALL_MAX_COLS, ONCALL_MAX_ROWS

# Simple color helpers
_ORANGE = {"red": 1.0, "green": 0.65, "blue": 0.0}
_RED = {"red": 0.95, "green": 0.25, "blue": 0.25}


def _campus_kind(title: str) -> str:
    tl = (title or "").lower()
    if "call" in tl:
        return "ONCALL"
    if "mc" in tl or "main" in tl:
        return "MC"
    return "UNH"


def _find_day_col(grid: list[list[str]], day_canon: str) -> int | None:
    day_cols = _day_cols_from_first_row(grid)
    if day_canon not in day_cols:
        hdr = _header_day_cols(grid)
        for k, v in hdr.items():
            day_cols.setdefault(k, v)
    if day_canon not in day_cols:
        g = _find_day_col_anywhere(grid, day_canon)
        if g is not None:
            day_cols[day_canon] = g
    if day_canon not in day_cols:
        g2 = _find_day_col_fuzzy(grid, day_canon)
        if g2 is not None:
            day_cols[day_canon] = g2
    return day_cols.get(day_canon)


def _format_cells(ws: gspread.Worksheet, coords: list[tuple[int, int]], rgb: dict) -> None:
    """Apply background color to multiple cells in one batch_update. coords are 0-based."""
    if not coords:
        return
    requests = []
    for (r0, c0) in coords:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": r0,
                        "endRowIndex": r0 + 1,
                        "startColumnIndex": c0,
                        "endColumnIndex": c0 + 1,
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": rgb}},
                    "fields": "userEnteredFormat.backgroundColor",
                }
            }
        )
    ws.spreadsheet.batch_update({"requests": requests})


_SHIFT_SWAPS_RE = re.compile(r"^\s*shift\s*swaps?(?:\s*for\s*the\s*week)?\s*$", re.I)


def _find_shift_swaps_col(grid: list[list[str]]) -> tuple[int, int] | None:
    """Return (header_row, col) for the 'Shift Swaps for the week' column."""
    if not grid:
        return None
    R = min(len(grid), 80)
    C = min(max((len(r) for r in grid[:R]), default=0), 80)
    for r in range(R):
        row = grid[r]
        for c in range(C):
            v = (row[c] if c < len(row) else "") or ""
            if v and _SHIFT_SWAPS_RE.match(str(v).strip()):
                return (r, c)
    return None


def _parse_12h_time(s: str) -> datetime:
    """Parse 12h time strings like '7:00 AM', '7 AM', '7:00AM' into a datetime (dummy date)."""
    t = (s or "").strip().upper().replace(".", "")
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"(\d)(AM|PM)$", r"\1 \2", t)
    t = re.sub(r"(\d:\d\d)(AM|PM)$", r"\1 \2", t)
    if re.fullmatch(r"\d{1,2} (AM|PM)", t):
        t = t.replace(" ", ":00 ")
    return datetime.strptime(t, "%I:%M %p")


def _fmt_swap_timerange(sdt: datetime, edt: datetime) -> str:
    """Format like '1-3:30 pm' (or '11:30 am-12:30 pm')."""
    def _parts(dt: datetime) -> tuple[str, str]:
        hour = dt.strftime("%I").lstrip("0") or "0"
        minute = dt.strftime("%M")
        mm = "" if minute == "00" else f":{minute}"
        mer = dt.strftime("%p").lower()
        return f"{hour}{mm}", mer

    s_txt, s_mer = _parts(sdt)
    e_txt, e_mer = _parts(edt)
    if s_mer == e_mer:
        return f"{s_txt}-{e_txt} {e_mer}"
    return f"{s_txt} {s_mer}-{e_txt} {e_mer}"


def _extract_mmdd_for_day_col(grid: list[list[str]], col: int) -> str | None:
    """Try to extract an mm/dd date from cells near the weekday header column."""
    if not grid:
        return None
    mmdd_re = re.compile(r"\b(\d{1,2})/(\d{1,2})\b")

    # Look near the top of that column for something like "Tue 10/21" or "10/21"
    for r in range(min(25, len(grid))):
        v = (grid[r][col] if col < len(grid[r]) else "") or ""
        m = mmdd_re.search(str(v))
        if m:
            return f"{int(m.group(1))}/{int(m.group(2))}"

    # Fallback: try dateutil parse if installed
    try:
        from dateutil import parser as dateparser
    except Exception:
        return None

    for r in range(min(25, len(grid))):
        v = (grid[r][col] if col < len(grid[r]) else "") or ""
        s = str(v).strip()
        if not s:
            continue
        try:
            dt = dateparser.parse(s, fuzzy=True, default=datetime(2020, 1, 1))
            if dt:
                return f"{dt.month}/{dt.day}"
        except Exception:
            continue

    return None


def handle_callout(
    st, ss, schedule, *,
    canon_target_name: str,
    campus_title: str,
    day: str,
    start, end,
    covered_by: Optional[str] = None,
) -> str:
    """
    Mark a Call-Out:
      • If covered_by is None/empty → color cell RED.
      • If covered_by is provided → color cell ORANGE.

    Note: Swap section logging ("Shift Swaps for the week" / "Future Swaps/Call outs")
    is now rendered idempotently from Supabase via oa_app.jobs.sync_swaps_to_sheets.
    This function only marks the schedule grid cells.
    Works for UNH/MC (half-hour lanes) and On-Call (fixed blocks).
    """
    dbg_log: list[str] = []

    def dbg(x):  # keep for internal use if needed
        dbg_log.append(str(x))

    def fail(msg: str):
        # Keep callout errors clean (no debug spam unless you want to add a flag later)
        raise ValueError(msg)

    kind = _campus_kind(campus_title)
    covered_by = (covered_by or "").strip() or None

    try:
        ws = ss.worksheet(campus_title)
    except Exception as e:
        fail(f"Could not open worksheet '{campus_title}': {e}")

    sdt = _ensure_dt(start)
    edt = _ensure_dt(end, ref_date=sdt.date())

    if edt <= sdt:
        if 0 <= edt.time().hour <= 5:
            edt = edt + timedelta(days=1)
        else:
            fail("End time must be after start time.")
    if not (_is_half_hour_boundary_dt(sdt) and _is_half_hour_boundary_dt(edt)):
        fail("Times must be on 30-minute boundaries (:00 or :30).")

    grid = _read_grid(ws)
    if not grid:
        fail("Empty sheet.")

    day_canon = (day or "").strip().lower()
    c0 = _find_day_col(grid, day_canon)
    if c0 is None:
        fail(f"Could not map weekday '{day_canon.title()}' to a column in '{ws.title}'.")

    # Find the exact lane cell(s) containing this OA in the requested window
    target_coords: list[tuple[int, int]] = []  # (row_idx_0, col_idx_0)

    if kind == "ONCALL":
        want_s = sdt.strftime("%I:%M %p")
        want_e = edt.strftime("%I:%M %p")

        label_rows: list[int] = []
        for r in range(len(grid)):
            v = (grid[r][c0] if c0 < len(grid[r]) else "") or ""
            if _RANGE_RE.match(v or ""):
                label_rows.append(r)

        r_label = None
        r_next = len(grid)
        for i, r in enumerate(label_rows):
            v = (grid[r][c0] if c0 < len(grid[r]) else "") or ""
            m = _RANGE_RE.match(v or "")
            if not m:
                continue
            s_raw, e_raw = m.group(1), m.group(2)
            s2, e2 = _parse_time_cell(s_raw), _parse_time_cell(e_raw)
            if s2 and e2 and s2.strftime("%I:%M %p") == want_s and e2.strftime("%I:%M %p") == want_e:
                r_label = r
                r_next = (label_rows[i + 1] if i + 1 < len(label_rows) else len(grid))
                break

        if r_label is None:
            fail(f"Could not locate On-Call block '{want_s} – {want_e}'.")

        lane_rows = list(range(r_label + 1, r_next))
        for rr in lane_rows:
            v = (grid[rr][c0] if (rr < len(grid) and c0 < len(grid[rr])) else "") or ""
            if v and canon_target_name.lower() in v.lower():
                target_coords.append((rr, c0))
                break

        if not target_coords:
            fail(f"{canon_target_name} not found in that On-Call block.")

    else:
        # UNH / MC: mark every half-hour slot in the window where this OA appears in a lane.
        time_rows: list[int] = []
        for r, row in enumerate(grid):
            col0 = (row[0] if len(row) >= 1 else "") or ""
            if _TIME_CELL_RE.match(col0) and _parse_time_cell(col0):
                time_rows.append(r)
        time_rows.append(len(grid))

        bands: dict[str, tuple[int, int]] = {}
        for i in range(len(time_rows) - 1):
            r0 = time_rows[i]
            r1 = time_rows[i + 1]
            start_label = (grid[r0][0] if len(grid[r0]) >= 1 else "") or ""
            dt = _parse_time_cell(start_label)
            if dt:
                bands[dt.strftime("%I:%M %p")] = (r0, r1)

        for (seg_s, _) in _range_to_slots(sdt, edt):
            lab = seg_s.strftime("%I:%M %p")
            if lab not in bands:
                continue
            r0, r1 = bands[lab]
            lane_rows = list(range(r0 + 1, r1))
            for rr in lane_rows:
                v = (grid[rr][c0] if (rr < len(grid) and c0 < len(grid[rr])) else "") or ""
                if v and canon_target_name.lower() in v.lower():
                    target_coords.append((rr, c0))
                    break

        if not target_coords:
            fail(f"{canon_target_name} not found in UNH/MC lanes for the selected window.")

    # 1) Color the cells (no text changes)
    color = _ORANGE if covered_by else _RED
    _format_cells(ws, target_coords, color)

    # 2) Bump sheet version + seed cache so UI is fast on rerun
    try:
        import gspread.utils as a1
        bump_ws_version(ws)
        try:
            end_col_letter = a1.rowcol_to_a1(1, ONCALL_MAX_COLS).split("1")[0]
            full_range = f"A1:{end_col_letter}{ONCALL_MAX_ROWS}"
            seed_batch_get_cache(ws, [full_range], [grid])
        except Exception:
            pass
    except Exception:
        try:
            bump_ws_version(ws)
        except Exception:
            pass

    label = f"{day.title()} {fmt_time(sdt)}–{fmt_time(edt)}"
    if covered_by:
        return (
            f"Call-Out marked for **{canon_target_name}** on **{campus_title}** ({label}) — "
            f"**orange** (covered)."
        )
    return f"Call-Out marked for **{canon_target_name}** on **{campus_title}** ({label}) — **red** (no cover)."
