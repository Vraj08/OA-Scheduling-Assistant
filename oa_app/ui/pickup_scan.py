"""Scan schedule tabs for *red* (no-cover) call-out cells.

This module reads Google Sheets gridData (values + formats) to discover
call-out cells that are colored **red** (no cover). It then produces:

1) A "tradeboard" table (time x day) that lists the called-out OA names.
2) A list of contiguous called-out windows, suitable for a pickup request.

Notes:
  - UNH/MC tabs are half-hour ladders: a time label row in col A, followed by
    one or more lane rows until the next time label.
  - On-Call tabs have per-day block labels like "7:00 AM - 11:00 AM" inside
    each day column, with lanes beneath each label.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from ..core.utils import fmt_time
from ..integrations.gspread_io import with_backoff
from . import schedule_query


_MMDD_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})\b")


@dataclass(frozen=True)
class PickupWindow:
    campus_title: str
    kind: str  # "UNH" | "MC" | "ONCALL"
    day_canon: str  # "monday" ...
    target_name: str
    start: datetime
    end: datetime


def _rgb(cell: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """Return an rgbColor dict {red, green, blue} if present."""
    if not isinstance(cell, dict):
        return None
    fmt = cell.get("effectiveFormat") or cell.get("userEnteredFormat") or {}
    if not isinstance(fmt, dict):
        return None

    bg = fmt.get("backgroundColor")
    if isinstance(bg, dict):
        return bg

    # Newer Sheets API can surface color under backgroundColorStyle.rgbColor
    bgs = fmt.get("backgroundColorStyle") or {}
    if isinstance(bgs, dict):
        rgb = bgs.get("rgbColor")
        if isinstance(rgb, dict):
            return rgb

    return None


def _is_red(bg: Optional[Dict[str, float]]) -> bool:
    """Detect the 'no-cover callout' red used in your sheets.

    The schedule uses both a stronger red (set by the app) and a light pink
    (common Google Sheets 'light red' fill). We treat both as callout-red.
    """
    if not bg:
        return False
    r = float(bg.get("red", 0.0) or 0.0)
    g = float(bg.get("green", 0.0) or 0.0)
    b = float(bg.get("blue", 0.0) or 0.0)

    # Ignore near-white.
    if r >= 0.93 and g >= 0.93 and b >= 0.93:
        return False

    # Strong red (what the app paints).
    if (r >= 0.78) and (g <= 0.55) and (b <= 0.55):
        return True

    # Light red / pink (Google Sheets light red variants). Different tabs/weeks
    # can use slightly different shades, so keep this a bit broader.
    # False positives are further prevented by:
    #   - requiring a non-empty, cleaned name (_clean_name)
    #   - excluding the "covered" orange fill in _is_orange
    if r >= 0.85 and g >= 0.55 and b >= 0.55:
        if (r - g) >= 0.06 and (r - b) >= 0.06:
            return True

    return False



def _is_orange(bg: Optional[Dict[str, float]]) -> bool:
    if not bg:
        return False
    r = float(bg.get("red", 0.0) or 0.0)
    g = float(bg.get("green", 0.0) or 0.0)
    b = float(bg.get("blue", 0.0) or 0.0)
    # Covered orange is set by the app as (1.0, 0.65, 0.0)
    return (r >= 0.90) and (g >= 0.50) and (b <= 0.20)


def _cell_text(cell: Dict[str, Any]) -> str:
    v = cell.get("formattedValue")
    if v is None:
        # Some cells only have userEnteredValue
        u = cell.get("userEnteredValue")
        if isinstance(u, dict):
            v = u.get("stringValue") or u.get("numberValue") or u.get("boolValue")
    return ("" if v is None else str(v)).strip()


def _fetch_griddata(ss, title: str, *, max_rows: int, max_cols: int) -> Tuple[List[List[str]], List[List[Optional[Dict[str, float]]]]]:
    """Return (values_grid, bg_grid) for a bounded range starting at A1."""
    import gspread.utils as a1

    end_a1 = a1.rowcol_to_a1(int(max_rows), int(max_cols))
    rng = f"{title}!A1:{end_a1}"
    meta = with_backoff(
        ss.fetch_sheet_metadata,
        params={"includeGridData": True, "ranges": [rng]},
    )

    sheets = (meta or {}).get("sheets") or []
    if not sheets:
        return [], []
    data = (sheets[0] or {}).get("data") or []
    if not data:
        return [], []

    row_data = (data[0] or {}).get("rowData") or []
    grid: List[List[str]] = []
    bg_grid: List[List[Optional[Dict[str, float]]]] = []

    for rd in row_data:
        vals = (rd or {}).get("values") or []
        row_txt: List[str] = []
        row_bg: List[Optional[Dict[str, float]]] = []
        for c in range(max_cols):
            cell = vals[c] if c < len(vals) and isinstance(vals[c], dict) else {}
            row_txt.append(_cell_text(cell))
            row_bg.append(_rgb(cell))
        grid.append(row_txt)
        bg_grid.append(row_bg)

    # Ensure we have exactly max_rows rows (pad if the API returned fewer).
    while len(grid) < max_rows:
        grid.append([""] * max_cols)
        bg_grid.append([None] * max_cols)
    grid = grid[:max_rows]
    bg_grid = bg_grid[:max_rows]
    return grid, bg_grid


def _extract_mmdd_for_col(grid: List[List[str]], col: int) -> Optional[str]:
    # Look near the top of the column for something like "Mon 1/26" or "1/26".
    for r in range(min(25, len(grid))):
        v = (grid[r][col] if col < len(grid[r]) else "") or ""
        m = _MMDD_RE.search(str(v))
        if m:
            return f"{int(m.group(1))}/{int(m.group(2))}"
    return None


def _day_cols_from_grid(grid: List[List[str]]) -> Dict[str, int]:
    """Map canonical day -> column index (0-based)."""
    out: Dict[str, int] = {}
    max_r = min(20, len(grid))
    for r in range(max_r):
        row = grid[r] if r < len(grid) else []
        for c, v in enumerate(row):
            d = schedule_query._canon_day_from_header(v)
            if d and d not in out:
                out[d] = c
    return out


def _time_rows_unh_mc(grid: List[List[str]]) -> List[int]:
    trs: List[int] = []
    for r, row in enumerate(grid):
        if not row:
            continue
        v = (row[0] if len(row) >= 1 else "") or ""
        if schedule_query._TIME_CELL_RE.match(v) and schedule_query._parse_time_cell(v):
            trs.append(r)
    return trs


def _clean_name(cell_txt: str) -> str:
    """Normalize a schedule cell to a display name.

    - Strips prefixes like 'OA:' / 'GOA:'.
    - Ignores legends / examples / placeholders like 'Available GOA Slot'.
    """
    s = (cell_txt or "").strip()
    if not s:
        return ""

    # Normalize whitespace (also handles non‑breaking spaces / odd spacing).
    s = re.sub(r"\s+", " ", s, flags=re.UNICODE).strip()
    low = s.lower().strip()

    # Ignore legends / notes / placeholders inside the sheet.
    # Be aggressive here: anything that mentions call-outs is a legend label,
    # not a real OA name.
    if "called out" in low or "call out" in low or "call-out" in low:
        return ""
    if low.startswith("ex:") or low.startswith("example"):
        return ""
    if "time - off" in low:
        return ""
    if "available" in low and "slot" in low:
        return ""
    if low in {"available goa slot", "available oa slot"}:
        return ""

    # Strip role prefixes.
    s = re.sub(r"^\s*(oa|goa)\s*[:\-]\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"^\s*(oa|goa)\s+", "", s, flags=re.IGNORECASE)

    s = s.strip()
    if not s or s.lower() in {"oa", "goa"}:
        return ""

    # Final guard: reject obvious non-names.
    low2 = s.lower()
    if "available" in low2 or "slot" in low2 or "called out" in low2:
        return ""
    return s



def _group_halfhour_slots(slots: List[Tuple[datetime, datetime, str, str, str]]) -> List[PickupWindow]:
    """slots: (start,end, campus_title, kind, day_canon, target_name)"""
    # Normalize & sort
    slots2 = []
    for s, e, campus, kind, day, name in slots:
        if not name:
            continue
        slots2.append((s, e, campus, kind, day, name))
    slots2.sort(key=lambda x: (x[2], x[4], x[5].lower(), x[0]))

    out: List[PickupWindow] = []
    cur = None
    for s, e, campus, kind, day, name in slots2:
        if cur is None:
            cur = [campus, kind, day, name, s, e]
            continue
        if campus == cur[0] and day == cur[2] and name == cur[3] and s == cur[5]:
            # contiguous
            cur[5] = e
        else:
            out.append(PickupWindow(cur[0], cur[1], cur[2], cur[3], cur[4], cur[5]))
            cur = [campus, kind, day, name, s, e]
    if cur is not None:
        out.append(PickupWindow(cur[0], cur[1], cur[2], cur[3], cur[4], cur[5]))
    return out


def build_tradeboard_unh_mc(
    ss,
    title: str,
    *,
    max_rows: int = 900,
    max_cols: int = 12,
) -> Tuple[pd.DataFrame, List[PickupWindow]]:
    """Return (tradeboard_df, grouped_windows) for a UNH/MC schedule tab.

    The tradeboard repeats the called-out OA label in *every* red time slot cell.
    Each cell shows:

        <Name>
        <start>–<end>
        <UNH|MC>
    """
    grid, bg = _fetch_griddata(ss, title, max_rows=max_rows, max_cols=max_cols)
    if not grid:
        return pd.DataFrame(), []

    day_cols = _day_cols_from_grid(grid)
    if not day_cols:
        return pd.DataFrame(), []

    days_order = [d for d in ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"] if d in day_cols]
    if not days_order:
        days_order = sorted(day_cols.keys())

    # Column labels with optional mm/dd
    col_labels: List[str] = []
    for d in days_order:
        c = day_cols[d]
        mmdd = _extract_mmdd_for_col(grid, c)
        col_labels.append(f"{d.title()} {mmdd}" if mmdd else d.title())

    time_rows = _time_rows_unh_mc(grid)
    if not time_rows:
        return pd.DataFrame(columns=["Time"] + col_labels), []

    # Determine campus kind from the worksheet title.
    kind = "MC" if re.search(r"\bmc\b|main", (title or "").lower()) else "UNH"

    # First pass: collect time slots and raw half-hour red slots.
    row_times: List[datetime] = []
    row_labels: List[str] = []
    halfhour_slots: List[Tuple[datetime, datetime, str, str, str, str]] = []  # (s,e,title,kind,day,name)

    time_rows.append(len(grid))
    for i in range(len(time_rows) - 1):
        r0, r1 = time_rows[i], time_rows[i + 1]
        t_txt = (grid[r0][0] if grid[r0] else "") or ""
        t_dt = schedule_query._parse_time_cell(t_txt)
        if not t_dt:
            continue
        t_end = t_dt + timedelta(minutes=30)

        row_times.append(t_dt)
        row_labels.append(fmt_time(t_dt) if t_dt.minute == 0 else "")

        lane_rows = list(range(r0 + 1, r1))
        for d in days_order:
            c = day_cols[d]
            for rr in lane_rows:
                if rr >= len(grid):
                    continue
                txt_raw = grid[rr][c] if c < len(grid[rr]) else ""
                txt = _clean_name(txt_raw)
                bgc = bg[rr][c] if rr < len(bg) and c < len(bg[rr]) else None
                if txt and _is_red(bgc) and (not _is_orange(bgc)):
                    halfhour_slots.append((t_dt, t_end, title, kind, d, txt))

    # Group contiguous half-hour slots into full callout windows.
    windows = _group_halfhour_slots(halfhour_slots)

    # Build a lookup: (day, time) -> list of labels (repeat full window in each slot).
    slot_labels: Dict[Tuple[str, datetime], List[str]] = {}
    if windows and row_times:
        # Precompute all times for quick iteration.
        times_sorted = sorted(row_times)

        def _add_label(day: str, t: datetime, label: str) -> None:
            slot_labels.setdefault((day, t), []).append(label)

        for w in windows:
            label = f"{w.target_name}\n{fmt_time(w.start)}–{fmt_time(w.end)}\n{w.kind}"
            cur = w.start
            while cur < w.end:
                if cur in times_sorted:
                    _add_label(w.day_canon, cur, label)
                cur += timedelta(minutes=30)

    # Build the tradeboard table.
    table: List[List[str]] = []
    for t in row_times:
        row_vals: List[str] = []
        for d in days_order:
            labels = slot_labels.get((d, t), [])
            # Dedup while preserving order.
            seen = set()
            uniq = []
            for s in labels:
                k = s.strip().lower()
                if k and k not in seen:
                    seen.add(k)
                    uniq.append(s)
            row_vals.append("\n\n".join(uniq))
        table.append(row_vals)

    df = pd.DataFrame(table, columns=col_labels)
    df.insert(0, "Time", row_labels)

    return df, windows




def build_tradeboard_oncall(
    ss,
    title: str,
    *,
    max_rows: int = 900,
    max_cols: int = 16,
) -> Tuple[pd.DataFrame, List[PickupWindow]]:
    """Return (tradeboard_df, windows) for an On-Call week tab.

    Mode A: only include blocks that contain at least one *red* (no-cover) callout.

    The On-Call layout differs from UNH/MC:
      - A header row contains day/date labels like 'Sunday 2/15'
      - Column A typically contains the block time range like '7:00 PM - 11:00 PM'
      - Each block has one or more lane rows beneath it (names/roles)

    We:
      - Locate the header row by scanning the top of the sheet for date labels.
      - Locate block rows by matching the time-range pattern.
      - Within each block, collect red cells and build pickup windows.
    """
    grid, bg = _fetch_griddata(ss, title, max_rows=max_rows, max_cols=max_cols)
    if not grid:
        return pd.DataFrame(), []

    # --- Find the header row (day/date labels) ---
    def _score_header_row(r: int) -> int:
        row = grid[r] if r < len(grid) else []
        score = 0
        for c in range(1, min(max_cols, len(row))):
            v = (row[c] or "").strip()
            if not v:
                continue
            low = v.lower()
            if "time - off" in low or "shift swaps" in low or "future swaps" in low:
                break
            if schedule_query._canon_day_from_header(v):
                score += 1
                continue
            if _MMDD_RE.search(v):
                score += 1
        return score

    header_row = None
    best = 0
    for r in range(min(18, len(grid))):
        sc = _score_header_row(r)
        if sc > best:
            best = sc
            header_row = r

    if header_row is None or best < 4:
        # Fallback: treat row 0 as header if we can't find one.
        header_row = 0

    hdr = grid[header_row]

    # Determine day columns from header row.
    date_cols: List[int] = []
    for c in range(1, max_cols):
        if c >= len(hdr):
            break
        v = (hdr[c] or "").strip()
        if not v:
            continue
        low = v.lower()
        if "time - off" in low or "shift swaps" in low or "future swaps" in low:
            break
        if schedule_query._canon_day_from_header(v) or _MMDD_RE.search(v):
            date_cols.append(c)

    if not date_cols:
        return pd.DataFrame(), []

    # Map columns to canonical weekdays.
    day_cols: Dict[str, int] = {}
    for idx, c in enumerate(date_cols):
        v = (hdr[c] or "").strip()
        d = schedule_query._canon_day_from_header(v)
        if not d:
            # Fallback: assume first date col is Sunday.
            d = schedule_query._WEEK_ORDER_7[idx] if idx < len(schedule_query._WEEK_ORDER_7) else f"day{idx}"
        if d not in day_cols:
            day_cols[d] = c

    days_order = [d for d in schedule_query._WEEK_ORDER_7 if d in day_cols]
    if not days_order:
        days_order = list(day_cols.keys())

    # Use the header strings as display labels if possible.
    col_labels: List[str] = []
    for d in days_order:
        c = day_cols[d]
        label = (hdr[c] or "").strip()
        col_labels.append(label if label else d.title())

    # --- Find block label rows ---
    scan_cols = [day_cols[d] for d in days_order]

    # On-Call tabs come in two layouts:
    #   (1) A single shared time-range label in column A for each block (same hours for all days)
    #   (2) Per-day time-range labels in the day columns (hours can differ by day)
    #
    # We detect "label rows" wherever a time-range appears either in col A or in any day column.
    label_rows: List[int] = []
    for r in range(header_row + 1, len(grid)):
        v0 = (grid[r][0] if len(grid[r]) > 0 else "") or ""
        if schedule_query._RANGE_RE.match(str(v0)):
            label_rows.append(r)
            continue
        for c in scan_cols:
            v = (grid[r][c] if c < len(grid[r]) else "") or ""
            if schedule_query._RANGE_RE.match(str(v)):
                label_rows.append(r)
                break

    if not label_rows:
        return pd.DataFrame(columns=["Time"] + col_labels), []

    label_rows.append(len(grid))

    blocks: Dict[str, Dict[str, List[str]]] = {}
    windows: List[PickupWindow] = []

    for i in range(len(label_rows) - 1):
        r_label = label_rows[i]
        r_next = label_rows[i + 1]

        # Shared time-range (layout 1), if present:
        shared_range = ""
        v0 = (grid[r_label][0] if len(grid[r_label]) > 0 else "") or ""
        if schedule_query._RANGE_RE.match(str(v0)):
            shared_range = str(v0)

        lane_rows = list(range(r_label, r_next))

        # For each day, determine that day's range.
        #
        # On-Call tabs come in two layouts:
        #   (1) A single shared time-range label in column A for each block
        #       (same hours for all days)
        #   (2) Per-day time-range labels in the day columns (hours can differ by day)
        #
        # Some sheets (including copies/exports) can contain *both* a shared
        # label in col A and a per-day label in the day column on the same row.
        # In that case, the per-day label must win; otherwise we can mis-read
        # a weekday window (e.g., 7 PM–12 AM) as a shorter shared window.
        for d in days_order:
            c = day_cols[d]

            # Prefer per-day range at the label row if present.
            cell = (grid[r_label][c] if c < len(grid[r_label]) else "") or ""
            if schedule_query._RANGE_RE.match(str(cell)):
                range_txt = str(cell)
            else:
                range_txt = shared_range

            m = schedule_query._RANGE_RE.match(range_txt.strip()) if range_txt else None
            if not m:
                continue

            sdt = schedule_query._parse_time_cell(m.group(1))
            edt = schedule_query._parse_time_cell(m.group(2))
            if not sdt or not edt:
                continue
            if edt <= sdt:
                edt = edt + timedelta(days=1)

            # Defensive: some On-Call templates accidentally encode the weekday
            # evening window as a short range (e.g., 7 PM–8 PM) while the
            # underlying schedule expectation is the standard 5-hour window.
            # If we detect a very short evening range, expand it so the
            # tradeboard blocks don't end abruptly.
            try:
                dur_m = int((edt - sdt).total_seconds() // 60)
            except Exception:
                dur_m = 0
            if sdt.hour >= 18 and 0 < dur_m <= 90:
                edt = sdt + timedelta(hours=5)

            block_key = f"{fmt_time(sdt)} - {fmt_time(edt)}"

            names: List[str] = []
            any_names = False

            # Scan from label row through lane rows; ignore embedded time-range labels.
            for rr in lane_rows:
                if rr >= len(grid) or c >= len(grid[rr]):
                    continue
                raw = grid[rr][c]
                if schedule_query._RANGE_RE.match(str(raw or "")):
                    continue
                txt = _clean_name(raw)
                bgc = bg[rr][c] if rr < len(bg) and c < len(bg[rr]) else None
                if txt and _is_red(bgc) and (not _is_orange(bgc)):
                    any_names = True
                    names.append(f"{txt}\n{fmt_time(sdt)}–{fmt_time(edt)}\nOn-Call")
                    windows.append(PickupWindow(title, "ONCALL", d, txt, sdt, edt))

            if names and any_names:
                blocks.setdefault(block_key, {}).setdefault(d, []).extend(names)

    # Mode A: keep only blocks that have at least one name (already enforced).
    # Sort rows by start time.
    def _start_minutes(k: str) -> int:
        left = k.split("-")[0].strip()
        dt = schedule_query._parse_time_cell(left)
        if not dt:
            return 999999
        return dt.hour * 60 + dt.minute

    rows_sorted = sorted(blocks.keys(), key=_start_minutes)

    table: List[List[str]] = []
    for key in rows_sorted:
        row: List[str] = []
        for d in days_order:
            labels = blocks.get(key, {}).get(d, [])
            # Dedup while preserving order.
            seen = set()
            uniq = []
            for s in labels:
                k = s.strip().lower()
                if k and k not in seen:
                    seen.add(k)
                    uniq.append(s)
            row.append("\n\n".join(uniq))
        table.append(row)

    df = pd.DataFrame(table, columns=col_labels)
    df.insert(0, "Time", rows_sorted)

    return df, windows




@st.cache_data(ttl=15, show_spinner=False)
def cached_tradeboard(
    ss_id: str,
    tab_title: str,
    version: int,
    kind: str,
    *,
    max_rows: int = 900,
    max_cols: int = 16,
) -> Dict[str, Any]:
    """Cached wrapper returning JSON-ish data."""
    ss = st.session_state.get("_SS_HANDLE_BY_ID", {}).get(ss_id)
    if not ss:
        return {"df": None, "windows": []}
    if kind in {"UNH", "MC"}:
        df, wins = build_tradeboard_unh_mc(ss, tab_title, max_rows=max_rows, max_cols=max_cols)
    else:
        df, wins = build_tradeboard_oncall(ss, tab_title, max_rows=max_rows, max_cols=max_cols)
    return {
        "df": df,
        "windows": [
            {
                "campus_title": w.campus_title,
                "kind": w.kind,
                "day_canon": w.day_canon,
                "target_name": w.target_name,
                "start": w.start.isoformat(),
                "end": w.end.isoformat(),
            }
            for w in wins
        ],
    }
