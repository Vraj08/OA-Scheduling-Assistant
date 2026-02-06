"""Idempotent rendering of swap/callout sections from Supabase into Google Sheets.

Supabase is the source of truth for callouts/pickups. The Google Sheet sections
"Shift Swaps for the week" and "Future Swaps/Call outs" are treated as a
rendered view.

This module does **not** update the schedule grid cells (that is handled by the
existing approval apply logic). It only rewrites the side sections.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Iterable

from zoneinfo import ZoneInfo

import gspread

from ..core.sheets_sections import (
    Section,
    blanks,
    compute_section,
    find_header_cell,
    pad_rows,
    read_top_grid,
)


LA = ZoneInfo("America/Los_Angeles")

HDR_WEEKLY = "Shift Swaps for the week"
HDR_FUTURE = "Future Swaps/Call outs"

# Colors (Sheets API RGB in 0..1)
ORANGE = {"red": 1.0, "green": 0.65, "blue": 0.0}
RED = {"red": 0.95, "green": 0.25, "blue": 0.25}


def _week_bounds(today: date) -> tuple[date, date]:
    start = today - timedelta(days=today.weekday())  # Monday
    end = start + timedelta(days=6)
    return start, end


def _parse_dt(x: Any) -> datetime | None:
    if not x:
        return None
    if isinstance(x, datetime):
        return x
    s = str(x)
    try:
        # Python 3.11: fromisoformat supports offsets
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def _fmt_day(d: date) -> str:
    return d.strftime("%A")


def _fmt_timerange(sdt: datetime, edt: datetime) -> str:
    def _fmt(t: datetime) -> str:
        return t.astimezone(LA).strftime("%-I:%M %p").replace(":00", "")

    return f"{_fmt(sdt)} - {_fmt(edt)}"


@dataclass
class RowOut:
    values: list[str]
    color: dict


def _overlap_seconds(a0: datetime, a1: datetime, b0: datetime, b1: datetime) -> float:
    lo = max(a0, b0)
    hi = min(a1, b1)
    return max(0.0, (hi - lo).total_seconds())


def _best_cover_for_callout(callout: dict, pickups: list[dict]) -> dict | None:
    """Find the best matching pickup for a callout using overlap + identity."""
    c_campus = str(callout.get("campus", "")).upper()
    c_date = str(callout.get("event_date", ""))
    c_name = str(callout.get("caller_name", "")).strip().lower()
    c_s = _parse_dt(callout.get("shift_start_at"))
    c_e = _parse_dt(callout.get("shift_end_at"))
    if not (c_date and c_name and c_s and c_e):
        return None

    best: dict | None = None
    best_ov = 0.0
    for p in pickups:
        if str(p.get("campus", "")).upper() != c_campus:
            continue
        if str(p.get("event_date", "")) != c_date:
            continue
        if str(p.get("target_name", "")).strip().lower() != c_name:
            continue
        p_s = _parse_dt(p.get("shift_start_at"))
        p_e = _parse_dt(p.get("shift_end_at"))
        if not (p_s and p_e):
            continue
        ov = _overlap_seconds(c_s, c_e, p_s, p_e)
        if ov > best_ov:
            best_ov = ov
            best = p
    return best


def _row_from_callout(c: dict, cover_pickup: dict | None) -> RowOut:
    d = date.fromisoformat(str(c["event_date"]))
    sdt = _parse_dt(c.get("shift_start_at"))
    edt = _parse_dt(c.get("shift_end_at"))
    tr = _fmt_timerange(sdt, edt) if (sdt and edt) else ""
    caller = str(c.get("caller_name", "")).strip()
    coverer = ""
    if cover_pickup:
        coverer = str(cover_pickup.get("picker_name", "")).strip()
    notes = str(c.get("reason", "") or "").strip()
    campus = str(c.get("campus", "")).upper()
    color = ORANGE if coverer else RED
    return RowOut(
        values=[
            d.isoformat(),
            _fmt_day(d),
            campus,
            tr,
            caller,
            coverer,
            "CALLOUT",
            notes,
        ],
        color=color,
    )


def _row_from_pickup(p: dict) -> RowOut:
    d = date.fromisoformat(str(p["event_date"]))
    sdt = _parse_dt(p.get("shift_start_at"))
    edt = _parse_dt(p.get("shift_end_at"))
    tr = _fmt_timerange(sdt, edt) if (sdt and edt) else ""
    target = str(p.get("target_name", "")).strip()
    picker = str(p.get("picker_name", "")).strip()
    notes = str(p.get("note", "") or "").strip()
    campus = str(p.get("campus", "")).upper()
    return RowOut(
        values=[
            d.isoformat(),
            _fmt_day(d),
            campus,
            tr,
            target,
            picker,
            "PICKUP",
            notes,
        ],
        color=ORANGE,
    )


def _query_range(supabase, table: str, *, d0: date, d1: date | None, future_only: bool = False) -> list[dict]:
    q = supabase.table(table).select("*")
    if future_only:
        q = q.gt("event_date", d0.isoformat())
    else:
        q = q.gte("event_date", d0.isoformat())
        if d1 is not None:
            q = q.lte("event_date", d1.isoformat())
    resp = q.execute()
    return list(resp.data or [])


def _sort_key(r: RowOut) -> tuple:
    # values: [date, day, campus, time, ...]
    return (r.values[0], r.values[2], r.values[3])


def _format_rows(ws: gspread.Worksheet, *, start_row: int, start_col: int, row_colors: list[dict], num_cols: int) -> None:
    """Apply background color per row across num_cols columns."""
    if not row_colors:
        return
    requests: list[dict] = []
    for i, rgb in enumerate(row_colors):
        r0 = (start_row - 1) + i
        c0 = start_col - 1
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": r0,
                        "endRowIndex": r0 + 1,
                        "startColumnIndex": c0,
                        "endColumnIndex": c0 + num_cols,
                    },
                    "cell": {"userEnteredFormat": {"backgroundColor": rgb}},
                    "fields": "userEnteredFormat.backgroundColor",
                }
            }
        )
    ws.spreadsheet.batch_update({"requests": requests})


def _clear_format(ws: gspread.Worksheet, section: Section) -> None:
    """Best-effort reset of background color for the section."""
    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": ws.id,
                    "startRowIndex": section.start_row - 1,
                    "endRowIndex": section.start_row - 1 + section.max_rows,
                    "startColumnIndex": section.start_col - 1,
                    "endColumnIndex": section.start_col - 1 + section.num_cols,
                },
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 1, "green": 1, "blue": 1}}},
                "fields": "userEnteredFormat.backgroundColor",
            }
        }
    ]
    ws.spreadsheet.batch_update({"requests": requests})


def _write_section(ws: gspread.Worksheet, section: Section, rows: list[RowOut]) -> int:
    # Clear values + reset formatting
    ws.batch_update([{ "range": section.a1_range(), "values": blanks(section.max_rows, section.num_cols)}])
    try:
        _clear_format(ws, section)
    except Exception:
        pass

    if not rows:
        return 0

    vals = pad_rows([r.values for r in rows], section.num_cols)
    # write only needed rows
    import gspread.utils as a1
    tl = a1.rowcol_to_a1(section.start_row, section.start_col)
    br = a1.rowcol_to_a1(section.start_row + len(vals) - 1, section.start_col + section.num_cols - 1)
    ws.batch_update([{"range": f"{tl}:{br}", "values": vals}])

    # formatting per row
    try:
        _format_rows(ws, start_row=section.start_row, start_col=section.start_col, row_colors=[r.color for r in rows], num_cols=section.num_cols)
    except Exception:
        pass
    return len(rows)


def _find_sections_in_ws(ws: gspread.Worksheet, *, max_rows: int = 250, max_cols: int = 80) -> tuple[Section | None, Section | None]:
    grid = read_top_grid(ws, max_rows=max_rows, max_cols=max_cols)
    loc_weekly = find_header_cell(grid, HDR_WEEKLY)
    loc_future = find_header_cell(grid, HDR_FUTURE)
    weekly = compute_section(loc_weekly[0], loc_weekly[1]) if loc_weekly else None
    future = compute_section(loc_future[0], loc_future[1]) if loc_future else None
    return weekly, future


def sync_swaps_to_sheets(ss: gspread.Spreadsheet, supabase, *, sheet_title: str | None = None) -> dict[str, Any]:
    """Rewrite swap sections from Supabase into the workbook.

    If sheet_title is provided, only that worksheet is updated; otherwise, every
    worksheet that contains BOTH headers will be updated.
    """
    today = datetime.now(LA).date()
    wk0, wk1 = _week_bounds(today)

    # Query Supabase
    weekly_callouts = _query_range(supabase, "callouts", d0=wk0, d1=wk1)
    weekly_pickups = _query_range(supabase, "pickups", d0=wk0, d1=wk1)
    future_callouts = _query_range(supabase, "callouts", d0=wk1, d1=None, future_only=True)
    future_pickups = _query_range(supabase, "pickups", d0=wk1, d1=None, future_only=True)

    # Build rows with cover detection
    def build_rows(callouts: list[dict], pickups: list[dict]) -> list[RowOut]:
        outs: list[RowOut] = []
        for c in callouts:
            cover = _best_cover_for_callout(c, pickups)
            outs.append(_row_from_callout(c, cover))
        for p in pickups:
            outs.append(_row_from_pickup(p))
        outs.sort(key=_sort_key)
        return outs

    weekly_rows = build_rows(weekly_callouts, weekly_pickups)
    future_rows = build_rows(future_callouts, future_pickups)

    targets: list[gspread.Worksheet] = []
    if sheet_title:
        targets = [ss.worksheet(sheet_title)]
    else:
        for ws in ss.worksheets():
            weekly_sec, future_sec = _find_sections_in_ws(ws)
            if weekly_sec and future_sec:
                targets.append(ws)

    if not targets:
        raise ValueError(
            f"Could not find both headers ('{HDR_WEEKLY}', '{HDR_FUTURE}') in any worksheet. "
            "Check the workbook template."
        )

    summary: dict[str, Any] = {
        "week_start": wk0.isoformat(),
        "week_end": wk1.isoformat(),
        "weekly_written": 0,
        "future_written": 0,
        "sheets_updated": [],
    }

    for ws in targets:
        weekly_sec, future_sec = _find_sections_in_ws(ws)
        if not (weekly_sec and future_sec):
            continue
        n1 = _write_section(ws, weekly_sec, weekly_rows)
        n2 = _write_section(ws, future_sec, future_rows)
        summary["weekly_written"] += n1
        summary["future_written"] += n2
        summary["sheets_updated"].append(ws.title)

    return summary
