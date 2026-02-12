"""Write callout/pickup entries into the Shift Swaps sections.

The Google Sheet contains two log sections per schedule tab:
  - "Shift Swaps for the week"
  - "Future Swaps/Call outs"

This module appends a *single* row into the appropriate section and applies a
background fill to match existing conventions:
  - Callout: red
  - Pickup: orange

Idempotency:
  We avoid duplicating an identical entry by scanning existing rows in both
  sections and comparing a stable key:

    date + campus + caller + coverer + start/end + type

We intentionally ignore the notes field when determining duplicates.
"""

from __future__ import annotations

from datetime import date, datetime, time
from hashlib import sha1
import re
from typing import Optional, Tuple, List

import gspread

from ..core import sheets_sections, utils
from ..core.week_range import LA_TZ, date_for_weekday, la_today, week_range_from_worksheet
from ..integrations.gspread_io import with_backoff


# NOTE: In the sheet template these sections are **one-column log lists**, not a
# multi-column table. The headers typically appear adjacent (e.g., I1 and J1 for
# UNH/MC or K1 and L1 for On-Call), and each entry is stored as a single cell
# under the corresponding header.

HDR_WEEKLY = "Shift Swaps for the week"
HDR_FUTURE = "Future Swaps/Call outs"

# Variants seen across tabs / historical templates.
HDR_WEEKLY_ALIASES = [
    "Shift Swaps for the week",
    "Shift Swaps/Covers",
    "Shift Swaps",
]
HDR_FUTURE_ALIASES = [
    "Future Swaps/Call outs",
    "Future Swaps/Callouts",
]


RED = {"red": 0.9, "green": 0.2, "blue": 0.2}
ORANGE = {"red": 1.0, "green": 0.65, "blue": 0.0}
WHITE = {"red": 1.0, "green": 1.0, "blue": 1.0}


def _to_la(dt: datetime) -> datetime:
    # Treat naive datetimes as LA-local.
    if dt.tzinfo is None:
        return dt.replace(tzinfo=LA_TZ)
    return dt.astimezone(LA_TZ)


def _mins_since_midnight(dt: datetime) -> int:
    d = _to_la(dt)
    m = d.hour * 60 + d.minute
    # Interpret 12:00 AM end as 24:00 when it is intended to be the shift end.
    if d.time() == time(0, 0):
        return 24 * 60
    return m


def _fmt_time_human(t: time) -> str:
    return utils.fmt_time(datetime.combine(date.today(), t))


def _fmt_interval_human(a_min: int, b_min: int) -> str:
    a = time(a_min // 60 % 24, a_min % 60)
    b_min2 = 0 if b_min == 24 * 60 else b_min
    b = time(b_min2 // 60 % 24, b_min2 % 60)
    return f"{utils.fmt_time(datetime.combine(date.today(), a)).replace(':00 ', ' ')} - {utils.fmt_time(datetime.combine(date.today(), b)).replace(':00 ', ' ')}"


def _subtract_interval(intervals: List[Tuple[int, int]], cs: int, ce: int) -> List[Tuple[int, int]]:
    if ce <= cs:
        return intervals
    out: List[Tuple[int, int]] = []
    for a, b in intervals:
        if b <= cs or ce <= a:
            out.append((a, b))
            continue
        # overlap exists
        if cs <= a and ce >= b:
            continue
        if cs <= a < ce < b:
            out.append((ce, b))
            continue
        if a < cs < b <= ce:
            out.append((a, cs))
            continue
        if a < cs and ce < b:
            out.append((a, cs))
            out.append((ce, b))
            continue
    out.sort()
    # Merge adjacent/touching
    merged: List[Tuple[int, int]] = []
    for a, b in out:
        if not merged:
            merged.append((a, b))
        else:
            pa, pb = merged[-1]
            if a <= pb:
                merged[-1] = (pa, max(pb, b))
            else:
                merged.append((a, b))
    return merged


_RE_MD = re.compile(r"\b(\d{1,2}/\d{1,2})\b")


def _extract_oa_key(cell_text: str) -> Optional[str]:
    m = re.search(r"oa_key\s*=\s*([0-9a-f]{8,40})", (cell_text or ""), flags=re.I)
    return m.group(1) if m else None


def _parse_nocover_intervals_from_line(line: str) -> List[Tuple[int, int]]:
    """Parse 'No cover: 9 AM - 10 AM, 11:30 AM - 5 PM' into minute intervals."""
    m = re.search(r"no\s*cover\s*:\s*(.+)$", (line or ""), flags=re.I)
    if not m:
        return []
    s = m.group(1).strip()
    # Cut off trailing sentences (Reason:, Note:, etc.)
    s = re.split(r"\b(reason|note)\s*:\s*", s, maxsplit=1, flags=re.I)[0].strip()
    s = s.rstrip(".")
    parts = [p.strip() for p in s.split(",") if p.strip()]
    out: List[Tuple[int, int]] = []
    for p in parts:
        p2 = utils.clean_dash(p)
        if "-" not in p2:
            continue
        a_raw, b_raw = [x.strip() for x in p2.split("-", 1)]
        try:
            a = utils.parse_time_str(a_raw)
            b = utils.parse_time_str(b_raw)
        except Exception:
            continue
        a_min = a.hour * 60 + a.minute
        b_min = b.hour * 60 + b.minute
        if b == time(0, 0):
            b_min = 24 * 60
        if b_min <= a_min:
            continue
        out.append((a_min, b_min))
    out.sort()
    return out


def _extract_reason(line: str) -> str:
    m = re.search(r"\breason\s*:\s*(.+)$", (line or ""), flags=re.I)
    if not m:
        return ""
    return m.group(1).strip().rstrip(".")


def _render_callout_cell(
    *,
    caller: str,
    event_date: date,
    campus: str,
    uncovered: List[Tuple[int, int]],
    reason: str,
    key_hash: str,
) -> str:
    md = f"{event_date.month}/{event_date.day}"
    if not uncovered:
        # no cell to render
        return ""
    ranges = ", ".join(_fmt_interval_human(a, b) for a, b in uncovered)
    core = f"{caller} called out on {md} ({campus}). No cover: {ranges}."
    if reason:
        core = f"{core} Reason: {reason}."
    return f"{core}\n[oa_key={key_hash}]"


def _maybe_reconcile_callouts_after_pickup(
    ws: gspread.Worksheet,
    *,
    weekly_sec: sheets_sections.Section,
    future_sec: sheets_sections.Section,
    event_date: date,
    campus: str,
    target: str,
    pickup_start_at: datetime,
    pickup_end_at: datetime,
) -> None:
    """When a pickup covers a portion of a callout, update the callout entry to show only remaining 'no cover' times."""
    target_k = utils.name_key(target)
    md = f"{event_date.month}/{event_date.day}"

    cs = _mins_since_midnight(pickup_start_at)
    ce = _mins_since_midnight(pickup_end_at)

    for sec in (weekly_sec, future_sec):
        vals = _read_section(ws, sec)
        for i, cell in enumerate(vals):
            if not cell or "called out on" not in cell.lower():
                continue
            if md not in cell:
                continue
            if f"({campus})".lower() not in cell.lower():
                continue
            # caller name match
            first_line = str(cell).split("\n", 1)[0]
            caller_name = first_line.split(" called out on", 1)[0].strip()
            if utils.name_key(caller_name) != target_k:
                continue

            uncovered = _parse_nocover_intervals_from_line(first_line)
            if not uncovered:
                continue
            # Only adjust if pickup overlaps uncovered.
            overlaps = any(not (b <= cs or ce <= a) for a, b in uncovered)
            if not overlaps:
                continue

            new_uncovered = _subtract_interval(uncovered, cs, ce)
            key_hash = _extract_oa_key(cell) or ""
            reason = _extract_reason(first_line)

            row_1based = sec.start_row + i
            a1 = gspread.utils.rowcol_to_a1(row_1based, sec.start_col)
            if not new_uncovered:
                # Fully covered now: clear row and reset formatting.
                with_backoff(ws.update, a1, [[""]])
                _format_row(ws, row_1based=row_1based, start_col_1based=sec.start_col, num_cols=sec.num_cols, color=WHITE)
            else:
                new_text = _render_callout_cell(
                    caller=caller_name,
                    event_date=event_date,
                    campus=campus,
                    uncovered=new_uncovered,
                    reason=reason,
                    key_hash=key_hash,
                )
                with_backoff(ws.update, a1, [[new_text]])
                _format_row(ws, row_1based=row_1based, start_col_1based=sec.start_col, num_cols=sec.num_cols, color=RED)


def _fmt_ampm(dt: datetime) -> str:
    # 12-hour, drop :00 when possible
    s = dt.strftime("%I:%M %p").lstrip("0")
    s = s.replace(":00 ", " ")
    return s


def fmt_timerange(start_at: datetime, end_at: datetime) -> str:
    return f"{_fmt_ampm(start_at)} - {_fmt_ampm(end_at)}"


def infer_week_range(
    ws: gspread.Worksheet,
    *,
    today: Optional[date] = None,
) -> Optional[Tuple[date, date]]:
    return week_range_from_worksheet(ws, today=today or la_today())


def is_in_worksheet_week(ws: gspread.Worksheet, event_date: date) -> bool:
    wr = infer_week_range(ws)
    if not wr:
        return False
    ws_start, ws_end = wr
    return ws_start <= event_date <= ws_end


def resolve_event_date_for_day(ws: gspread.Worksheet, day_canon: str) -> Optional[date]:
    """Resolve a weekday name to an actual date in the worksheet's week range."""
    wr = infer_week_range(ws)
    if not wr:
        return None
    ws_start, ws_end = wr
    return date_for_weekday(ws_start, ws_end, day_canon)


def _norm(s: str) -> str:
    return " ".join(str(s or "").strip().lower().split())


def _row_key(
    *,
    date_str: str,
    campus: str,
    time_range: str,
    caller: str,
    coverer: str,
    typ: str,
) -> str:
    return "|".join(
        [
            _norm(date_str),
            _norm(campus),
            _norm(time_range),
            _norm(caller),
            _norm(coverer),
            _norm(typ),
        ]
    )


def _key_hash(key: str) -> str:
    # Short, stable fingerprint to embed into the cell for idempotency.
    return sha1(key.encode("utf-8")).hexdigest()[:12]


def _make_entry_text(
    *,
    typ: str,
    event_date: date,
    campus: str,
    caller: str,
    coverer: str,
    time_range: str,
    notes: str,
    key_hash: str,
) -> str:
    """Human-friendly entry text.

    Requirements from your sheet:
    - Callouts read like: "Vraj Patel called out on 2/2 for 9 AM - 5 PM. No cover."
    - Pickups read like: "Sydney Hunter is covering Vraj Patel on 2/3 for 10 AM - 11:30 AM."

    We still embed a stable id marker for idempotency.
    """
    caller = (caller or "").strip()
    coverer = (coverer or "").strip() or "no cover"
    notes = (notes or "").strip()
    md = f"{event_date.month}/{event_date.day}"

    if typ.upper() == "PICKUP":
        core = f"{coverer} is covering {caller} on {md} ({campus}) for {time_range}."
        if notes:
            core = f"{core} Note: {notes}."
    else:
        # Callouts are logged as no cover, and may be updated later as pickups cover parts.
        core = f"{caller} called out on {md} ({campus}). No cover: {time_range}."
        if notes:
            core = f"{core} Reason: {notes}."

    # Keep the key on a second line so the first line stays clean.
    return f"{core}\n[oa_key={key_hash}]"


def _find_first_header(grid: list[list[str]], candidates: list[str]) -> Optional[tuple[int, int]]:
    for h in candidates:
        loc = sheets_sections.find_header_cell(grid, h)
        if loc:
            return loc
    return None


def _find_header_contains(grid: list[list[str]], needles: list[str]) -> Optional[tuple[int, int]]:
    """Fallback: find the first cell whose normalized text contains all needle tokens."""
    if not grid:
        return None
    toks = [_norm(n) for n in needles if _norm(n)]
    if not toks:
        return None
    R = len(grid)
    C = max((len(r) for r in grid), default=0)
    for r in range(R):
        row = grid[r]
        for c in range(min(C, len(row))):
            v = row[c] if c < len(row) else ""
            if not v:
                continue
            vv = _norm(str(v))
            if all(t in vv for t in toks):
                return (r + 1, c + 1)
    return None


def _load_sections(ws: gspread.Worksheet) -> Tuple[sheets_sections.Section, sheets_sections.Section]:
    """Locate weekly + future swap sections.

    In your template these are usually adjacent one-column lists (weekly header
    immediately left of future header). Some older tabs use "Shift Swaps/Covers"
    without a future header; in that case we create the future header in the
    empty cell to the right when possible.
    """
    # Sections can live far to the right; scan a generous area first (fast path).
    grid = sheets_sections.read_top_grid(ws, max_rows=500, max_cols=160)
    loc_weekly = _find_first_header(grid, HDR_WEEKLY_ALIASES)
    loc_future = _find_first_header(grid, HDR_FUTURE_ALIASES)

    # Extra tolerance for slightly modified headers.
    if not loc_weekly:
        loc_weekly = _find_header_contains(grid, ["shift", "swap"])
    if not loc_future:
        loc_future = _find_header_contains(grid, ["future", "swap"])

    # Slow but robust fallback: full-sheet text search via Sheets API.
    # This handles cases where the swap sections are placed very far right or
    # the sheet API truncates our grid read.
    if not loc_weekly:
        # Regex fallback matches headers even if they have extra punctuation/newlines.
        try:
            cells = with_backoff(ws.findall, re.compile(r"shift\s*swaps", re.I))
        except Exception:
            cells = []
        if cells:
            loc_weekly = (cells[0].row, cells[0].col)

    if not loc_future:
        try:
            cells = with_backoff(ws.findall, re.compile(r"future\s*swaps", re.I))
        except Exception:
            cells = []
        if cells:
            loc_future = (cells[0].row, cells[0].col)

    if not loc_weekly and not loc_future:
        # Helpful debug: list any cells containing "shift swaps" / "future swaps"
        raise ValueError(
            "Could not find Shift Swaps sections in this tab. "
            "Expected headers like 'Shift Swaps for the week' (or 'Shift Swaps/Covers') "
            "and 'Future Swaps/Call outs'."
        )

    if loc_weekly and not loc_future:
        # Try to infer/create a future header near the weekly header row.
        r, c = loc_weekly
        # Prefer immediate right cell if blank.
        chosen: Optional[Tuple[int, int]] = None
        for offset in range(1, 6):
            c2 = c + offset
            existing = ""
            if r - 1 < len(grid) and c2 - 1 < len(grid[r - 1]):
                existing = str(grid[r - 1][c2 - 1] or "").strip()
            if existing:
                # If a future header already exists slightly to the right, use it.
                if "future" in existing.lower() and "swap" in existing.lower():
                    chosen = (r, c2)
                    break
                continue
            # Blank cell: best candidate.
            chosen = (r, c2)
            break

        if chosen:
            rr, cc = chosen
            # Create the header text if truly empty.
            try:
                with_backoff(ws.update, gspread.utils.rowcol_to_a1(rr, cc), [[HDR_FUTURE]])
            except Exception:
                # Even if header creation fails (permissions, etc.), we can still
                # treat this column as the future section to avoid blocking swaps.
                pass
            loc_future = chosen

    if not loc_weekly or not loc_future:
        raise ValueError(
            "Could not find Shift Swaps sections in this tab. "
            "Expected headers: 'Shift Swaps for the week' (or 'Shift Swaps/Covers') "
            "and 'Future Swaps/Call outs'."
        )

    # One-column lists under each header.
    weekly = sheets_sections.compute_section(loc_weekly[0], loc_weekly[1], num_cols=1)
    future = sheets_sections.compute_section(loc_future[0], loc_future[1], num_cols=1)
    return weekly, future


def _read_section(ws: gspread.Worksheet, sec: sheets_sections.Section) -> list[str]:
    """Read the section as a fixed-height list.

    The Sheets Values API omits *trailing* empty rows/cells. If we simply call
    ws.get(range) we may only receive rows up to the last non-empty cell, which
    would make it look like there are no blank rows to append into.

    We therefore pad the response to the section's declared max_rows.
    """
    vals = with_backoff(ws.get, sec.a1_range()) or []
    vals = sheets_sections.pad_rows(vals, sec.num_cols)

    # Pad missing trailing rows.
    missing = sec.max_rows - len(vals)
    if missing > 0:
        vals.extend([[""] * sec.num_cols for _ in range(missing)])

    # one-column
    return [str(r[0] or "") for r in vals]


def _first_empty_row(values: list[str]) -> Optional[int]:
    for i, v in enumerate(values):
        if not str(v or "").strip():
            return i
    return None


def _format_row(
    ws: gspread.Worksheet,
    *,
    row_1based: int,
    start_col_1based: int,
    num_cols: int,
    color: dict,
) -> None:
    sheet_id = ws._properties.get("sheetId")
    if sheet_id is None:
        return
    req = {
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": row_1based - 1,
                "endRowIndex": row_1based,
                "startColumnIndex": start_col_1based - 1,
                "endColumnIndex": (start_col_1based - 1) + num_cols,
            },
            "cell": {"userEnteredFormat": {"backgroundColor": color}},
            "fields": "userEnteredFormat(backgroundColor)",
        }
    }
    with_backoff(ws.spreadsheet.batch_update, {"requests": [req]})


def append_swap_entry(
    ss: gspread.Spreadsheet,
    *,
    campus_title: str,
    campus: str,
    event_date: date,
    start_at: datetime,
    end_at: datetime,
    caller: str,
    coverer: str,
    typ: str,  # "CALLOUT" | "PICKUP"
    notes: str = "",
    force_future: Optional[bool] = None,
) -> str:
    """Append a callout/pickup entry into the correct swaps section.

    Returns which section was used: "weekly" or "future".
    """
    ws = with_backoff(ss.worksheet, campus_title)
    weekly_sec, future_sec = _load_sections(ws)

    wr = infer_week_range(ws)
    in_week = bool(wr and (wr[0] <= event_date <= wr[1]))
    if force_future is True:
        in_week = False
    if force_future is False:
        in_week = True

    sec = weekly_sec if in_week else future_sec
    other = future_sec if in_week else weekly_sec

    date_str = event_date.isoformat()
    time_range = fmt_timerange(start_at, end_at)

    coverer = (coverer or "").strip() or "no cover"

    want_key = _row_key(
        date_str=date_str,
        campus=campus,
        time_range=time_range,
        caller=caller,
        coverer=coverer,
        typ=typ,
    )

    key_hash = _key_hash(want_key)
    entry_text = _make_entry_text(
        typ=typ,
        event_date=event_date,
        campus=campus,
        caller=caller,
        coverer=coverer,
        time_range=time_range,
        notes=notes,
        key_hash=key_hash,
    )

    # Idempotency: scan both sections for the embedded key.
    sec_vals = _read_section(ws, sec)
    other_vals = _read_section(ws, other)
    needle = f"oa_key={key_hash}".lower()
    for vals in (sec_vals, other_vals):
        for v in vals:
            if needle in str(v or "").lower():
                return "weekly" if in_week else "future"

    idx0 = _first_empty_row(sec_vals)
    if idx0 is None:
        # Section is "full" within sec.max_rows. Append just below the declared
        # section block (do NOT block swaps).
        idx0 = sec.max_rows

    row_1based = sec.start_row + idx0
    # Ensure the worksheet has enough rows to write into.
    try:
        row_count = int(getattr(ws, "row_count", 0) or 0)
        if row_count and row_1based > row_count:
            with_backoff(ws.add_rows, (row_1based - row_count) + 50)
    except Exception:
        pass
    a1 = gspread.utils.rowcol_to_a1(row_1based, sec.start_col)
    with_backoff(ws.update, a1, [[entry_text]])

    color = ORANGE if typ.upper() == "PICKUP" else RED
    _format_row(
        ws,
        row_1based=row_1based,
        start_col_1based=sec.start_col,
        num_cols=sec.num_cols,
        color=color,
    )

    # If this is a pickup covering a callout, update the existing callout entry
    # so it only shows the remaining "no cover" window(s).
    if typ.upper() == "PICKUP":
        try:
            _maybe_reconcile_callouts_after_pickup(
                ws,
                weekly_sec=weekly_sec,
                future_sec=future_sec,
                event_date=event_date,
                campus=campus,
                target=caller,
                pickup_start_at=start_at,
                pickup_end_at=end_at,
            )
        except Exception:
            # Never block the pickup log from being written.
            pass

    return "weekly" if in_week else "future"
