"""Week-range inference helpers.

We need to decide whether a shift is in the worksheet's *current week* versus a
future week.

UNH/MC tabs typically show a single-week schedule with the week range printed
somewhere in the header area (not always in the tab title).

On-Call tabs commonly include the week range in the tab title, e.g.:

  "On Call 1/25 - 1/31"

This module extracts a (week_start, week_end) date range from either:
  - worksheet title text, or
  - the visible header area of the worksheet.

Year handling:
  Most templates omit years. We infer years relative to "today" in
  America/Los_Angeles and handle year rollover (e.g., 12/30 - 1/5).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable, Optional
from zoneinfo import ZoneInfo

import gspread

from . import sheets_sections


LA_TZ = ZoneInfo("America/Los_Angeles")


MMDD_RE = re.compile(r"(?<!\d)(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?(?!\d)")
RANGE_RE = re.compile(
    r"(?<!\d)(\d{1,2}/\d{1,2}(?:/\d{2,4})?)\s*[\-–—]\s*(\d{1,2}/\d{1,2}(?:/\d{2,4})?)(?!\d)",
    re.I,
)

# Some templates use compact tokens without slashes in tab titles, e.g.:
#   "On Call 28 - 214"    -> 2/8 - 2/14
#   "On Call 928-104"    -> 9/28 - 10/4
#   "On Call 1012-1018"  -> 10/12 - 10/18
#
# We support these by parsing each side as a compact M/D token.
_COMPACT_RANGE_RE = re.compile(r"(?<!\d)(\d{2,4})\s*[\-–—]\s*(\d{2,4})(?!\d)")


def la_today() -> date:
    return datetime.now(LA_TZ).date()


def _coerce_year(y: int) -> int:
    if y < 100:
        # assume 20xx (good enough for this project)
        return 2000 + y
    return y


def _infer_year_for_mmdd(m: int, d: int, *, today: date) -> int:
    """Infer the most likely year for a month/day near `today`.

    We pick the year such that the resulting date is closest to `today`.
    """
    base = today.year
    candidates: list[date] = []
    for yy in (base - 1, base, base + 1):
        try:
            candidates.append(date(yy, m, d))
        except Exception:
            continue
    if not candidates:
        return base
    best = min(candidates, key=lambda dt: abs((dt - today).days))
    return best.year


def _date_from_token(token: str, *, today: date) -> Optional[date]:
    m = MMDD_RE.search(token or "")
    if not m:
        return None
    mm = int(m.group(1))
    dd = int(m.group(2))
    yy = m.group(3)
    try:
        if yy:
            yr = _coerce_year(int(yy))
        else:
            yr = _infer_year_for_mmdd(mm, dd, today=today)
        return date(yr, mm, dd)
    except Exception:
        return None


def _md_from_compact_token(tok: str) -> Optional[tuple[int, int]]:
    """Parse a compact M/D token like '28', '214', '1011'.

    Rules (matches the templates used in this project):
      - len==2:  M/D where M is first digit, D is second digit (e.g., 28 => 2/8)
      - len==3:
          - if starts with 10/11/12 => MM/D (e.g., 104 => 10/4)
          - else => M/DD (e.g., 214 => 2/14, 112 => 1/12)
      - len==4:  MM/DD (e.g., 1011 => 10/11)
    """
    s = re.sub(r"\D", "", str(tok or "").strip())
    if not s:
        return None
    if len(s) == 2:
        mm = int(s[0])
        dd = int(s[1])
    elif len(s) == 3:
        if s[:2] in {"10", "11", "12"}:
            mm = int(s[:2])
            dd = int(s[2])
        else:
            mm = int(s[0])
            dd = int(s[1:])
    elif len(s) == 4:
        mm = int(s[:2])
        dd = int(s[2:])
    else:
        return None

    if not (1 <= mm <= 12 and 1 <= dd <= 31):
        return None
    return mm, dd


def _date_from_compact_token(tok: str, *, today: date) -> Optional[date]:
    md = _md_from_compact_token(tok)
    if not md:
        return None
    mm, dd = md
    try:
        yr = _infer_year_for_mmdd(mm, dd, today=today)
        return date(yr, mm, dd)
    except Exception:
        return None


def _parse_range_tokens(a: str, b: str, *, today: date) -> Optional[tuple[date, date]]:
    da = _date_from_token(a, today=today)
    db = _date_from_token(b, today=today)
    if not da or not db:
        return None
    # Handle year rollover: 12/30 - 1/5
    if db < da:
        try:
            db = date(da.year + 1, db.month, db.day)
        except Exception:
            pass
    return da, db


def week_range_from_text(text: str, *, today: Optional[date] = None) -> Optional[tuple[date, date]]:
    """Extract a week range from arbitrary text, if present."""
    today = today or la_today()
    s = (text or "").strip()
    if not s:
        return None
    m = RANGE_RE.search(s)
    if m:
        return _parse_range_tokens(m.group(1), m.group(2), today=today)

    # Compact numeric form (no slashes), used by some On-Call tab titles.
    # Example: "On Call 28 - 214" -> 2/8 - 2/14
    m2 = _COMPACT_RANGE_RE.search(s)
    if m2:
        da = _date_from_compact_token(m2.group(1), today=today)
        db = _date_from_compact_token(m2.group(2), today=today)
        if da and db:
            if db < da:
                try:
                    db = date(da.year + 1, db.month, db.day)
                except Exception:
                    pass
            return da, db
    return None


def week_range_from_title(title: str, *, today: Optional[date] = None) -> Optional[tuple[date, date]]:
    return week_range_from_text(title, today=today)


def _cluster_week_dates(dates: Iterable[date]) -> Optional[tuple[date, date]]:
    ds = sorted({d for d in dates if isinstance(d, date)})
    if len(ds) < 2:
        return None

    # Find the densest cluster whose span is <= 6 days.
    best: Optional[tuple[int, int]] = None
    for i in range(len(ds)):
        for j in range(i, len(ds)):
            span = (ds[j] - ds[i]).days
            if span > 6:
                break
            if best is None or (j - i) > (best[1] - best[0]):
                best = (i, j)
    if best is None:
        return None
    i, j = best
    return ds[i], ds[j]


def week_range_from_worksheet(
    ws: gspread.Worksheet,
    *,
    today: Optional[date] = None,
    scan_rows: int = 35,
    scan_cols: int = 30,
) -> Optional[tuple[date, date]]:
    """Infer (week_start, week_end) from a worksheet.

    Strategy:
      1) Try the worksheet title (works well for On-Call).
      2) Scan the top-left header area for an explicit "M/D - M/D" range.
      3) Otherwise, collect M/D tokens from the header area and pick a 7-day cluster.
    """
    today = today or la_today()

    # 1) Title
    wr = week_range_from_title(getattr(ws, "title", ""), today=today)
    if wr:
        return wr

    # 2/3) Header scan
    grid = sheets_sections.read_top_grid(ws, max_rows=scan_rows, max_cols=scan_cols)
    if not grid:
        return None

    # (2) explicit range string
    for r in grid:
        for cell in r:
            if not cell:
                continue
            m = RANGE_RE.search(str(cell))
            if m:
                wr2 = _parse_range_tokens(m.group(1), m.group(2), today=today)
                if wr2:
                    return wr2

    # (3) collect tokens and cluster
    found: list[date] = []
    for r in grid:
        for cell in r:
            if not cell:
                continue
            for mm in MMDD_RE.finditer(str(cell)):
                tok = mm.group(0)
                d = _date_from_token(tok, today=today)
                if d:
                    found.append(d)
    return _cluster_week_dates(found)


def date_for_weekday(week_start: date, week_end: date, weekday_canon: str) -> Optional[date]:
    """Return the date within [week_start, week_end] that matches the weekday."""
    want = (weekday_canon or "").strip().lower()
    cur = week_start
    while cur <= week_end:
        if cur.strftime("%A").strip().lower() == want:
            return cur
        cur = cur + timedelta(days=1)
    return None
