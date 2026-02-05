
from __future__ import annotations
import os
import re
from typing import Iterable, List, Optional, Tuple, Dict

import streamlit as st
import gspread
import gspread.utils as a1

from ..config import (
    OA_SCHEDULE_SHEETS,   # ["UNH ...", "MC ..."]
    AUDIT_SHEET,
    LOCKS_SHEET,
    ONCALL_MAX_COLS,
    ONCALL_MAX_ROWS,
    DAY_CACHE_TTL_SEC,
    ONCALL_SHEET_OVERRIDE,  # optional override of On-Call tab name
)
from .quotas import _safe_batch_get, read_day_column_map_cached


# ──────────────────────────────────────────────────────────────────────────────
# Debug controls (no-UI): secrets/env/session_state
# ──────────────────────────────────────────────────────────────────────────────

def _hours_debug_enabled() -> bool:
    """Return True if the caller explicitly enabled slow/verbose counting mode.
    This only affects *how* we count UNH/MC (fast vs grid), not the resulting totals.
    """
    try:
        if bool(st.session_state.get("HOURS_DEBUG")):
            return True
    except Exception:
        pass
    if str(os.environ.get("HOURS_DEBUG", "")).strip() not in ("", "0", "false", "False"):
        return True
    try:
        return bool(st.secrets.get("hours_debug", False))
    except Exception:
        return False


# ──────────────────────────────────────────────────────────────────────────────
# Resolve the three tabs we total over: UNH, MC, and On-Call (neighbor to MC)
# ──────────────────────────────────────────────────────────────────────────────

_DENY_LOW = {AUDIT_SHEET.strip().lower(), LOCKS_SHEET.strip().lower()}


@st.cache_data(ttl=120, show_spinner=False)
def _cached_visible_titles(ss_id: str) -> list[str]:
    """Visible worksheet titles in UI order.

    Avoid calling `ss.worksheets()` on every rerun (very slow). We reuse the
    Spreadsheet handle stored in session_state.
    """
    ss = st.session_state.get("_SS_HANDLE_BY_ID", {}).get(ss_id)
    if not ss:
        return []
    try:
        ws_all = ss.worksheets()
    except Exception:
        return []
    titles: list[str] = []
    for w in ws_all:
        try:
            hidden = bool(getattr(w, "_properties", {}).get("hidden", False))
        except Exception:
            hidden = False
        if hidden:
            continue
        t = str(getattr(w, "title", "") or "").strip()
        if not t:
            continue
        if t.lower() in _DENY_LOW:
            continue
        titles.append(t)
    return titles

def _resolve_title(actuals: List[gspread.Worksheet], wanted: str) -> str | None:
    wanted_low = (wanted or "").strip().lower()
    by_low = {w.title.strip().lower(): w.title for w in actuals}
    if wanted_low in by_low:
        return by_low[wanted_low]
    first = wanted_low.split()[0] if wanted_low else ""
    for w in actuals:
        t = w.title.strip(); tl = t.lower()
        if tl == wanted_low or (first and tl.startswith(first)):
            return t
    return None

def _three_titles_unh_mc_oncall(ss: gspread.Spreadsheet) -> list[str]:
    ss_id = getattr(ss, "id", "")
    titles = _cached_visible_titles(ss_id)
    if not titles:
        # Fallback for very first run / auth hiccups.
        try:
            titles = [w.title for w in ss.worksheets() if not bool(getattr(w, "_properties", {}).get("hidden", False))]
        except Exception:
            return []

    unh_cfg, mc_cfg = OA_SCHEDULE_SHEETS[0], OA_SCHEDULE_SHEETS[1]

    def _resolve_from_titles(wanted: str) -> str | None:
        want = (wanted or "").strip().lower()
        by_low = {t.strip().lower(): t for t in titles}
        if want in by_low:
            return by_low[want]
        first = want.split()[0] if want else ""
        for t in titles:
            tl = t.lower()
            if tl == want or (first and tl.startswith(first)):
                return t
        return None

    unh_title = _resolve_from_titles(unh_cfg)
    mc_title = _resolve_from_titles(mc_cfg)

    out: list[str] = []
    if unh_title:
        out.append(unh_title)
    if mc_title:
        out.append(mc_title)

    # Prefer explicit override; else pick the visible neighbor to MC’s right.
    oncall_title = None
    if mc_title:
        if ONCALL_SHEET_OVERRIDE and ONCALL_SHEET_OVERRIDE.strip():
            oncall_title = _resolve_from_titles(ONCALL_SHEET_OVERRIDE)
        else:
            try:
                idx = titles.index(mc_title)
            except ValueError:
                idx = -1
            if idx >= 0:
                j = idx + 1
                while j < len(titles):
                    cand = titles[j]
                    if cand.strip().lower() not in _DENY_LOW:
                        oncall_title = cand
                        break
                    j += 1

    if oncall_title:
        out.append(oncall_title)

    # De-dup
    seen, final = set(), []
    for t in out:
        if t and t not in seen:
            seen.add(t); final.append(t)
    return final

# ──────────────────────────────────────────────────────────────────────────────
# Name matching (handles "OA: Name", "GOA: Name", "Name1 & Name2")
# ──────────────────────────────────────────────────────────────────────────────

_SPLIT_RE = re.compile(r"[,\n/&+]|(?:\s+\band\b\s+)", re.I)
_PREFIX_RE = re.compile(r"^\s*(?:OA|GOA|On[-\s]*Call)\s*:\s*", re.I)

def _canon(s: str) -> str:
    s = _PREFIX_RE.sub("", s or "")
    return " ".join("".join(ch for ch in s.lower() if ch.isalnum() or ch.isspace()).split())

def _cell_mentions_person(cell_value: str, canon_name: str) -> bool:
    if not cell_value:
        return False
    target = _canon(canon_name)
    if _canon(cell_value) == target:
        return True
    parts: Iterable[str] = (p.strip() for p in _SPLIT_RE.split(str(cell_value)) if p.strip())
    return any(_canon(p) == target for p in parts)


# ──────────────────────────────────────────────────────────────────────────────
# UNH/MC: generic half-hour grid counter (0.5h per matched cell)
# ──────────────────────────────────────────────────────────────────────────────

def _count_half_hour_grid(ws: gspread.Worksheet, canon_name: str) -> float:
    end_col_letter = a1.rowcol_to_a1(1, ONCALL_MAX_COLS).split("1")[0]
    values = _safe_batch_get(ws, [f"A1:{end_col_letter}{ONCALL_MAX_ROWS}"])[0] or []
    total = 0.0
    for row in values:
        for cell in (row or []):
            if _cell_mentions_person(str(cell), canon_name):
                total += 0.5
    return total


# ──────────────────────────────────────────────────────────────────────────────
# On-Call by day headers (Mon–Fri=5h, Sat/Sun=4h, unknown→assume weekday=5h)
# ──────────────────────────────────────────────────────────────────────────────

_DAY_ALIASES = {
    "monday": "monday", "mon": "monday",
    "tuesday": "tuesday", "tue": "tuesday", "tues": "tuesday",
    "wednesday": "wednesday", "wed": "wednesday",
    "thursday": "thursday", "thu": "thursday", "thur": "thursday", "thurs": "thursday",
    "friday": "friday", "fri": "friday",
    "saturday": "saturday", "sat": "saturday",
    "sunday": "sunday", "sun": "sunday",
}
_WEEKDAYS = {"monday","tuesday","wednesday","thursday","friday"}

def _normalize_day(s: str) -> Optional[str]:
    s = (s or "").strip().lower()
    s_clean = "".join(ch for ch in s if ch.isalpha() or ch.isspace())
    tokens = {tok for tok in s_clean.split() if tok}
    for tok in list(tokens):
        if tok in _DAY_ALIASES:
            return _DAY_ALIASES[tok]
    return None

def _find_header_row_with_days(values: List[List[str]], max_scan_rows: int = 10) -> Tuple[Optional[int], Dict[int, str]]:
    rows_to_scan = values[:max_scan_rows]
    for r, row in enumerate(rows_to_scan):
        colmap: Dict[int, str] = {}
        hits = 0
        for c, cell in enumerate(row or []):
            day = _normalize_day(str(cell))
            if day:
                colmap[c] = day
                hits += 1
        if hits >= 2:
            return r, colmap
    return None, {}

def _count_oncall_by_day_headers(ws: gspread.Worksheet, canon_name: str) -> float:
    end_col_letter = a1.rowcol_to_a1(1, ONCALL_MAX_COLS).split("1")[0]
    grid = _safe_batch_get(ws, [f"A1:{end_col_letter}{ONCALL_MAX_ROWS}"])[0] or []

    header_r, day_by_col = _find_header_row_with_days(grid)

    def weight_for_col(cidx: int) -> float:
        day = day_by_col.get(cidx)
        if not day:
            return 5.0  # missing/unknown header → weekday weight
        return 5.0 if day in _WEEKDAYS else 4.0

    total = 0.0
    # If no header: every mention gets 5h
    if header_r is None:
        for row in grid:
            for cell in (row or []):
                if _cell_mentions_person(str(cell), canon_name):
                    total += 5.0
        return total

    # With header: count below it using column-specific weights
    for r in range(header_r + 1, len(grid)):
        row = grid[r] or []
        for c, cell in enumerate(row, start=1):
            if _cell_mentions_person(str(cell), canon_name):
                total += weight_for_col(c - 1)
    return total


# ──────────────────────────────────────────────────────────────────────────────
# Cache-busting for strict recomputes
# ──────────────────────────────────────────────────────────────────────────────

def _clear_ws_cache_for_titles(ss: gspread.Spreadsheet, schedule, titles: list[str]) -> None:
    cache = st.session_state.setdefault("WS_RANGE_CACHE", {})
    ws_ids = set()
    for t in titles:
        try:
            info = schedule._get_sheet(t)
            ws_ids.add(getattr(info.ws, "id", info.ws.title))
        except Exception:
            try:
                ws = ss.worksheet(t)
                ws_ids.add(getattr(ws, "id", ws.title))
            except Exception:
                pass
    for key in list(cache.keys()):
        # WS_RANGE_CACHE key shape changed over time.
        # Old: (ws_id, ranges)
        # New: (ws_id, ws_version, ranges)
        # We only care about the worksheet id, so grab element 0 when possible.
        wid = None
        if isinstance(key, (tuple, list)) and len(key) >= 1:
            try:
                wid = str(key[0])
            except Exception:
                wid = None
        if wid and wid in ws_ids:
            cache.pop(key, None)


# ──────────────────────────────────────────────────────────────────────────────
# EXPORTED API (unchanged signatures)
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def compute_hours_fast(_ss, _schedule, canon_name: str, epoch) -> float:
    """
    Cached sidebar metric. Returns the **uncapped** total hours for the week.
    UNH + MC: 0.5h per cell
    On-Call: 5h per mention in Mon–Fri columns; 4h in Sat/Sun; if no header → 5h.
    Notes:
      - `epoch` is only used as a cache key. The UI should pass a value derived
        from worksheet versions (e.g., a tuple of (title, version) pairs). This
        keeps the sidebar fast while still updating immediately after writes.
    """
    titles = _three_titles_unh_mc_oncall(_ss)
    if len(titles) < 2:
        return 0.0

    total_unh = total_mc = total_on = 0.0

    # 1) UNH + MC
    #
    # We intentionally use the full-grid counter here. It is *one* batch_get per
    # sheet (read-through cached by worksheet version) and is the most robust
    # across the various sheet layouts you have in production.
    for idx, label in enumerate(("UNH", "MC")):
        if len(titles) <= idx:
            continue
        t = titles[idx]
        try:
            ws = _ss.worksheet(t)
            subtotal = _count_half_hour_grid(ws, canon_name)
        except Exception:
            subtotal = 0.0
        if label == "UNH":
            total_unh = subtotal
        else:
            total_mc = subtotal

    # 2) On-Call (neighbor to MC)
    if len(titles) >= 3:
        try:
            ws_on = _ss.worksheet(titles[2])
            total_on = _count_oncall_by_day_headers(ws_on, canon_name)
        except Exception:
            total_on = 0.0

    return total_unh + total_mc + total_on


def invalidate_hours_caches():
    st.session_state["HOURS_EPOCH"] = st.session_state.get("HOURS_EPOCH", 0) + 1


def total_hours_from_unh_mc_and_neighbor(_ss: gspread.Spreadsheet, _schedule, canon_name: str) -> float:
    """
    Fresh (non-cached) strict total used for the 20h cap check when adding shifts.
    Returns the **uncapped** total.
    """
    # "Strict" total used for cap checks. We no longer hard-clear caches here
    # because it forces an expensive full refetch on every add/remove.
    # Read-through caching is keyed by worksheet version; writes bump version.
    titles = _three_titles_unh_mc_oncall(_ss)

    total_unh = total_mc = total_on = 0.0

    # UNH + MC (fresh)
    for idx, label in enumerate(("UNH", "MC")):
        if len(titles) <= idx:
            continue
        t = titles[idx]
        try:
            ws = _ss.worksheet(t)
            # In fresh mode, prefer raw grid
            subtotal = _count_half_hour_grid(ws, canon_name)
        except Exception:
            subtotal = 0.0
        if label == "UNH":
            total_unh = subtotal
        else:
            total_mc = subtotal

    # Neighbor (On-Call)
    if len(titles) >= 3:
        try:
            ws_on = _ss.worksheet(titles[2])
            total_on = _count_oncall_by_day_headers(ws_on, canon_name)
        except Exception:
            total_on = 0.0

    return total_unh + total_mc + total_on
