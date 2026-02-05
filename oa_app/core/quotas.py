import time as _pytime
from collections import OrderedDict
from typing import Dict, Iterable, List, Tuple

import gspread
import streamlit as st
import gspread.utils as a1


# ---- Cache + invalidation helpers ----

# Keep caches bounded so long Streamlit sessions don't bloat.
_RANGE_CACHE_MAX = 256  # across all worksheets/ranges in a session


def _ws_id(ws) -> str:
    return str(getattr(ws, "id", ws.title))


def _get_ws_version_map() -> Dict[str, int]:
    return st.session_state.setdefault("WS_VER", {})


def bump_ws_version(ws) -> None:
    """Call after any write/format to this worksheet.

    We version the worksheet so read caches can be safely reused without returning stale data.
    """
    ver = _get_ws_version_map()
    wid = _ws_id(ws)
    newv = int(ver.get(wid, 0)) + 1
    ver[wid] = newv
    # Also index by title when we have one. This allows callers (esp. UI)
    # to key caches off tab titles without needing extra API calls.
    try:
        title = str(getattr(ws, "title", "") or "").strip()
        if title:
            ver[title] = newv
    except Exception:
        pass


def get_ws_version(ws_or_title) -> int:
    """Return the current version for a worksheet (or title).

    We version worksheets in-session to safely reuse cached reads.
    Accepts either a gspread.Worksheet or a string (title / id).
    """
    ver = _get_ws_version_map()
    if ws_or_title is None:
        return 0
    if isinstance(ws_or_title, str):
        return int(ver.get(ws_or_title, 0))
    try:
        return int(ver.get(_ws_id(ws_or_title), 0))
    except Exception:
        try:
            return int(ver.get(str(getattr(ws_or_title, "title", "")), 0))
        except Exception:
            return 0


def seed_batch_get_cache(ws, ranges: list[str], vals: list) -> None:
    """Seed the read-through batch_get cache for the worksheet's *current* version.

    After a write, we bump the worksheet version (so we never serve stale cached
    reads). That bump would normally force the next read to hit the API again.

    However, in our add/remove flows we already have an updated in-memory grid
    (because we read the grid to locate lanes/slots). Seeding lets the UI
    rerun re-use that grid immediately, avoiding an expensive refetch.

    `vals` must match gspread's `ws.batch_get(...)` return shape.
    Example: for a single range, pass `[grid]`.
    """
    cache = _range_cache()
    wid = _ws_id(ws)
    ver = _get_ws_version_map().get(wid, 0)
    key = (wid, ver, tuple(ranges))
    cache[key] = vals
    cache.move_to_end(key)
    while len(cache) > _RANGE_CACHE_MAX:
        cache.popitem(last=False)


def _range_cache() -> "OrderedDict[Tuple[str,int,Tuple[str,...]], list]":
    # LRU-ish cache: OrderedDict where newest is at the end
    return st.session_state.setdefault("WS_RANGE_CACHE", OrderedDict())

def _safe_batch_get(ws, ranges, *, retries: int = 4, backoff: float = 0.7):
    # Read-through cache keyed by (worksheet, version, ranges).
    # Version bumps on any write so we never serve stale reads.
    cache = _range_cache()
    wid = _ws_id(ws)
    ver = _get_ws_version_map().get(wid, 0)
    key = (wid, ver, tuple(ranges))
    if key in cache:
        cache.move_to_end(key)
        return cache[key]
    for i in range(retries):
        try:
            vals = ws.batch_get(ranges, major_dimension="ROWS")
            cache[key] = vals
            cache.move_to_end(key)
            # bound cache size
            while len(cache) > _RANGE_CACHE_MAX:
                cache.popitem(last=False)
            return vals
        except Exception as e:
            if "429" in str(e) or "Quota exceeded" in str(e):
                _pytime.sleep(backoff * (2 ** i)); continue
            raise
    vals = ws.batch_get(ranges, major_dimension="ROWS")
    cache[key] = vals
    cache.move_to_end(key)
    while len(cache) > _RANGE_CACHE_MAX:
        cache.popitem(last=False)
    return vals

def read_cols_exact(ws, start_row: int, end_row: int, col_indices: list[int]) -> dict[int, list[str]]:
    if end_row < start_row:
        return {c: [] for c in col_indices}
    a1s = [f"{a1.rowcol_to_a1(start_row,c)}:{a1.rowcol_to_a1(end_row,c)}" for c in col_indices]
    blocks = _safe_batch_get(ws, a1s)
    need = end_row - start_row + 1
    out = {}
    for c, block in zip(col_indices, blocks):
        col_vals = [r[0] if r else "" for r in (block or [])]
        if len(col_vals) < need: col_vals += [""] * (need - len(col_vals))
        out[c] = col_vals[:need]
    return out

def first_row(ws, max_cols: int) -> list[str]:
    end_a1 = a1.rowcol_to_a1(1, max_cols)
    block = _safe_batch_get(ws, [f"A1:{end_a1}"])[0]
    return block[0] if block else []

def read_day_column_map_cached(info, col: int, ttl_sec: int):
    return read_col_band_map_cached(info, col, info.day_min_row, info.day_max_row, ttl_sec)


def read_col_band_map_cached(info, col: int, band_min_row: int, band_max_row: int, ttl_sec: int):
    """Read a single column for a band of rows and cache it.

    This is the hot-path read for searching a lane/window. Keeping the band tight makes calls faster
    and makes caching more effective.
    """
    # self-initialize day cache
    day_cache = st.session_state.setdefault("DAY_CACHE", {})
    ws_id = _ws_id(info.ws)
    ver = _get_ws_version_map().get(ws_id, 0)
    key = (ws_id, ver, col, int(band_min_row), int(band_max_row))
    now = _pytime.time()
    entry = day_cache.get(key)
    if entry and (now - entry[0]) <= ttl_sec:
        return entry[1]
    if band_max_row < band_min_row:
        return {}
    rng = f"{a1.rowcol_to_a1(band_min_row,col)}:{a1.rowcol_to_a1(band_max_row,col)}"
    block = _safe_batch_get(info.ws, [rng])[0]
    flat = [(r[0] if r and len(r)>0 else "") for r in (block or [])]
    mapping = {band_min_row + i: v for i, v in enumerate(flat)}
    day_cache[key] = (now, mapping)
    return mapping

def invalidate_day_cache(info, col: int):
    # bump version (invalidates range cache + all band caches for this sheet)
    bump_ws_version(info.ws)
    # best-effort cleanup of old entries for this worksheet
    day_cache = st.session_state.setdefault("DAY_CACHE", {})
    wsid = _ws_id(info.ws)
    for k in list(day_cache.keys()):
        if isinstance(k, tuple) and len(k) >= 2 and k[0] == wsid:
            day_cache.pop(k, None)
