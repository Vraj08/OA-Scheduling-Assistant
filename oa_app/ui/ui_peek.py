import pandas as pd
import streamlit as st
import re
import gspread.utils as a1
from datetime import datetime
from dateutil import parser as dateparser
from ..config import ONCALL_MAX_COLS, ONCALL_MAX_ROWS
from ..core.quotas import read_cols_exact, _safe_batch_get


_HEADER_KEYWORDS = {
    "time",
    "name",
    "role",
    "email",
    "phone",
    "campus",
    "day",
    "start",
    "end",
    "details",
    "status",
    "requester",
    "action",
    "reviewedby",
    "reviewedat",
    "id",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
}
_DATEISH_RE = re.compile(r"\b\d{1,4}\s*[-/]\s*\d{1,2}(?:\s*[-/]\s*\d{1,4})?\b")
_TIMEISH_RE = re.compile(r"\b\d{1,2}:\d{2}\s*(?:am|pm)\b", re.I)


def _unique_display_headers(hdr):
    """Make worksheet headers safe for Streamlit/PyArrow rendering."""
    out = []
    seen = set()
    counts = {}

    for idx, raw in enumerate(hdr or [], start=1):
        base = str(raw or "").strip() or f"Column {idx}"
        counts[base] = counts.get(base, 0) + 1
        candidate = base if counts[base] == 1 else f"{base} ({counts[base]})"
        while candidate in seen:
            counts[base] += 1
            candidate = f"{base} ({counts[base]})"
        seen.add(candidate)
        out.append(candidate)

    return out


def _row_looks_like_header(hdr):
    cells = [str(c or "").strip() for c in (hdr or [])]
    nonempty = [c for c in cells if c]
    if not nonempty:
        return False
    if len(nonempty) < len(cells):
        return True

    for cell in nonempty:
        low = cell.lower()
        if low in _HEADER_KEYWORDS:
            return True
        if any(tok in _HEADER_KEYWORDS for tok in re.findall(r"[a-z]+", low)):
            return True
        if _DATEISH_RE.search(low) or _TIMEISH_RE.search(low):
            return True
    return False


def _df_from_grid(vals):
    """Convert a gspread batch_get grid (list of rows) into a dataframe."""
    if not vals:
        return pd.DataFrame()
    hdr = vals[0] if vals else []
    body = vals[1:] if len(vals) > 1 else []
    # If the first row looks like a header, use it.
    if _row_looks_like_header(hdr):
        w = len(hdr)
        norm = [r + [""] * (w - len(r)) if len(r) < w else r[:w] for r in body]
        return pd.DataFrame(norm, columns=_unique_display_headers(hdr))
    width = max(len(r or []) for r in vals) if vals else 0
    if width <= 0:
        return pd.DataFrame()
    norm = [(r or []) + [""] * (width - len(r or [])) for r in vals]
    return pd.DataFrame(norm, columns=[f"Column {idx}" for idx in range(1, width + 1)])


def _render_sheet_as_table(ss, title: str, *, max_cols: int = ONCALL_MAX_COLS, max_rows: int = ONCALL_MAX_ROWS):
    """Render any worksheet as a dataframe (no schedule parsing)."""
    try:
        ws = ss.worksheet(title)
    except Exception as e:
        st.warning(f"Could not open worksheet '{title}': {e}")
        return

    end_col = a1.rowcol_to_a1(1, max_cols).split("1")[0]
    vals = _safe_batch_get(ws, [f"A1:{end_col}{max_rows}"])[0]
    if not vals:
        st.info("This worksheet is empty.")
        return

    df = _df_from_grid(vals)
    st.dataframe(df, height=520, use_container_width=True)


def peek_generic_sheet(ss, title: str, *, max_cols: int = ONCALL_MAX_COLS, max_rows: int = ONCALL_MAX_ROWS):
    """Fallback peek for non-schedule tabs (Policies, Notes, etc.)."""
    with st.expander(f"Peek (as table): {title}"):
        _render_sheet_as_table(ss, title, max_cols=max_cols, max_rows=max_rows)

def peek_exact(schedule, tab_titles: list[str]):
    # Renders MC/UNH style (Mon–Sun header) sheets.
    # Creates its own expander; don't wrap it in another one.
    with st.expander("Peek (exactly as in sheet)"):
        tab = st.selectbox("Campus tab", tab_titles, index=0, key="peek_tab_raw")
        try:
            # Pass the *full* tab title. Using split()[0] can accidentally select
            # non-schedule tabs that happen to share a prefix (e.g., "EO ...").
            info = schedule._get_sheet(tab)
        except RuntimeError as e:
            # Most common: "Could not read weekday header" for a Policies/Notes sheet.
            st.info(f"This tab doesn't look like a schedule grid: {e}")
            _render_sheet_as_table(schedule.ss, tab)
            return
        view_mode = st.radio("View", ["Selected day", "All days"], horizontal=True, key="peek_view_raw")
        max_rows = st.number_input("Max rows to show (0 = all)", min_value=0, value=0, step=1, key="peek_rows_raw")

        total_rows = info.ws.row_count or 2000
        start_r = 2 if total_rows >= 2 else 1
        end_r = total_rows

        if not getattr(info, "header_map", None):
            st.info("No weekday headers detected in this worksheet.")
            _render_sheet_as_table(schedule.ss, tab)
            return

        if view_mode == "Selected day":
            day = st.selectbox("Day", [d.title() for d in sorted(info.header_map.keys())], key="peek_day_raw")
            if day.lower() not in info.header_map:
                st.info("Could not find that day in this worksheet.")
            else:
                cols = [1, info.header_map[day.lower()]]
                data = read_cols_exact(info.ws, start_r, end_r, cols)
                out_time, out_day = data[1], data[cols[1]]
                if max_rows and max_rows > 0:
                    out_time = out_time[:max_rows]; out_day = out_day[:max_rows]
                st.dataframe(pd.DataFrame({"Time": out_time, day.title(): out_day}), height=520, use_container_width=True)
        else:
            order = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
            day_cols = [(d, info.header_map[d]) for d in order if d in info.header_map]
            if not day_cols:
                st.info("No weekday headers detected in this worksheet.")
            else:
                cols = [1] + [c for _, c in day_cols]
                data = read_cols_exact(info.ws, start_r, end_r, cols)
                out = {"Time": data[1]}
                for d, c in day_cols:
                    out[d.title()] = data[c]
                if max_rows and max_rows > 0:
                    for k in list(out.keys()): out[k] = out[k][:max_rows]
                st.dataframe(pd.DataFrame(out), height=520, use_container_width=True)

def peek_oncall(ss):
    # Multi-select viewer for any visible On-Call sheets (kept for your existing flows).
    with st.expander("Peek On-Call (weekly sheets, as-is)"):
        try:
            all_ws = ss.worksheets()
        except Exception as e:
            st.warning(f"Could not list worksheets: {e}")
            return
        oc_ws = [w for w in all_ws if re.search(r"\bon\s*[- ]?\s*call\b", w.title, flags=re.I)]
        if not oc_ws:
            st.info("No visible On-Call worksheets found.")
            return

        def _parse_title_date(t: str):
            try:
                m = re.search(r"(\d{4}[-/]\d{1,2}[-/]\d{1,2}|[A-Za-z]{3,9}\s+\d{1,2}(?:,\s*\d{4})?)", t)
                if m: return dateparser.parse(m.group(1))
            except Exception:
                return None
            return None

        oc_ws.sort(key=lambda w: (_parse_title_date(w.title) or datetime.min, w.title), reverse=True)
        titles = [w.title for w in oc_ws]
        sel = st.selectbox("On-Call sheet", titles, index=0, key="oncall_sel")
        ws = next(w for w in oc_ws if w.title == sel)

        end_col = a1.rowcol_to_a1(1, ONCALL_MAX_COLS).split("1")[0]
        vals = _safe_batch_get(ws, [f"A1:{end_col}{ONCALL_MAX_ROWS}"])[0]
        if not vals:
            st.info("This On-Call worksheet is empty."); return
        df = _df_from_grid(vals)
        st.dataframe(df, height=520, use_container_width=True)

def peek_oncall_single(ss, title: str):
    # Focused viewer for exactly one On-Call sheet (used when user selects a single tab in sidebar).
    with st.expander(f"Peek (On-Call): {title}"):
        try:
            ws = ss.worksheet(title)
        except Exception as e:
            st.warning(f"Could not open worksheet '{title}': {e}")
            return

        end_col = a1.rowcol_to_a1(1, ONCALL_MAX_COLS).split("1")[0]
        vals = _safe_batch_get(ws, [f"A1:{end_col}{ONCALL_MAX_ROWS}"])[0]
        if not vals:
            st.info("This On-Call worksheet is empty."); return

        df = _df_from_grid(vals)

        st.dataframe(df, height=520, use_container_width=True)
