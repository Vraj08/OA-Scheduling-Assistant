"""Helpers for locating and writing to named sections inside Google Sheets.

The OA schedule workbook contains "side" sections like:
  - "Shift Swaps for the week"
  - "Future Swaps/Call outs"

These sections may move as the template evolves. We therefore locate them by
header text rather than hard-coded row numbers.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable

import gspread
import gspread.utils as a1


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


@dataclass(frozen=True)
class Section:
    """A rectangular section anchored under a header cell."""

    header_text: str
    header_row: int  # 1-based
    header_col: int  # 1-based
    start_row: int   # 1-based (first data row)
    start_col: int   # 1-based
    max_rows: int
    num_cols: int

    def a1_range(self) -> str:
        end_row = self.start_row + self.max_rows - 1
        end_col = self.start_col + self.num_cols - 1
        tl = a1.rowcol_to_a1(self.start_row, self.start_col)
        br = a1.rowcol_to_a1(end_row, end_col)
        return f"{tl}:{br}"


def read_top_grid(ws: gspread.Worksheet, *, max_rows: int = 250, max_cols: int = 80) -> list[list[str]]:
    """Read a top-left grid range (best-effort) for header searching."""
    end_col_letter = a1.rowcol_to_a1(1, max_cols).split("1")[0]
    rng = f"A1:{end_col_letter}{max_rows}"
    try:
        values = ws.get(rng)
    except Exception:
        # Fallback: get_all_values can be heavier but may succeed.
        values = ws.get_all_values()
        values = values[:max_rows]
        values = [row[:max_cols] for row in values]
    return values or []


def find_header_cell(grid: list[list[str]], header_text: str) -> tuple[int, int] | None:
    """Return (row, col) 1-based for the first matching header cell."""
    want = _norm(header_text)
    if not want or not grid:
        return None
    # tolerate minor punctuation differences
    want_re = re.compile(r"^" + re.escape(want).replace("/", r"[/ ]").replace(" ", r"\s*") + r"$", re.I)
    R = len(grid)
    C = max((len(r) for r in grid), default=0)
    for r in range(R):
        row = grid[r]
        for c in range(min(C, len(row))):
            v = row[c] if c < len(row) else ""
            if not v:
                continue
            if want_re.match(_norm(str(v))):
                return (r + 1, c + 1)
    return None


def compute_section(header_row: int, header_col: int, *, max_rows: int = 200, num_cols: int = 8) -> Section:
    """Compute a section rectangle under a header cell."""
    return Section(
        header_text="",
        header_row=header_row,
        header_col=header_col,
        start_row=header_row + 1,
        start_col=header_col,
        max_rows=max_rows,
        num_cols=num_cols,
    )


def pad_rows(rows: list[list[str]], num_cols: int) -> list[list[str]]:
    """Pad (or trim) each row to exactly num_cols columns."""
    out: list[list[str]] = []
    for r in rows:
        rr = list(r[:num_cols])
        if len(rr) < num_cols:
            rr.extend([""] * (num_cols - len(rr)))
        out.append(rr)
    return out


def blanks(max_rows: int, num_cols: int) -> list[list[str]]:
    return [[""] * num_cols for _ in range(max_rows)]
