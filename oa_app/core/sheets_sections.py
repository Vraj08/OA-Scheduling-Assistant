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
    """Normalization used for header matching.

    We intentionally tolerate minor punctuation differences across sheet templates.
    """
    s = (s or "").strip().lower()
    # Keep slashes (used in some headers), but otherwise strip punctuation.
    s = re.sub(r"[^\w\s/]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def find_header_cells(grid: list[list[str]], header_text: str) -> list[tuple[int, int]]:
    """Return all (row, col) 1-based header matches within a grid."""
    want = _norm(header_text)
    if not want or not grid:
        return []
    # Flexible matching strategy:
    #   - exact match (after normalization)
    #   - tolerate slash vs space
    #   - allow trailing text (e.g., "... (pending)")
    # This is intentionally more permissive because templates evolve.
    want2 = want.replace("/", " ")
    hits: list[tuple[int, int]] = []
    R = len(grid)
    C = max((len(r) for r in grid), default=0)
    for r in range(R):
        row = grid[r]
        for c in range(min(C, len(row))):
            v = row[c] if c < len(row) else ""
            if not v:
                continue
            cell = _norm(str(v))
            cell2 = cell.replace("/", " ")
            if cell == want or cell2 == want2 or cell2.startswith(want2) or (want2 and want2 in cell2):
                hits.append((r + 1, c + 1))
    return hits


def find_header_cell_best(grid: list[list[str]], header_text: str) -> tuple[int, int] | None:
    """Return the "best" header match.

    Many templates contain duplicate labels (e.g., once in a legend/footer).
    The swap/callout sections are typically positioned to the *right* of the
    schedule grid, so we prefer the rightmost match, breaking ties by choosing
    the topmost.
    """
    hits = find_header_cells(grid, header_text)
    if not hits:
        return None
    # Prefer rightmost (highest column), then topmost (lowest row).
    return sorted(hits, key=lambda rc: (-rc[1], rc[0]))[0]


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
        return ws.get(rng) or []
    except Exception:
        # Avoid ws.get_all_values(): it can be extremely heavy and may trigger
        # Google Sheets read-quota (429) errors. If the bounded read fails,
        # return empty and let the caller decide what to do.
        return []


def find_header_cell(grid: list[list[str]], header_text: str) -> tuple[int, int] | None:
    """Return (row, col) 1-based for the best matching header cell."""
    return find_header_cell_best(grid, header_text)


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
