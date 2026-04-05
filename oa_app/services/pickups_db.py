"""Pickups DB helpers (Supabase).

Backend-only table:
  public.pickups

This module is intentionally small:
- Safe to import when Supabase is not configured.
- Provides idempotent upsert keyed by approval_id.
- Provides a simple weekly sum query for hours adjustments.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from ..core.utils import name_key
from ..integrations.supabase_io import get_supabase, supabase_enabled, with_retry


def supabase_pickups_enabled() -> bool:
    return supabase_enabled()


def upsert_pickup(payload: dict) -> dict:
    """Upsert a pickup row keyed by approval_id.

    Returns inserted/updated row dict (best-effort).
    """
    if not supabase_pickups_enabled():
        return {}
    sb = get_supabase()
    resp = with_retry(lambda: sb.table("pickups").upsert(payload, on_conflict="approval_id").execute())
    data = getattr(resp, "data", None) or []
    return data[0] if data else {}


def list_pickups_for_week(*, picker_name: str, week_start: date, week_end: date) -> list[dict[str, Any]]:
    """Return current-week pickup rows for one picker (best-effort)."""
    if not supabase_pickups_enabled():
        return []
    sb = get_supabase()
    picker = (picker_name or "").strip()
    resp = with_retry(
        lambda: sb.table("pickups")
        .select("event_date,duration_hours,picker_name")
        .gte("event_date", str(week_start))
        .lte("event_date", str(week_end))
        .execute()
    )
    rows: list[dict[str, Any]] = getattr(resp, "data", None) or []
    target_k = name_key(picker)
    out: list[dict[str, Any]] = []
    for r in rows:
        if name_key(str(r.get("picker_name", ""))) != target_k:
            continue
        out.append(r)
    return out


def sum_pickup_hours_for_week(*, picker_name: str, week_start: date, week_end: date) -> float:
    """Sum duration_hours for a picker within [week_start, week_end] (inclusive)."""
    total = 0.0
    for r in list_pickups_for_week(picker_name=picker_name, week_start=week_start, week_end=week_end):
        try:
            total += float(r.get("duration_hours") or 0.0)
        except Exception:
            pass
    return float(total)
