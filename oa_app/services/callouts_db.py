"""Callouts DB helpers (Supabase).

Backend-only table:
  public.callouts

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


def supabase_callouts_enabled() -> bool:
    return supabase_enabled()


def upsert_callout(payload: dict) -> dict:
    """Upsert a callout row keyed by approval_id.

    Returns inserted/updated row dict (best-effort).
    """
    if not supabase_callouts_enabled():
        return {}
    sb = get_supabase()
    resp = with_retry(lambda: sb.table("callouts").upsert(payload, on_conflict="approval_id").execute())
    data = getattr(resp, "data", None) or []
    return data[0] if data else {}


def sum_callout_hours_for_week(*, caller_name: str, week_start: date, week_end: date) -> float:
    """Sum duration_hours for a caller within [week_start, week_end] (inclusive)."""
    if not supabase_callouts_enabled():
        return 0.0
    sb = get_supabase()
    # Use canonical display name match, but tolerate minor spacing/case by normalizing
    # on the Python side (we store canon roster names in DB).
    caller = (caller_name or "").strip()
    resp = with_retry(
        lambda: sb.table("callouts")
        .select("duration_hours,caller_name")
        .gte("event_date", str(week_start))
        .lte("event_date", str(week_end))
        .execute()
    )
    rows: list[dict[str, Any]] = getattr(resp, "data", None) or []
    target_k = name_key(caller)
    total = 0.0
    for r in rows:
        if name_key(str(r.get("caller_name", ""))) != target_k:
            continue
        try:
            total += float(r.get("duration_hours") or 0.0)
        except Exception:
            pass
    return float(total)
