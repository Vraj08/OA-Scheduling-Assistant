"""Supabase integration.

We use Supabase (Postgres) as the backend for *operational* data to reduce
Google Sheets API load under concurrent usage:
  - approvals (pending actions + history)
  - audit log
  - locks

The schedule itself can remain in Google Sheets.

Secrets expected:
  SUPABASE_URL
  SUPABASE_KEY
"""

from __future__ import annotations

import random
import time
from typing import Any, Callable, TypeVar

import streamlit as st

T = TypeVar("T")


def supabase_enabled() -> bool:
    return bool(st.secrets.get("SUPABASE_URL") and st.secrets.get("SUPABASE_KEY"))


@st.cache_resource(show_spinner=False)
def get_supabase():
    """Return a cached Supabase client."""
    from supabase import create_client

    url = st.secrets.get("SUPABASE_URL")
    key = st.secrets.get("SUPABASE_KEY")
    if not (url and key):
        raise RuntimeError("Supabase secrets missing: SUPABASE_URL / SUPABASE_KEY")
    return create_client(url, key)


def with_retry(fn: Callable[..., T], *args: Any, retries: int = 5, base: float = 0.35, **kwargs: Any) -> T:
    """Retry wrapper (network hiccups / transient 429/5xx)."""
    last: Exception | None = None
    for i in range(max(1, retries)):
        try:
            return fn(*args, **kwargs)
        except Exception as e:  # pragma: no cover
            last = e
            # exponential backoff with jitter
            time.sleep(base * (2**i) + random.random() * 0.15)
    assert last is not None
    raise last
