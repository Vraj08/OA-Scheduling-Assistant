from datetime import datetime, timezone
from gspread import WorksheetNotFound
import gspread
from gspread.exceptions import APIError
import time as _pytime
import streamlit as st
from ..config import LOCKS_SHEET
from .quotas import _safe_batch_get
from ..integrations.supabase_io import get_supabase, supabase_enabled, with_retry


def _use_db() -> bool:
    # Allow forcing legacy Sheets locks.
    if str(st.secrets.get("USE_SHEETS_LOCKS", "")).strip().lower() in {"1", "true", "yes"}:
        return False
    return supabase_enabled()

def _retry_429(fn, *args, retries: int = 5, backoff: float = 0.8, **kwargs):
    for i in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            s = str(e).lower()
            if "429" in s or "quota exceeded" in s:
                _pytime.sleep(backoff * (2 ** i))
                continue
            raise
    return fn(*args, **kwargs)

def get_or_create_locks_sheet(ss) -> "gspread.Worksheet":
    # In Supabase mode we don't use a Sheets worksheet for locks.
    if _use_db():
        return None  # type: ignore[return-value]
    try:
        return _retry_429(ss.worksheet, LOCKS_SHEET)
    except WorksheetNotFound:
        pass
    try:
        ws = _retry_429(ss.add_worksheet, title=LOCKS_SHEET, rows=2000, cols=6)
    except APIError as e:
        if "already exists" in str(e).lower():
            ws = _retry_429(ss.worksheet, LOCKS_SHEET)
        else:
            raise
    try:
        block = _safe_batch_get(ws, ["A1:F1"])[0]
        if not (block and block[0] and any(c.strip() for c in block[0])):
            _retry_429(ws.update, range_name="A1:F1",
                       values=[["Key","Actor","ISOTime","Status","Row","Notes"]])
    except Exception:
        pass
    return ws

def lock_key(ws_title: str, day: str, start_str: str, end_str: str) -> str:
    return f"{ws_title}|{day.lower()}|{start_str}-{end_str}"

def acquire_fcfs_lock(locks_ws, key: str, actor: str, ttl_sec: int = 90) -> tuple[bool, int]:
    """First-come-first-served lock with minimal API calls.

    The previous implementation appended a row and then called `get_all_values()`,
    which becomes very slow as the locks sheet grows.

    Fast path:
      1) append_row (often returns an API response containing updatedRange)
      2) read only a small trailing window of rows for the same key
      3) mark your own row as won/lost

    Fallback:
      If we cannot determine the appended row index, we fall back to the
      slower full scan.
    """

    # ─────────────── Supabase mode (preferred) ───────────────
    if _use_db() or locks_ws is None:
        sb = get_supabase()
        now = datetime.now(timezone.utc)
        exp = now.timestamp() + ttl_sec

        # Prefer an atomic RPC if you create it (recommended).
        # SQL (run once in Supabase):
        #   create table if not exists public.locks (
        #     key text primary key,
        #     owner text not null,
        #     expires_at timestamptz not null
        #   );
        #   create or replace function public.acquire_lock(p_key text, p_owner text, p_ttl_sec int)
        #   returns boolean language plpgsql as $$
        #   declare now_ts timestamptz := now();
        #   begin
        #     insert into public.locks(key, owner, expires_at)
        #     values (p_key, p_owner, now_ts + make_interval(secs => p_ttl_sec))
        #     on conflict (key) do update
        #       set owner = excluded.owner,
        #           expires_at = excluded.expires_at
        #     where public.locks.expires_at < now_ts;
        #     return (select owner = p_owner from public.locks where key = p_key);
        #   end $$;

        try:
            resp = with_retry(lambda: sb.rpc("acquire_lock", {"p_key": key, "p_owner": actor, "p_ttl_sec": int(ttl_sec)}).execute())
            ok = bool(getattr(resp, "data", None))
            return ok, 1
        except Exception:
            # Fallback (best-effort): non-atomic upsert + verify owner
            pass

        now_iso = now.isoformat()
        exp_iso = datetime.fromtimestamp(exp, tz=timezone.utc).isoformat()
        # Read current lock
        current = with_retry(lambda: sb.table("locks").select("owner,expires_at").eq("key", key).maybe_single().execute())
        row = getattr(current, "data", None)
        can_take = False
        if not row:
            can_take = True
        else:
            try:
                # expires_at might come back as string
                ea = row.get("expires_at")
                ea_dt = datetime.fromisoformat(ea.replace("Z", "+00:00")) if isinstance(ea, str) else ea
                can_take = (ea_dt.timestamp() < now.timestamp())
            except Exception:
                # If parsing fails, treat as expired.
                can_take = True

        if can_take:
            with_retry(lambda: sb.table("locks").upsert({"key": key, "owner": actor, "expires_at": exp_iso}).execute())

        # Verify ownership
        after = with_retry(lambda: sb.table("locks").select("owner").eq("key", key).maybe_single().execute())
        owner = (getattr(after, "data", None) or {}).get("owner")
        return (owner == actor), 1

    now = datetime.now(timezone.utc).isoformat()
    resp = _retry_429(locks_ws.append_row, [key, actor, now, "pending", "", ""], value_input_option="RAW")

    appended_row: int | None = None
    try:
        # gspread sometimes returns the raw Sheets API response from append.
        # We try to parse the appended row from updates.updatedRange like:
        #   "Locks!A123:F123"
        updated = None
        if isinstance(resp, dict):
            updated = (resp.get("updates") or {}).get("updatedRange") or (resp.get("updates") or {}).get("updated_range")
        if updated and isinstance(updated, str):
            import re
            m = re.search(r"!A(\d+):", updated)
            if m:
                appended_row = int(m.group(1))
    except Exception:
        appended_row = None

    cutoff = datetime.now(timezone.utc).timestamp() - ttl_sec

    # --- Fast scan: only last N rows ---
    try:
        if appended_row and appended_row >= 2:
            # Look back a bit for competing claims of the same key.
            lookback = 200
            start = max(2, appended_row - lookback)
            block = _safe_batch_get(locks_ws, [f"A{start}:C{appended_row}"])[0]
            rows = block or []

            claims = []
            for i, r in enumerate(rows):
                idx = start + i
                k = r[0] if len(r) > 0 else ""
                a = r[1] if len(r) > 1 else ""
                t = r[2] if len(r) > 2 else ""
                try:
                    ts = datetime.fromisoformat(t).timestamp()
                except Exception:
                    continue
                if k == key and ts >= cutoff:
                    claims.append((idx, a, ts))

            if claims:
                claims.sort(key=lambda x: x[2])
                winner_row = claims[0][0]
                is_winner = (claims[0][1] == actor)
                try:
                    _retry_429(locks_ws.update, range_name=f"D{appended_row}", values=[["won" if is_winner else "lost"]])
                except Exception:
                    pass
                return is_winner, winner_row
    except Exception:
        # fall through to slow path
        pass

    # --- Slow fallback (old behavior) ---
    vals = _retry_429(locks_ws.get_all_values)
    rows = vals[1:] if len(vals) > 1 else []
    claims = []
    for idx, r in enumerate(rows, start=2):
        k = r[0] if len(r) > 0 else ""
        a = r[1] if len(r) > 1 else ""
        t = r[2] if len(r) > 2 else ""
        try:
            ts = datetime.fromisoformat(t).timestamp()
        except Exception:
            continue
        if k == key and ts >= cutoff:
            claims.append((idx, a, ts))
    if not claims:
        return False, -1
    claims.sort(key=lambda x: x[2])
    winner_row = claims[0][0]
    is_winner = (claims[0][1] == actor)
    try:
        my_row = max(i for (i, a, _) in claims if a == actor)
        _retry_429(locks_ws.update, range_name=f"D{my_row}", values=[["won" if is_winner else "lost"]])
    except Exception:
        pass
    return is_winner, winner_row
