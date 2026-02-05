"""Recovery UI for transient connectivity/quota errors.

This app is used by non-technical users. When Google auth / Google Sheets
calls fail due to internet issues or API rate limits, we should *not* crash
with a long stacktrace. Instead, show a clear "what happened" + "what to do"
panel and provide one-click recovery actions.

Design goals:
  - Never leak secrets.
  - Prefer actionable, plain-English instructions.
  - Offer retry + hard refresh (clear caches) without losing widget state.
  - For rate-limit/quota errors: suggest waiting 30s and auto-refresh.
"""

from __future__ import annotations

import re
import time
import traceback
from dataclasses import dataclass
from typing import Iterable, List, Optional

import streamlit as st


@dataclass
class RecoveryAdvice:
    kind: str  # network | quota | auth | config | unknown
    title: str
    summary: str
    steps: List[str]
    retry_after_sec: Optional[int] = None
    details: Optional[str] = None


def _exc_name(e: BaseException) -> str:
    return type(e).__name__


def _s(e: BaseException) -> str:
    try:
        return str(e) or _exc_name(e)
    except Exception:
        return _exc_name(e)

def _iter_exc_chain(e: BaseException):
    """Yield exception and its causes/contexts (best-effort)."""
    seen: set[int] = set()
    cur: BaseException | None = e
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        yield cur
        cur = getattr(cur, '__cause__', None) or getattr(cur, '__context__', None)



def _looks_like_quota(msg: str) -> bool:
    m = msg.lower()
    return (
        "quota" in m
        or "rate limit" in m
        or "too many requests" in m
        or re.search(r"\b429\b", m) is not None
        or "read requests" in m
        or "write requests" in m
    )


def _get_http_status(e: BaseException) -> int | None:
    """Best-effort extract of an HTTP status code from common exception shapes."""
    # requests exceptions often carry .response.status_code
    try:
        resp = getattr(e, "response", None)
        if resp is not None:
            code = getattr(resp, "status_code", None)
            if isinstance(code, int):
                return code
    except Exception:
        pass

    # gspread.exceptions.APIError carries .response (a requests Response)
    try:
        resp = getattr(e, "response", None)
        code = getattr(resp, "status_code", None)
        if isinstance(code, int):
            return code
    except Exception:
        pass

    # googleapiclient errors may carry .status_code or .resp.status
    try:
        code = getattr(e, "status_code", None)
        if isinstance(code, int):
            return code
    except Exception:
        pass
    try:
        resp = getattr(e, "resp", None)
        code = getattr(resp, "status", None)
        if isinstance(code, int):
            return code
    except Exception:
        pass

    return None


def _looks_like_offline(msg: str) -> bool:
    m = msg.lower()
    return any(
        tok in m
        for tok in [
            "failed to resolve",
            "name resolution",
            "getaddrinfo failed",
            "nodename nor servname",
            "temporary failure in name resolution",
            "network is unreachable",
            "no route to host",
            "connection aborted",
            "connection reset",
            "remote end closed connection",
            "remotedisconnected",
            "timed out",
            "timeout",
            "transporterror",
        ]
    )


def _looks_like_auth(msg: str) -> bool:
    m = msg.lower()
    return any(
        tok in m
        for tok in [
            "invalid_grant",
            "invalid jwt",
            "unauthorized",
            "permission denied",
            "forbidden",
            "insufficient permissions",
            "not authorized",
            "service account",
            "invalid scope",
        ]
    )


def _looks_like_config(msg: str) -> bool:
    m = msg.lower()
    return any(
        tok in m
        for tok in [
            "missing sheet_url",
            "missing service account",
            "supabase secrets missing",
            "secrets",
            "keyerror",
        ]
    )


def classify_exception(e: BaseException, *, where: str = "") -> RecoveryAdvice:
    """Classify an exception into a recovery category + user actions."""
    chain = list(_iter_exc_chain(e))
    msg = ' | '.join(_s(x) for x in chain)
    names = {type(x).__name__ for x in chain}
    ctx = f" while {where}" if where else ""

    http_status: int | None = None
    for x in chain:
        code = _get_http_status(x)
        if isinstance(code, int):
            http_status = code
            break

    # Quota / rate limiting
    if _looks_like_quota(msg) or http_status == 429:
        return RecoveryAdvice(
            kind="quota",
            title="Google API rate limit hit",
            summary=(
                "Too many requests were sent to Google Sheets in a short time. "
                "This is common when multiple people use the app at once."
            ),
            steps=[
                "Wait 30 seconds (Google usually resets the limit quickly).",
                "The app will auto-refresh, or you can click **Retry now**.",
                "If it keeps happening: use **Hard refresh** once, then try again.",
            ],
            retry_after_sec=30,
            details=f"{ctx}: {msg}",
        )
    # Offline / network
    if _looks_like_offline(msg) or (names & {
        "TransportError",
        "RemoteDisconnected",
        "NameResolutionError",
        "ConnectionError",
        "ConnectionResetError",
        "ReadTimeout",
        "Timeout",
    }):
        return RecoveryAdvice(
            kind="network",
            title="Connection problem",
            summary=(
                "The app couldn't reach Google (internet/DNS/VPN issue or a temporary drop)."
            ),
            steps=[
                "Check you have internet (try opening any website in a new tab).",
                "If you're on VPN, try turning it off (or switching networks / hotspot).",
                "Then click **Retry now**.",
            ],
            retry_after_sec=None,
            details=f"{ctx}: {msg}",
        )

    # Auth / permissions
    if _looks_like_auth(msg):
        return RecoveryAdvice(
            kind="auth",
            title="Google authentication / permissions issue",
            summary=(
                "Google rejected the request. This usually means the service account "
                "is missing access to the Sheet, or secrets are wrong/expired."
            ),
            steps=[
                "Confirm the Google Sheet is shared with the service account email.",
                "Confirm `.streamlit/secrets.toml` has a valid `gcp_service_account` JSON.",
                "After fixing, click **Hard refresh** then **Retry now**.",
            ],
            retry_after_sec=None,
            details=f"{ctx}: {msg}",
        )

    # Config
    if _looks_like_config(msg):
        return RecoveryAdvice(
            kind="config",
            title="Configuration issue",
            summary="The app is missing required configuration (secrets / URLs).",
            steps=[
                "Open `.streamlit/secrets.toml` and ensure SHEET_URL and gcp_service_account are set.",
                "Restart the Streamlit app after updating secrets.",
            ],
            retry_after_sec=None,
            details=f"{ctx}: {msg}",
        )

    # Unknown
    return RecoveryAdvice(
        kind="unknown",
        title="Unexpected error",
        summary="Something unexpected happened.",
        steps=[
            "Click **Retry now**.",
            "If it keeps happening: click **Hard refresh**.",
            "If it still fails, share the error details with the GOAs.",
        ],
        retry_after_sec=None,
        details=f"{ctx}: {msg}",
    )


def clear_hard_caches() -> None:
    """Clear Streamlit caches + in-session read caches.

    This does NOT wipe widget/session state.
    """
    try:
        st.cache_data.clear()
    except Exception:
        pass
    try:
        st.cache_resource.clear()
    except Exception:
        pass

    # App-specific caches (safe to drop)
    for k in ["WS_RANGE_CACHE", "DAY_CACHE", "WS_VER", "_SCHEDULE_BY_ID", "_SS_HANDLE_BY_ID"]:
        try:
            st.session_state.pop(k, None)
        except Exception:
            pass


def _auto_reload_js(ms: int) -> None:
    """Trigger a browser refresh after `ms` milliseconds."""
    try:
        import streamlit.components.v1 as components

        components.html(
            f"""<script>
            setTimeout(function(){{
              try {{ window.location.reload(); }} catch(e) {{}}
            }}, {int(ms)});
            </script>""",
            height=0,
        )
    except Exception:
        # If components isn't available, we simply don't auto-refresh.
        pass


def show_recovery_popup(
    e: BaseException,
    *,
    where: str = "",
    show_trace: bool = False,
) -> None:
    """Render a top-of-page recovery panel and stop execution."""
    advice = classify_exception(e, where=where)

    # Visual "popup" box
    st.markdown(
        """
        <style>
        .oa-recover {
          border: 1px solid rgba(255, 86, 48, 0.40);
          border-radius: 14px;
          padding: 16px 16px 10px 16px;
          background: rgba(255, 86, 48, 0.06);
          margin: 8px 0 14px 0;
        }
        .oa-recover h3 { margin: 0 0 6px 0; }
        .oa-recover p { margin: 0 0 10px 0; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div class="oa-recover">
          <h3>⚠️ {advice.title}</h3>
          <p>{advice.summary}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    for i, step in enumerate(advice.steps, start=1):
        st.write(f"{i}. {step}")

    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        if st.button("Retry now", type="primary", key=f"retry_now_{time.time_ns()}"):
            st.rerun()
    with c2:
        if st.button("Hard refresh (clear caches)", key=f"hard_refresh_{time.time_ns()}"):
            clear_hard_caches()
            st.rerun()
    with c3:
        if st.button("Show technical details", key=f"show_details_{time.time_ns()}"):
            st.session_state["_SHOW_TECH_DETAILS"] = True

    if advice.retry_after_sec:
        # Keep the same Streamlit session so widget state persists.
        # Requirement: for quota errors we always auto-retry (no extra click needed).
        ph = st.empty()
        for remaining in range(int(advice.retry_after_sec), 0, -1):
            ph.info(f"Waiting… retrying in {remaining} seconds")
            time.sleep(1)
        ph.empty()
        st.rerun()

    if st.session_state.get("_SHOW_TECH_DETAILS") or show_trace:
        st.markdown("#### Details")
        st.code(advice.details or _s(e))
        st.markdown("#### Traceback")
        st.code(traceback.format_exc())

    st.stop()


def maybe_show_recovery_popup(e: BaseException, *, where: str = "") -> bool:
    """If exception is likely recoverable, show popup and stop.

    Returns True if handled (and execution is stopped).
    """
    advice = classify_exception(e, where=where)
    if advice.kind in {"network", "quota", "auth", "config"}:
        show_recovery_popup(e, where=where)
        return True
    return False
