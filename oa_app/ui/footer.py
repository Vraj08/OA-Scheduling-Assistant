"""Global footer.

Requirement: show the same copyright line on every screen/page in the UI.

We implement a fixed, aesthetic footer that stays visible at the bottom of the
viewport (and adds padding so content isn't hidden behind it).
"""

from __future__ import annotations

import streamlit as st


# Keep the year fixed per user request.
COPYRIGHT_TEXT = "© 2026 Vraj Patel. All rights reserved."


def render_global_footer() -> None:
    """Render a fixed footer at the bottom of the page."""
    st.markdown(
        """
        <style>
          /* Make sure main content doesn't get covered by the fixed footer */
          [data-testid="stAppViewContainer"] .main .block-container {
            padding-bottom: 4.5rem;
          }

          .vp-global-footer {
            position: fixed;
            left: 0;
            right: 0;
            bottom: 0;
            width: 100%;
            z-index: 999999;
            pointer-events: none;
            display: flex;
            justify-content: center;
          }

          .vp-global-footer .vp-global-footer__inner {
            margin: 0.75rem 1rem;
            padding: 0.45rem 0.9rem;
            border-radius: 999px;
            font-size: 0.80rem;
            line-height: 1.1rem;
            letter-spacing: 0.2px;
            border: 1px solid var(--oa-footer-border, rgba(49, 51, 63, 0.12));
            background: var(--oa-footer-bg, rgba(255, 255, 255, 0.70));
            color: var(--oa-footer-ink, rgba(49, 51, 63, 0.72));
            backdrop-filter: blur(10px);
            -webkit-backdrop-filter: blur(10px);
            box-shadow: var(--oa-shadow-soft, 0 6px 22px rgba(0, 0, 0, 0.06));
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
        <div class="vp-global-footer">
          <div class="vp-global-footer__inner">{COPYRIGHT_TEXT}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
