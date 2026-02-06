"""Global UI theme (subtle + clean + colorful).

The UI must stay consistent across reruns (e.g., switching tabs), so we inject
CSS on *every* run rather than only once per session.

This file is intentionally visual-only: no functional behavior changes.
"""

from __future__ import annotations

import streamlit as st


_THEME_CSS = r"""
<style>
  :root {
    /* Core palette (subtle) */
    --oa-accent: #4F46E5;      /* indigo */
    --oa-accent2: #14B8A6;     /* teal */
    --oa-ink: rgba(15, 23, 42, 0.92);
    --oa-ink-muted: rgba(15, 23, 42, 0.62);

    /* Surfaces */
    --oa-surface: rgba(238, 242, 255, 0.72);
    --oa-surface-strong: rgba(238, 242, 255, 0.92);
    --oa-border: rgba(15, 23, 42, 0.08);

    /* Shadows / radii (clean; not overly rounded) */
    --oa-radius: 14px;
    --oa-radius-card: 12px;
    --oa-radius-control: 10px;
    --oa-shadow: 0 18px 46px rgba(2, 6, 23, 0.10);
    --oa-shadow-soft: 0 10px 26px rgba(2, 6, 23, 0.07);

    /* Status (soft, matches pastel background) */
    --oa-danger: #E35151;
    --oa-danger-bg: rgba(227, 81, 81, 0.10);
  }

  /* ------------------------- App background -------------------------- */
  [data-testid="stAppViewContainer"] {
    background:
      radial-gradient(900px 500px at 12% 8%, rgba(79, 70, 229, 0.12), rgba(0,0,0,0)),
      radial-gradient(900px 520px at 88% 12%, rgba(20, 184, 166, 0.10), rgba(0,0,0,0)),
      linear-gradient(180deg, #F6F7FB 0%, #EEF2FF 60%, #F8FAFC 100%);
  }

  html, body, .stApp {
    background:
      radial-gradient(900px 500px at 12% 8%, rgba(79, 70, 229, 0.12), rgba(0,0,0,0)),
      radial-gradient(900px 520px at 88% 12%, rgba(20, 184, 166, 0.10), rgba(0,0,0,0)),
      linear-gradient(180deg, #F6F7FB 0%, #EEF2FF 60%, #F8FAFC 100%);
  }


  /* Let the background show through */
  [data-testid="stHeader"] { background: transparent; }

  section[data-testid="stMain"] { background: transparent; }
  [data-testid="stMain"] { background: transparent; }
  [data-testid="stSidebar"] { background: transparent; }

  /* Thin accent bar at very top */
  [data-testid="stDecoration"] {
    background-image: linear-gradient(90deg, var(--oa-accent), var(--oa-accent2));
    opacity: 0.95;
  }

  /* ------------------------- Main content shell ---------------------- */
  [data-testid="stAppViewContainer"] .main .block-container,
  [data-testid="stMainBlockContainer"],
  section[data-testid="stMain"] .block-container {
    margin-top: 0.85rem;
    background: linear-gradient(180deg, rgba(238,242,255,0.70), rgba(224,231,255,0.54));
    border: 1px solid var(--oa-border);
    border-radius: var(--oa-radius);
    box-shadow: var(--oa-shadow);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    /* Leave space for the fixed footer */
    padding: 2.0rem 2.0rem 5.6rem 2.0rem;
  }

  /* Headings */
  h1, h2, h3, h4 {
    color: var(--oa-ink) !important;
    letter-spacing: -0.2px;
  }
  h1 { font-weight: 800 !important; }

  /* Captions / muted text */
  .stCaption, small, [data-testid="stCaptionContainer"] {
    color: var(--oa-ink-muted) !important;
  }

  /* ------------------------- Sidebar -------------------------------- */
  [data-testid="stSidebar"] > div:first-child {
    background: linear-gradient(180deg, rgba(224,231,255,0.94), rgba(238,242,255,0.86));
    border-right: 1px solid var(--oa-border);
  }
  [data-testid="stSidebar"] .stMarkdown, [data-testid="stSidebar"] label, [data-testid="stSidebar"] p {
    color: var(--oa-ink) !important;
  }

  /* ------------------------- Inputs / selects ------------------------ */
  .stTextInput input,
  .stTextArea textarea,
  .stNumberInput input,
  .stDateInput input,
  .stTimeInput input {
    background: var(--oa-surface-strong) !important;
    border-radius: var(--oa-radius-control) !important;
    border: 1px solid rgba(15, 23, 42, 0.10) !important;
    box-shadow: 0 10px 22px rgba(2, 6, 23, 0.05);
  }

  /* Baseweb selects (target ONLY the control, not the label/help text) */
  [data-testid="stSelectbox"] div[data-baseweb="select"] > div,
  [data-testid="stMultiSelect"] div[data-baseweb="select"] > div {
    background: var(--oa-surface-strong) !important;
    border-radius: var(--oa-radius-control) !important;
    border: 1px solid rgba(15, 23, 42, 0.10) !important;
    box-shadow: 0 10px 22px rgba(2, 6, 23, 0.05);
  }

  /* Prevent “pill bubbles” behind labels like “Select a tab” */
  [data-testid="stSelectbox"] label,
  [data-testid="stMultiSelect"] label,
  [data-testid="stSelectbox"] [data-testid="stMarkdownContainer"],
  [data-testid="stMultiSelect"] [data-testid="stMarkdownContainer"] {
    background: transparent !important;
    border: 0 !important;
    box-shadow: none !important;
    border-radius: 0 !important;
    padding: 0 !important;
  }
  .stTextInput input:focus,
  .stTextArea textarea:focus,
  .stNumberInput input:focus,
  .stDateInput input:focus,
  .stTimeInput input:focus {
    border-color: rgba(79, 70, 229, 0.38) !important;
    box-shadow: 0 0 0 3px rgba(79, 70, 229, 0.12) !important;
  }

  /* ------------------------- Buttons -------------------------------- */
  .stButton > button {
    border: 0 !important;
    border-radius: 12px !important;
    padding: 0.46rem 0.82rem !important;
    font-weight: 700 !important;
    font-size: 0.92rem !important;
    color: white !important;
    background: linear-gradient(135deg, rgba(79,70,229,0.95), rgba(20,184,166,0.90)) !important;
    box-shadow: 0 12px 28px rgba(2, 6, 23, 0.14);
    transition: transform 120ms ease, filter 120ms ease, box-shadow 120ms ease;
    white-space: nowrap !important;  /* keeps “Clear caches” on one line */
    line-height: 1.05 !important;
  }
  .stButton > button:hover {
    filter: brightness(1.03);
    transform: translateY(-1px);
    box-shadow: 0 16px 34px rgba(2, 6, 23, 0.16);
  }
  .stButton > button:active { transform: translateY(0px) scale(0.99); }

  /* Secondary buttons */
  .stButton > button[kind="secondary"] {
    background: linear-gradient(180deg, rgba(238,242,255,0.88), rgba(224,231,255,0.78)) !important;
    color: var(--oa-ink) !important;
    border: 1px solid rgba(15, 23, 42, 0.12) !important;
    box-shadow: 0 10px 22px rgba(2, 6, 23, 0.06);
  }

  /* ------------------------- Tabs ----------------------------------- */
  [data-testid="stTabs"] [role="tablist"] { gap: 0.45rem; }
  [data-testid="stTabs"] [role="tab"] {
    border-radius: 12px !important;
    padding: 0.40rem 0.85rem !important;
    background: rgba(238,242,255,0.68) !important;
    border: 1px solid rgba(15,23,42,0.10) !important;
    box-shadow: 0 10px 24px rgba(2,6,23,0.05);
  }
  [data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    background: linear-gradient(135deg, rgba(79,70,229,0.14), rgba(20,184,166,0.12)) !important;
    border-color: rgba(79,70,229,0.20) !important;
  }

  /* Tabs panel surface (avoid stark white areas inside tabs) */
  [data-testid="stTabs"] div[role="tabpanel"] {
    background: linear-gradient(180deg, rgba(238,242,255,0.58), rgba(224,231,255,0.44)) !important;
    border: 1px solid rgba(15,23,42,0.06) !important;
    border-radius: 14px !important;
    padding: 0.85rem 0.85rem 0.55rem 0.85rem !important;
    margin-top: 0.55rem;
  }

  /* ------------------------- Expanders / Metrics / Alerts ----------- */
  [data-testid="stExpander"] details,
  [data-testid="stMetric"],
  [data-testid="stAlert"],
  [data-testid="stDataFrame"],
  [data-testid="stPlotlyChart"],
  [data-testid="stTable"] {
    background: linear-gradient(180deg, rgba(238,242,255,0.70), rgba(224,231,255,0.55)) !important;
    border: 1px solid rgba(15,23,42,0.08) !important;
    border-radius: var(--oa-radius-card) !important;
    box-shadow: var(--oa-shadow-soft) !important;
  }

  /* Subtle color on expander headers (e.g., “Your Schedule (This Week)”) */
  [data-testid="stExpander"] details > summary {
    border-radius: 12px !important;
    padding: 0.50rem 0.65rem !important;
    background: linear-gradient(90deg, rgba(79,70,229,0.10), rgba(20,184,166,0.08)) !important;
    border: 1px solid rgba(15,23,42,0.08) !important;
  }

  /* Code blocks */
  pre, code { border-radius: 10px !important; }

  /* Footer polish (works with oa_app/ui/footer.py) */
  .vp-global-footer .vp-global-footer__inner {
    background: rgba(238,242,255,0.74) !important;
    border: 1px solid rgba(15,23,42,0.10) !important;
    color: rgba(15,23,42,0.70) !important;
    box-shadow: 0 16px 38px rgba(2, 6, 23, 0.12) !important;
  }




  /* Sidebar hours card (compact + aesthetic) */
  .oa-hours-card {
    background: linear-gradient(135deg, rgba(79,70,229,0.10), rgba(20,184,166,0.08));
    border: 1px solid rgba(15,23,42,0.10);
    border-radius: 14px;
    padding: 0.70rem 0.75rem 0.75rem 0.75rem;
    box-shadow: 0 14px 34px rgba(2,6,23,0.08);
    margin: 0.25rem 0 0.55rem 0;
  }

  /* Classic layout: label → big value → bar (matches older layout, modern colors) */
  .oa-hours-card--classic .oa-hours-card__label {
    /* Match the older (clean) look: regular weight label, subtle ink */
    font-weight: 520;
    font-size: 0.95rem;
    color: rgba(15,23,42,0.80);
    line-height: 1.15;
    margin-bottom: 0.15rem;
  }
  .oa-hours-card__scope {
    font-weight: 520;
    color: rgba(15,23,42,0.58);
  }
  .oa-hours-card__valueBig {
    /* Bigger like the older UI, but with subtle modern color */
    font-weight: 860;
    font-size: 2.55rem;
    letter-spacing: -0.6px;
    color: var(--oa-ink);
    line-height: 1.05;
    margin-top: 0.05rem;
  }
  .oa-hours-card__valueBig .oa-hours-card__cap {
    font-weight: 740;
    font-size: 1.20rem;
    color: rgba(15,23,42,0.60);
    margin-left: 0.15rem;
    vertical-align: baseline;
  }
  .oa-hours-card--classic .oa-hours-bar {
    margin-top: 0.70rem;
  }
  .oa-hours-card__top {
    display:flex;
    align-items:baseline;
    justify-content:space-between;
    gap: 0.6rem;
  }
  .oa-hours-card__label {
    font-weight: 750;
    font-size: 0.90rem;
    color: var(--oa-ink);
  }
  .oa-hours-card__sub {
    margin-top: 0.05rem;
    font-size: 0.78rem;
    color: var(--oa-ink-muted);
  }
  .oa-hours-card__value {
    font-weight: 850;
    font-size: 1.05rem;
    color: var(--oa-ink);
    white-space: nowrap;
  }
  .oa-hours-card__cap {
    font-weight: 750;
    color: var(--oa-ink-muted);
  }
  .oa-hours-bar {
    height: 10px;
    border-radius: 999px;
    margin-top: 0.60rem;
    background: rgba(15,23,42,0.08);
    overflow: hidden;
  }
  .oa-hours-bar__fill {
    height: 100%;
    border-radius: 999px;
    background: linear-gradient(90deg, rgba(79,70,229,0.95), rgba(20,184,166,0.90));
    box-shadow: 0 6px 18px rgba(2,6,23,0.14);
  }

  /* Inline metric (no big card) */
  .oa-inline-metric {
    display:flex;
    align-items:baseline;
    justify-content:space-between;
    gap: 0.75rem;
    padding: 0.15rem 0.10rem;
    margin: 0.15rem 0 0.35rem 0;
  }
  .oa-inline-metric__label {
    font-weight: 650;
    font-size: 0.86rem;
    color: var(--oa-ink-muted);
  }
  .oa-inline-metric__value {
    font-weight: 800;
    font-size: 1.05rem;
    color: var(--oa-ink);
  }

  /* Expander body: keep tinted surface, avoid stark white inside */
  [data-testid="stExpander"] details > div {
    background: transparent !important;
    padding-top: 0.25rem;
  }

  /* Dataframe inner grid: let wrapper tint show through */
  [data-testid="stDataFrame"] [role="grid"],
  [data-testid="stDataFrame"] [role="row"],
  [data-testid="stDataFrame"] [role="gridcell"] {
    background: transparent !important;
  }

  /* Make the overall page feel consistent even when widgets render their own wrappers */
  div[data-testid="stVerticalBlock"],
  div[data-testid="stHorizontalBlock"],
  div[data-testid="stContainer"],
  div[data-testid="stBlock"] {
    background: transparent !important;
  }


  /* ------------------------- Dark mode tweaks ----------------------- */
  @media (prefers-color-scheme: dark) {
    :root {
      --oa-ink: rgba(255,255,255,0.92);
      --oa-ink-muted: rgba(255,255,255,0.70);
      --oa-surface: rgba(17, 24, 39, 0.62);
      --oa-surface-strong: rgba(17, 24, 39, 0.78);
      --oa-border: rgba(255,255,255,0.14);
      --oa-shadow: 0 24px 70px rgba(0,0,0,0.55);
      --oa-shadow-soft: 0 14px 44px rgba(0,0,0,0.45);
      --oa-danger-bg: rgba(220, 38, 38, 0.10);
    }
    [data-testid="stAppViewContainer"] {
      background:
        radial-gradient(900px 500px at 12% 8%, rgba(79, 70, 229, 0.22), rgba(0,0,0,0)),
        radial-gradient(900px 520px at 88% 12%, rgba(20, 184, 166, 0.18), rgba(0,0,0,0)),
        linear-gradient(180deg, #0B1220 0%, #0B1220 100%);
    }
    [data-testid="stSidebar"] > div:first-child {
      background: rgba(15, 23, 42, 0.70);
      border-right: 1px solid rgba(255,255,255,0.12);
    }
    .vp-global-footer .vp-global-footer__inner {
      background: rgba(15, 23, 42, 0.62) !important;
      border: 1px solid rgba(255,255,255,0.14) !important;
      color: rgba(255,255,255,0.72) !important;
      box-shadow: 0 16px 38px rgba(0, 0, 0, 0.30) !important;
    }
  }
</style>
"""


def apply_vibrant_theme() -> None:
    """Apply the global theme.

    Important: inject CSS on every rerun so theme never "drops" when users
    click tabs or interact with widgets.
    """
    st.markdown(_THEME_CSS, unsafe_allow_html=True)
