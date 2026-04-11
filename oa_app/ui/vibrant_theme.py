"""Global UI theme (subtle + clean + colorful).

The UI must stay consistent across reruns (e.g., switching tabs), so we inject
CSS on *every* run rather than only once per session.

This file is intentionally visual-only: no functional behavior changes.
"""

from __future__ import annotations

from string import Template

import streamlit as st


def _theme_tokens(mode: str) -> dict[str, str]:
    if mode == "dark":
        return {
            "accent": "#7C8CFF",
            "accent2": "#2DD4BF",
            "ink": "rgba(244, 247, 255, 0.94)",
            "ink_muted": "rgba(216, 228, 255, 0.72)",
            "surface": "rgba(15, 23, 42, 0.68)",
            "surface_strong": "rgba(15, 23, 42, 0.84)",
            "border": "rgba(148, 163, 184, 0.22)",
            "page_glow_a": "rgba(124, 140, 255, 0.22)",
            "page_glow_b": "rgba(45, 212, 191, 0.18)",
            "page_bg": "linear-gradient(180deg, #08101d 0%, #0b1324 52%, #101a31 100%)",
            "shell_bg": "linear-gradient(180deg, rgba(11,19,36,0.84), rgba(15,23,42,0.74))",
            "sidebar_bg": "linear-gradient(180deg, rgba(9,16,30,0.96), rgba(15,23,42,0.88))",
            "input_border": "rgba(148, 163, 184, 0.24)",
            "input_shadow": "0 12px 30px rgba(0, 0, 0, 0.26)",
            "shadow": "0 22px 60px rgba(0, 0, 0, 0.34)",
            "shadow_soft": "0 12px 34px rgba(0, 0, 0, 0.24)",
            "danger": "#FF7B7B",
            "danger_bg": "rgba(255, 123, 123, 0.12)",
            "focus_border": "rgba(124, 140, 255, 0.62)",
            "focus_ring": "rgba(124, 140, 255, 0.18)",
            "button_bg": "linear-gradient(135deg, rgba(124,140,255,0.98), rgba(45,212,191,0.92))",
            "button_text": "#08101d",
            "button_shadow": "0 14px 30px rgba(0, 0, 0, 0.28)",
            "secondary_bg": "linear-gradient(180deg, rgba(15,23,42,0.92), rgba(30,41,59,0.88))",
            "secondary_text": "rgba(244, 247, 255, 0.92)",
            "secondary_border": "rgba(148, 163, 184, 0.28)",
            "tab_bg": "rgba(15, 23, 42, 0.78)",
            "tab_border": "rgba(148, 163, 184, 0.20)",
            "tab_active_bg": "linear-gradient(135deg, rgba(124,140,255,0.24), rgba(45,212,191,0.18))",
            "tab_active_border": "rgba(124, 140, 255, 0.34)",
            "panel_bg": "linear-gradient(180deg, rgba(9,16,30,0.82), rgba(15,23,42,0.70))",
            "card_bg": "linear-gradient(180deg, rgba(15,23,42,0.76), rgba(30,41,59,0.66))",
            "card_border": "rgba(148, 163, 184, 0.18)",
            "summary_bg": "linear-gradient(90deg, rgba(124,140,255,0.16), rgba(45,212,191,0.12))",
            "summary_border": "rgba(148, 163, 184, 0.18)",
            "footer_bg": "rgba(8, 16, 29, 0.80)",
            "footer_border": "rgba(148, 163, 184, 0.18)",
            "footer_ink": "rgba(226, 232, 240, 0.80)",
            "hours_bg": "linear-gradient(135deg, rgba(124,140,255,0.18), rgba(45,212,191,0.14))",
            "hours_track": "rgba(148, 163, 184, 0.18)",
            "hero_bg": "linear-gradient(135deg, rgba(124,140,255,0.22), rgba(45,212,191,0.16))",
            "hero_border": "rgba(148, 163, 184, 0.20)",
            "hero_shadow": "0 14px 34px rgba(0, 0, 0, 0.26)",
            "trade_empty_ink": "rgba(216, 228, 255, 0.76)",
            "trade_empty_bg": "linear-gradient(135deg, rgba(124,140,255,0.10), rgba(45,212,191,0.08))",
            "trade_empty_border": "rgba(148, 163, 184, 0.24)",
            "trade_card_bg": "linear-gradient(135deg, rgba(30,41,59,0.92), rgba(15,23,42,0.82))",
            "trade_card_border": "rgba(255, 123, 123, 0.26)",
            "trade_card_strip": "rgba(255, 123, 123, 0.82)",
            "trade_time_ink": "rgba(248, 250, 252, 0.94)",
            "trade_badge_bg": "linear-gradient(135deg, rgba(51,65,85,0.92), rgba(30,41,59,0.86))",
            "trade_badge_border": "rgba(148, 163, 184, 0.22)",
            "trade_badge_ink": "rgba(241, 245, 249, 0.92)",
            "trade_sub_ink": "rgba(191, 219, 254, 0.78)",
            "trade_name_bg": "linear-gradient(135deg, rgba(127,29,29,0.48), rgba(30,41,59,0.88))",
            "trade_name_border": "rgba(255, 123, 123, 0.30)",
            "trade_name_ink": "rgba(254, 226, 226, 0.94)",
            "trade_more_ink": "rgba(254, 202, 202, 0.88)",
            "campus_unh_bg": "rgba(8, 145, 178, 0.24)",
            "campus_unh_border": "rgba(103, 232, 249, 0.30)",
            "campus_unh_ink": "rgba(207, 250, 254, 0.94)",
            "campus_mc_bg": "rgba(22, 163, 74, 0.22)",
            "campus_mc_border": "rgba(134, 239, 172, 0.30)",
            "campus_mc_ink": "rgba(220, 252, 231, 0.94)",
            "campus_oncall_bg": "rgba(234, 88, 12, 0.24)",
            "campus_oncall_border": "rgba(253, 186, 116, 0.32)",
            "campus_oncall_ink": "rgba(255, 237, 213, 0.96)",
        }

    return {
        "accent": "#4F46E5",
        "accent2": "#14B8A6",
        "ink": "rgba(15, 23, 42, 0.92)",
        "ink_muted": "rgba(15, 23, 42, 0.62)",
        "surface": "rgba(238, 242, 255, 0.72)",
        "surface_strong": "rgba(238, 242, 255, 0.92)",
        "border": "rgba(15, 23, 42, 0.08)",
        "page_glow_a": "rgba(79, 70, 229, 0.12)",
        "page_glow_b": "rgba(20, 184, 166, 0.10)",
        "page_bg": "linear-gradient(180deg, #F6F7FB 0%, #EEF2FF 60%, #F8FAFC 100%)",
        "shell_bg": "linear-gradient(180deg, rgba(238,242,255,0.70), rgba(224,231,255,0.54))",
        "sidebar_bg": "linear-gradient(180deg, rgba(224,231,255,0.94), rgba(238,242,255,0.86))",
        "input_border": "rgba(15, 23, 42, 0.10)",
        "input_shadow": "0 10px 22px rgba(2, 6, 23, 0.05)",
        "shadow": "0 18px 46px rgba(2, 6, 23, 0.10)",
        "shadow_soft": "0 10px 26px rgba(2, 6, 23, 0.07)",
        "danger": "#E35151",
        "danger_bg": "rgba(227, 81, 81, 0.10)",
        "focus_border": "rgba(79, 70, 229, 0.38)",
        "focus_ring": "rgba(79, 70, 229, 0.12)",
        "button_bg": "linear-gradient(135deg, rgba(79,70,229,0.95), rgba(20,184,166,0.90))",
        "button_text": "#ffffff",
        "button_shadow": "0 12px 28px rgba(2, 6, 23, 0.14)",
        "secondary_bg": "linear-gradient(180deg, rgba(238,242,255,0.88), rgba(224,231,255,0.78))",
        "secondary_text": "rgba(15, 23, 42, 0.92)",
        "secondary_border": "rgba(15, 23, 42, 0.12)",
        "tab_bg": "rgba(238,242,255,0.68)",
        "tab_border": "rgba(15,23,42,0.10)",
        "tab_active_bg": "linear-gradient(135deg, rgba(79,70,229,0.14), rgba(20,184,166,0.12))",
        "tab_active_border": "rgba(79,70,229,0.20)",
        "panel_bg": "linear-gradient(180deg, rgba(238,242,255,0.58), rgba(224,231,255,0.44))",
        "card_bg": "linear-gradient(180deg, rgba(238,242,255,0.70), rgba(224,231,255,0.55))",
        "card_border": "rgba(15,23,42,0.08)",
        "summary_bg": "linear-gradient(90deg, rgba(79,70,229,0.10), rgba(20,184,166,0.08))",
        "summary_border": "rgba(15,23,42,0.08)",
        "footer_bg": "rgba(238,242,255,0.74)",
        "footer_border": "rgba(15,23,42,0.10)",
        "footer_ink": "rgba(15,23,42,0.70)",
        "hours_bg": "linear-gradient(135deg, rgba(79,70,229,0.10), rgba(20,184,166,0.08))",
        "hours_track": "rgba(15,23,42,0.08)",
        "hero_bg": "linear-gradient(135deg, rgba(79,70,229,0.10), rgba(20,184,166,0.08))",
        "hero_border": "rgba(15,23,42,0.10)",
        "hero_shadow": "0 12px 30px rgba(2,6,23,0.08)",
        "trade_empty_ink": "rgba(15,23,42,0.62)",
        "trade_empty_bg": "linear-gradient(135deg, rgba(79,70,229,0.06), rgba(20,184,166,0.05))",
        "trade_empty_border": "rgba(15,23,42,0.16)",
        "trade_card_bg": "linear-gradient(135deg, rgba(238,242,255,0.80), rgba(224,231,255,0.58))",
        "trade_card_border": "rgba(227,81,81,0.18)",
        "trade_card_strip": "rgba(227,81,81,0.72)",
        "trade_time_ink": "#111827",
        "trade_badge_bg": "linear-gradient(135deg, rgba(238,242,255,0.86), rgba(224,231,255,0.66))",
        "trade_badge_border": "rgba(15,23,42,0.12)",
        "trade_badge_ink": "#0f172a",
        "trade_sub_ink": "#6b7280",
        "trade_name_bg": "linear-gradient(135deg, rgba(227,81,81,0.10), rgba(238,242,255,0.72))",
        "trade_name_border": "rgba(227,81,81,0.18)",
        "trade_name_ink": "#7f1d1d",
        "trade_more_ink": "rgba(127,29,29,0.86)",
        "campus_unh_bg": "#e0f2fe",
        "campus_unh_border": "#bae6fd",
        "campus_unh_ink": "#0c4a6e",
        "campus_mc_bg": "#dcfce7",
        "campus_mc_border": "#bbf7d0",
        "campus_mc_ink": "#14532d",
        "campus_oncall_bg": "#ffedd5",
        "campus_oncall_border": "#fed7aa",
        "campus_oncall_ink": "#9a3412",
    }


_CSS_TEMPLATE = Template(
    r"""
<style>
  :root {
    --oa-accent: $accent;
    --oa-accent2: $accent2;
    --oa-ink: $ink;
    --oa-ink-muted: $ink_muted;
    --oa-surface: $surface;
    --oa-surface-strong: $surface_strong;
    --oa-border: $border;
    --oa-shadow: $shadow;
    --oa-shadow-soft: $shadow_soft;
    --oa-danger: $danger;
    --oa-danger-bg: $danger_bg;
    --oa-page-glow-a: $page_glow_a;
    --oa-page-glow-b: $page_glow_b;
    --oa-page-bg: $page_bg;
    --oa-shell-bg: $shell_bg;
    --oa-sidebar-bg: $sidebar_bg;
    --oa-input-border: $input_border;
    --oa-input-shadow: $input_shadow;
    --oa-focus-border: $focus_border;
    --oa-focus-ring: $focus_ring;
    --oa-button-bg: $button_bg;
    --oa-button-text: $button_text;
    --oa-button-shadow: $button_shadow;
    --oa-secondary-bg: $secondary_bg;
    --oa-secondary-text: $secondary_text;
    --oa-secondary-border: $secondary_border;
    --oa-tab-bg: $tab_bg;
    --oa-tab-border: $tab_border;
    --oa-tab-active-bg: $tab_active_bg;
    --oa-tab-active-border: $tab_active_border;
    --oa-panel-bg: $panel_bg;
    --oa-card-bg: $card_bg;
    --oa-card-border: $card_border;
    --oa-summary-bg: $summary_bg;
    --oa-summary-border: $summary_border;
    --oa-footer-bg: $footer_bg;
    --oa-footer-border: $footer_border;
    --oa-footer-ink: $footer_ink;
    --oa-hours-bg: $hours_bg;
    --oa-hours-track: $hours_track;
    --oa-hero-bg: $hero_bg;
    --oa-hero-border: $hero_border;
    --oa-hero-shadow: $hero_shadow;
    --oa-trade-empty-ink: $trade_empty_ink;
    --oa-trade-empty-bg: $trade_empty_bg;
    --oa-trade-empty-border: $trade_empty_border;
    --oa-trade-card-bg: $trade_card_bg;
    --oa-trade-card-border: $trade_card_border;
    --oa-trade-card-strip: $trade_card_strip;
    --oa-trade-time-ink: $trade_time_ink;
    --oa-trade-badge-bg: $trade_badge_bg;
    --oa-trade-badge-border: $trade_badge_border;
    --oa-trade-badge-ink: $trade_badge_ink;
    --oa-trade-sub-ink: $trade_sub_ink;
    --oa-trade-name-bg: $trade_name_bg;
    --oa-trade-name-border: $trade_name_border;
    --oa-trade-name-ink: $trade_name_ink;
    --oa-trade-more-ink: $trade_more_ink;
    --oa-campus-unh-bg: $campus_unh_bg;
    --oa-campus-unh-border: $campus_unh_border;
    --oa-campus-unh-ink: $campus_unh_ink;
    --oa-campus-mc-bg: $campus_mc_bg;
    --oa-campus-mc-border: $campus_mc_border;
    --oa-campus-mc-ink: $campus_mc_ink;
    --oa-campus-oncall-bg: $campus_oncall_bg;
    --oa-campus-oncall-border: $campus_oncall_border;
    --oa-campus-oncall-ink: $campus_oncall_ink;
    --oa-radius: 14px;
    --oa-radius-card: 12px;
    --oa-radius-control: 10px;
  }

  [data-testid="stAppViewContainer"],
  html,
  body,
  .stApp {
    background:
      radial-gradient(900px 500px at 12% 8%, var(--oa-page-glow-a), rgba(0,0,0,0)),
      radial-gradient(900px 520px at 88% 12%, var(--oa-page-glow-b), rgba(0,0,0,0)),
      var(--oa-page-bg);
    color: var(--oa-ink);
  }

  [data-testid="stHeader"],
  section[data-testid="stMain"],
  [data-testid="stMain"],
  [data-testid="stSidebar"] {
    background: transparent;
  }

  [data-testid="stDecoration"] {
    background-image: linear-gradient(90deg, var(--oa-accent), var(--oa-accent2));
    opacity: 0.96;
  }

  [data-testid="stAppViewContainer"] .main .block-container,
  [data-testid="stMainBlockContainer"],
  section[data-testid="stMain"] .block-container {
    margin-top: 0.85rem;
    background: var(--oa-shell-bg);
    border: 1px solid var(--oa-border);
    border-radius: var(--oa-radius);
    box-shadow: var(--oa-shadow);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    padding: 2rem 2rem 5.6rem 2rem;
  }

  h1, h2, h3, h4, h5, h6 {
    color: var(--oa-ink) !important;
    letter-spacing: -0.2px;
  }

  .stMarkdown,
  .stMarkdown p,
  .stMarkdown li,
  .stMarkdown div,
  p,
  label,
  span,
  .stCaption,
  small,
  [data-testid="stCaptionContainer"] {
    color: var(--oa-ink);
  }

  .stCaption,
  small,
  [data-testid="stCaptionContainer"] {
    color: var(--oa-ink-muted) !important;
  }

  [data-testid="stSidebar"] > div:first-child {
    background: var(--oa-sidebar-bg);
    border-right: 1px solid var(--oa-border);
  }

  [data-testid="stSidebar"] .stMarkdown,
  [data-testid="stSidebar"] label,
  [data-testid="stSidebar"] p,
  [data-testid="stSidebar"] span {
    color: var(--oa-ink) !important;
  }

  div[data-testid="stToggle"] {
    margin: 0.10rem 0 0.65rem 0;
  }

  div[data-testid="stToggle"] label {
    color: var(--oa-ink) !important;
  }

  div[data-testid="stToggle"] label[data-baseweb="checkbox"] {
    gap: 0.70rem;
    align-items: center;
  }

  div[data-testid="stToggle"] label[data-baseweb="checkbox"] > div:first-of-type {
    position: relative;
    min-width: 3.55rem !important;
    width: 3.55rem !important;
    height: 1.95rem !important;
    border-radius: 999px !important;
    border: 1px solid var(--oa-border) !important;
    background: linear-gradient(90deg, color-mix(in srgb, var(--oa-accent) 26%, transparent), color-mix(in srgb, var(--oa-accent2) 22%, transparent)) !important;
    box-shadow: inset 0 1px 2px rgba(255,255,255,0.10), var(--oa-shadow-soft) !important;
    overflow: hidden;
  }

  div[data-testid="stToggle"] label[data-baseweb="checkbox"] > div:first-of-type::before,
  div[data-testid="stToggle"] label[data-baseweb="checkbox"] > div:first-of-type::after {
    position: absolute;
    top: 50%;
    transform: translateY(-50%);
    font-size: 0.74rem;
    line-height: 1;
    z-index: 1;
    pointer-events: none;
    transition: opacity 140ms ease, transform 140ms ease;
  }

  div[data-testid="stToggle"] label[data-baseweb="checkbox"] > div:first-of-type::before {
    content: "☀️";
    left: 0.36rem;
    opacity: 0.92;
  }

  div[data-testid="stToggle"] label[data-baseweb="checkbox"] > div:first-of-type::after {
    content: "🌙";
    right: 0.34rem;
    opacity: 0.42;
  }

  div[data-testid="stToggle"] label[data-baseweb="checkbox"] > div:first-of-type > div {
    z-index: 2;
  }

  div[data-testid="stToggle"] label[data-baseweb="checkbox"]:has(input:checked) > div:first-of-type {
    background: linear-gradient(90deg, color-mix(in srgb, var(--oa-accent) 34%, transparent), color-mix(in srgb, var(--oa-accent2) 30%, transparent)) !important;
  }

  div[data-testid="stToggle"] label[data-baseweb="checkbox"]:has(input:checked) > div:first-of-type::before {
    opacity: 0.42;
    transform: translateY(-50%) scale(0.96);
  }

  div[data-testid="stToggle"] label[data-baseweb="checkbox"]:has(input:checked) > div:first-of-type::after {
    opacity: 0.96;
    transform: translateY(-50%) scale(1.04);
  }

  .stTextInput input,
  .stTextArea textarea,
  .stNumberInput input,
  .stDateInput input,
  .stTimeInput input {
    background: var(--oa-surface-strong) !important;
    color: var(--oa-ink) !important;
    border-radius: var(--oa-radius-control) !important;
    border: 1px solid var(--oa-input-border) !important;
    box-shadow: var(--oa-input-shadow);
  }

  [data-testid="stSelectbox"] div[data-baseweb="select"] > div,
  [data-testid="stMultiSelect"] div[data-baseweb="select"] > div {
    background: var(--oa-surface-strong) !important;
    color: var(--oa-ink) !important;
    border-radius: var(--oa-radius-control) !important;
    border: 1px solid var(--oa-input-border) !important;
    box-shadow: var(--oa-input-shadow);
  }

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
  .stTimeInput input:focus,
  [data-testid="stSelectbox"] div[data-baseweb="select"] > div:focus-within,
  [data-testid="stMultiSelect"] div[data-baseweb="select"] > div:focus-within {
    border-color: var(--oa-focus-border) !important;
    box-shadow: 0 0 0 3px var(--oa-focus-ring) !important;
  }

  .stButton > button {
    border: 0 !important;
    border-radius: 12px !important;
    padding: 0.46rem 0.82rem !important;
    font-weight: 700 !important;
    font-size: 0.92rem !important;
    color: var(--oa-button-text) !important;
    background: var(--oa-button-bg) !important;
    box-shadow: var(--oa-button-shadow);
    transition: transform 120ms ease, filter 120ms ease, box-shadow 120ms ease;
    white-space: nowrap !important;
    line-height: 1.05 !important;
  }

  .stButton > button:hover {
    filter: brightness(1.03);
    transform: translateY(-1px);
  }

  .stButton > button:active {
    transform: translateY(0px) scale(0.99);
  }

  .stButton > button[kind="secondary"] {
    background: var(--oa-secondary-bg) !important;
    color: var(--oa-secondary-text) !important;
    border: 1px solid var(--oa-secondary-border) !important;
    box-shadow: var(--oa-shadow-soft);
  }

  [data-testid="stTabs"] [role="tablist"] {
    gap: 0.45rem;
  }

  [data-testid="stTabs"] [role="tab"] {
    border-radius: 12px !important;
    padding: 0.40rem 0.85rem !important;
    background: var(--oa-tab-bg) !important;
    border: 1px solid var(--oa-tab-border) !important;
    box-shadow: var(--oa-shadow-soft);
  }

  [data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    background: var(--oa-tab-active-bg) !important;
    border-color: var(--oa-tab-active-border) !important;
  }

  [data-testid="stTabs"] div[role="tabpanel"] {
    background: var(--oa-panel-bg) !important;
    border: 1px solid var(--oa-card-border) !important;
    border-radius: 14px !important;
    padding: 0.85rem 0.85rem 0.55rem 0.85rem !important;
    margin-top: 0.55rem;
  }

  [data-testid="stExpander"] details,
  [data-testid="stMetric"],
  [data-testid="stAlert"],
  [data-testid="stDataFrame"],
  [data-testid="stPlotlyChart"],
  [data-testid="stTable"] {
    background: var(--oa-card-bg) !important;
    border: 1px solid var(--oa-card-border) !important;
    border-radius: var(--oa-radius-card) !important;
    box-shadow: var(--oa-shadow-soft) !important;
  }

  [data-testid="stExpander"] details > summary {
    border-radius: 12px !important;
    padding: 0.50rem 0.65rem !important;
    background: var(--oa-summary-bg) !important;
    border: 1px solid var(--oa-summary-border) !important;
  }

  [data-testid="stExpander"] details > div {
    background: transparent !important;
    padding-top: 0.25rem;
  }

  [data-testid="stDataFrame"] [role="grid"],
  [data-testid="stDataFrame"] [role="row"],
  [data-testid="stDataFrame"] [role="gridcell"] {
    background: transparent !important;
  }

  div[data-testid="stVerticalBlock"],
  div[data-testid="stHorizontalBlock"],
  div[data-testid="stContainer"],
  div[data-testid="stBlock"] {
    background: transparent !important;
  }

  pre,
  code {
    border-radius: 10px !important;
  }

  .oa-hero {
    padding: 1.05rem 1.15rem;
    border-radius: 18px;
    border: 1px solid var(--oa-hero-border);
    background: var(--oa-hero-bg);
    box-shadow: var(--oa-hero-shadow);
    margin-bottom: 1.05rem;
  }

  .oa-hero__title {
    font-size: 1.55rem;
    font-weight: 900;
    letter-spacing: -0.3px;
    color: var(--oa-ink);
  }

  .oa-hero__sub {
    margin-top: 0.25rem;
    font-size: 0.98rem;
    color: var(--oa-ink-muted);
  }

  .oa-hours-card {
    background: var(--oa-hours-bg);
    border: 1px solid var(--oa-border);
    border-radius: 14px;
    padding: 0.70rem 0.75rem 0.75rem 0.75rem;
    box-shadow: var(--oa-shadow-soft);
    margin: 0.25rem 0 0.55rem 0;
  }

  .oa-hours-card--classic .oa-hours-card__label {
    font-weight: 520;
    font-size: 0.95rem;
    color: color-mix(in srgb, var(--oa-ink) 78%, transparent);
    line-height: 1.15;
    margin-bottom: 0.15rem;
  }

  .oa-hours-card__scope {
    font-weight: 520;
    color: var(--oa-ink-muted);
  }

  .oa-hours-card__valueBig {
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
    color: var(--oa-ink-muted);
    margin-left: 0.15rem;
    vertical-align: baseline;
  }

  .oa-hours-card__subline,
  .oa-hours-card__hint,
  .oa-hours-card__sub {
    color: var(--oa-ink-muted);
  }

  .oa-hours-bar {
    height: 10px;
    border-radius: 999px;
    margin-top: 0.60rem;
    background: var(--oa-hours-track);
    overflow: hidden;
  }

  .oa-hours-bar__fill {
    height: 100%;
    border-radius: 999px;
    background: linear-gradient(90deg, var(--oa-accent), var(--oa-accent2));
    box-shadow: 0 6px 18px rgba(2, 6, 23, 0.14);
  }

  .oa-inline-metric {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
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

  .vp-global-footer .vp-global-footer__inner {
    background: var(--oa-footer-bg) !important;
    border: 1px solid var(--oa-footer-border) !important;
    color: var(--oa-footer-ink) !important;
    box-shadow: var(--oa-shadow-soft) !important;
  }
</style>
"""
)


def apply_vibrant_theme(mode: str = "light") -> None:
    """Apply the global theme.

    Important: inject CSS on every rerun so theme never "drops" when users
    click tabs or interact with widgets.
    """
    mode_key = "dark" if str(mode or "").strip().lower() == "dark" else "light"
    st.markdown(_CSS_TEMPLATE.substitute(_theme_tokens(mode_key)), unsafe_allow_html=True)
