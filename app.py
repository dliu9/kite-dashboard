from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path(__file__).parent / ".env")

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta, date, timezone

from db import Database
from data import DataFetcher
from correlation import (compute_impact_rows, compute_impact_rows_hourly,
                         EVENT_DESCRIPTIONS, HOURLY_METRICS, DAILY_METRICS)
import scraper
import insights as _ins

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="KITE Token Dashboard",
    page_icon="🪁",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

/* ══════════════════════════════════════════════════════════════
   KITE CYBER DARK THEME — complete coverage
   bg: #07071A  panel: #0D0D28  border: #1C1C42  cyan: #00E5FF
   ══════════════════════════════════════════════════════════════ */

/* ── Global backgrounds ── */
html, body, .stApp, [data-testid="stAppViewContainer"],
[data-testid="stMain"], .main { background-color: #07071A !important; }
.main .block-container { background-color: #07071A !important; padding-top: 1.5rem; }
[data-testid="stVerticalBlock"] { background-color: transparent !important; }

/* ── Sidebar ── */
section[data-testid="stSidebar"],
section[data-testid="stSidebar"] > div,
section[data-testid="stSidebar"] > div > div {
    background: #0A0A20 !important;
    border-right: 1px solid #1C1C42 !important;
}
section[data-testid="stSidebar"] * { color: #8899CC !important; }
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] .stTitle { color: #00E5FF !important; }
section[data-testid="stSidebar"] label { color: #6B82C0 !important; }
section[data-testid="stSidebar"] small,
section[data-testid="stSidebar"] .stCaption { color: #445580 !important; }

/* ── Typography ── */
h1, h2 { color: #E0EAFF !important; font-family: 'Syne', sans-serif !important; letter-spacing: -0.01em; }
h3 { color: #8AA8FF !important; font-family: 'Syne', sans-serif !important; }
h4, h5, h6 { color: #6B82C0 !important; }
p, div.stMarkdown p, .stCaption, small { color: #7A8FBF !important; }
label { color: #7A8FBF !important; }

/* ── All form input fields (text, number, date) ── */
input, textarea,
[data-baseweb="input"] input,
[data-baseweb="base-input"] input,
[data-testid="stDateInput"] input,
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input {
    background-color: #0D0D28 !important;
    color: #E0EAFF !important;
    border-color: #1C1C42 !important;
    caret-color: #00E5FF;
}
[data-baseweb="base-input"],
[data-baseweb="input"] {
    background-color: #0D0D28 !important;
    border-color: #1C1C42 !important;
}
[data-baseweb="base-input"]:focus-within,
[data-baseweb="input"]:focus-within { border-color: #00E5FF !important; box-shadow: 0 0 0 2px rgba(0,229,255,0.15) !important; }

/* ── Select boxes ── */
[data-baseweb="select"] > div,
.stSelectbox [data-baseweb="select"] > div {
    background-color: #0D0D28 !important;
    border-color: #1C1C42 !important;
    color: #E0EAFF !important;
}
[data-baseweb="select"] span,
[data-baseweb="select"] div[class*="singleValue"],
[data-baseweb="select"] div[class*="placeholder"] { color: #E0EAFF !important; }
[data-baseweb="select"] svg { fill: #5B6FA8 !important; }

/* ── Dropdown popovers (select options) ── */
[data-baseweb="popover"],
[data-baseweb="menu"],
ul[data-baseweb="menu"],
[role="listbox"] {
    background-color: #111135 !important;
    border: 1px solid #1C1C42 !important;
    box-shadow: 0 8px 32px rgba(0,0,0,0.6) !important;
}
[role="option"], li[role="option"] {
    background-color: #111135 !important;
    color: #C0CCEE !important;
}
[role="option"]:hover,
li[role="option"]:hover,
[aria-selected="true"] {
    background-color: #1C1C42 !important;
    color: #00E5FF !important;
}

/* ── Radio buttons ── */
[data-testid="stRadio"] > label { color: #7A8FBF !important; }
[data-testid="stRadio"] [role="radiogroup"] div { color: #C0CCEE !important; }
[data-testid="stRadio"] [role="radiogroup"] label { color: #C0CCEE !important; }
[data-testid="stRadio"] [data-baseweb="radio"] div[role="radio"] {
    border-color: #1C1C42 !important;
    background: #0D0D28 !important;
}
[data-testid="stRadio"] [data-baseweb="radio"][data-checked="true"] div[role="radio"],
[data-testid="stRadio"] input:checked + div { border-color: #00E5FF !important; }

/* ── Toggle / Checkbox ── */
[data-baseweb="checkbox"] div, [data-baseweb="toggle"] {
    background-color: #1C1C42 !important;
    border-color: #2A2A5A !important;
}
[data-baseweb="checkbox"][data-checked="true"] div,
[data-baseweb="toggle"][data-checked="true"] { background-color: #00E5FF !important; }
[data-testid="stToggle"] label { color: #C0CCEE !important; }

/* ── Sliders ── */
[data-testid="stSlider"] [role="slider"] { background: #00E5FF !important; border-color: #00E5FF !important; }
[data-testid="stSlider"] [data-testid="stSliderTrack"] { background: #1C1C42 !important; }
[data-testid="stSelectSlider"] [role="slider"] { background: #9D4EDD !important; }

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] { gap: 6px; background: transparent; border-bottom: 1px solid #1C1C42; }
.stTabs [data-baseweb="tab"] {
    background: #0D0D28 !important;
    border: 1px solid #1C1C42;
    border-bottom: none;
    border-radius: 8px 8px 0 0;
    color: #5B6FA8 !important;
    font-weight: 500;
    font-family: 'Syne', sans-serif;
    transition: color 0.2s;
}
.stTabs [aria-selected="true"] {
    background: #111135 !important;
    border-color: #2A2A5A;
    border-bottom: 2px solid #00E5FF !important;
    color: #00E5FF !important;
}
.stTabs [data-baseweb="tab"]:hover { color: #9BE8FF !important; }
[data-baseweb="tab-panel"] { background: transparent !important; }

/* ── Metric cards ── */
[data-testid="metric-container"] {
    background: linear-gradient(135deg, #0D0D28 0%, #111135 100%) !important;
    border: 1px solid rgba(0,229,255,0.18) !important;
    border-radius: 10px;
    padding: 14px 16px;
    box-shadow: 0 0 16px rgba(0,229,255,0.06), inset 0 1px 0 rgba(255,255,255,0.04);
}
[data-testid="metric-container"] label { color: #6B82C0 !important; font-size: 0.77rem !important; letter-spacing: 0.04em; text-transform: uppercase; }
[data-testid="metric-container"] [data-testid="metric-value"] { color: #E0EAFF !important; font-family: 'JetBrains Mono', monospace !important; }
[data-testid="metric-container"] [data-testid="metric-delta"] { font-size: 0.8rem; }

/* ── DataFrames ── */
hr { border-color: #1C1C42 !important; }
[data-testid="stDataFrame"],
[data-testid="stDataFrame"] > div,
[data-testid="stDataFrame"] iframe,
.stDataFrame { border: 1px solid #1C1C42 !important; border-radius: 8px !important; background: #0D0D28 !important; }
/* inner canvas rows of streamlit dataframe */
[data-testid="data-grid-canvas"] { background: #0D0D28 !important; }

/* ── Buttons ── */
.stButton button {
    background: linear-gradient(135deg, #0D0D28, #161648) !important;
    border: 1px solid rgba(0,229,255,0.3) !important;
    color: #00E5FF !important;
    border-radius: 8px !important;
    font-family: 'Syne', sans-serif !important;
    letter-spacing: 0.03em;
    transition: box-shadow 0.2s, border-color 0.2s;
}
.stButton button:hover { border-color: #00E5FF !important; box-shadow: 0 0 14px rgba(0,229,255,0.28) !important; }
button[kind="primary"],
[data-testid="stButton"] button[kind="primary"] {
    background: linear-gradient(135deg, #0096AA, #005F88) !important;
    border: 1px solid #00E5FF44 !important;
    color: #ffffff !important;
    box-shadow: 0 0 18px rgba(0,229,255,0.22) !important;
}

/* ── Alerts / info boxes ── */
[data-testid="stNotification"], [data-testid="stAlert"], .stAlert {
    border-radius: 8px !important;
    border-left-width: 3px !important;
    background: #0D0D28 !important;
}

/* ── Expanders ── */
[data-testid="stExpander"] { border-color: #1C1C42 !important; background: #0D0D28 !important; border-radius: 8px !important; }
[data-testid="stExpander"] summary { color: #8AA8FF !important; }

/* ── Progress bar (ProgressColumn in dataframe) ── */
[role="progressbar"] > div { background: linear-gradient(90deg, #00E5FF, #9D4EDD) !important; }

/* ── Scrollbars ── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #07071A; }
::-webkit-scrollbar-thumb { background: #1C1C42; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #2A2A5A; }

/* ══════════════════════════════════════════════════════════════
   AI INSIGHT PANEL — right-side slide dialog, 60% transparency
   ══════════════════════════════════════════════════════════════ */

/* Right-align and stretch the dialog to full height */
[data-testid="stDialog"] > div[role="dialog"] {
    position: fixed !important;
    right: 0 !important;
    left: auto !important;
    top: 0 !important;
    bottom: 0 !important;
    width: 38% !important;
    max-width: 38% !important;
    height: 100vh !important;
    max-height: 100vh !important;
    border-radius: 0 !important;
    background: rgba(13, 13, 40, 0.60) !important;
    backdrop-filter: blur(12px) !important;
    -webkit-backdrop-filter: blur(12px) !important;
    border-left: 1px solid rgba(0, 229, 255, 0.25) !important;
    box-shadow: -8px 0 32px rgba(0, 0, 0, 0.5) !important;
    overflow-y: auto !important;
    margin: 0 !important;
    transform: none !important;
}

/* Semi-transparent backdrop so chart stays visible on left */
[data-testid="stDialog"]::backdrop,
[data-testid="stDialogBackdrop"] {
    background: rgba(0, 0, 0, 0.35) !important;
    backdrop-filter: blur(2px) !important;
}

/* Insight panel inner content */
[data-testid="stDialog"] .stMarkdown p { color: #C0CCEE !important; }
[data-testid="stDialog"] h3 { color: #00E5FF !important; font-family: 'Syne', sans-serif !important; }
[data-testid="stDialog"] h4 { color: #8AA8FF !important; font-family: 'Syne', sans-serif !important; }

/* Clickable chart-title buttons — styled as subheaders */
button[data-testid^="stBaseButton"][key^="ih_"] {
    background: transparent !important;
    border: none !important;
    padding: 0 !important;
    color: #8AA8FF !important;
    font-family: 'Syne', sans-serif !important;
    font-size: 1.1rem !important;
    font-weight: 600 !important;
    text-align: left !important;
    cursor: pointer !important;
    text-decoration: none !important;
    width: auto !important;
}
button[data-testid^="stBaseButton"][key^="ih_"]:hover {
    color: #00E5FF !important;
    text-decoration: underline !important;
    text-underline-offset: 3px !important;
    background: transparent !important;
}
</style>
""", unsafe_allow_html=True)

# ── Shared resources ─────────────────────────────────────────────────────────
@st.cache_resource
def get_db():
    return Database()

@st.cache_resource
def get_fetcher():
    return DataFetcher()

db = get_db()
fetcher = get_fetcher()

# ── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://s2.coinmarketcap.com/static/img/coins/64x64/38828.png", width=56)
    st.title("🪁 KITE Dashboard")
    st.caption("Kite AI Token · Business Events Analysis")
    st.divider()

    # Date range
    st.subheader("📅 Date Range")
    c1, c2 = st.columns(2)
    start_date = c1.date_input("From", value=date(2025, 11, 1), min_value=date(2025, 11, 1))
    end_date = c2.date_input("To", value=date.today())
    st.divider()

    # Refresh controls
    st.subheader("🔄 Data Refresh")
    auto_refresh = st.toggle("Auto Refresh", value=False)
    if auto_refresh:
        refresh_mins = st.select_slider(
            "Interval (min)", options=[5, 10, 15, 30, 60, 120, 240], value=30
        )
        try:
            from streamlit_autorefresh import st_autorefresh
            st_autorefresh(interval=refresh_mins * 60 * 1000, key="auto_refresh_ticker")
        except ImportError:
            st.caption("Install `streamlit-autorefresh` for auto mode.")

    if st.button("⏱️ Refresh Hourly Prices (last 90d)", use_container_width=True, type="primary"):
        with st.spinner("Fetching hourly prices from CoinGecko…"):
            h_df = fetcher.get_historical_prices_hourly(days=90)
            if not h_df.empty:
                n = db.upsert_hourly_prices(h_df)
                db.log_refresh("price_hourly", n)
                st.toast(f"✅ {n} hourly price records saved")
            else:
                st.toast("⚠️ Hourly price fetch failed", icon="⚠️")
        st.rerun()

    if st.button("🔄 Refresh Price & Exchange Data", use_container_width=True):
        with st.spinner("Fetching price history from CoinGecko…"):
            df = fetcher.get_historical_prices(days=365)
            if not df.empty:
                n = db.upsert_prices(df)
                db.log_refresh("price_history", n)
                st.toast(f"✅ {n} price records saved")
            else:
                st.toast("⚠️ Price fetch failed", icon="⚠️")
        with st.spinner("Fetching exchange tickers…"):
            ex_df = fetcher.get_exchange_tickers()
            if not ex_df.empty:
                today_str = datetime.now().strftime("%Y-%m-%d")
                n = db.upsert_exchange_snapshots(ex_df, today_str)
                db.log_refresh("exchange_snapshots", n)
                st.toast(f"✅ {n} exchange records saved")
        st.rerun()

    # ── Tweet Fetch via twitterapi.io ────────────────────────────────────────
    st.subheader("🐦 Fetch Tweets")
    api_tweet_limit = st.select_slider(
        "Max tweets (budget control)",
        options=[20, 50, 100, 150, 200],
        value=100,
        help="Fetches in batches of 20 and stops as soon as tweets older than the start date appear. Lower = fewer API credits used.",
    )
    st.caption(f"Range: **{start_date.strftime('%Y-%m-%d')}** → **{end_date.strftime('%Y-%m-%d')}**")
    if st.button("🐦 Fetch Tweets via API", use_container_width=True, type="primary"):
        _s = start_date.strftime("%Y-%m-%d")
        _e = end_date.strftime("%Y-%m-%d")
        with st.spinner(f"Fetching up to {api_tweet_limit} tweets from @GoKiteAI ({_s} → {_e})…"):
            tweets, err = scraper.scrape_tweets_api(
                username="GoKiteAI",
                start_date=_s,
                end_date=_e,
                max_tweets=api_tweet_limit,
            )
        if err:
            st.error(f"API error: {err}")
        elif tweets:
            existing_ids = db.get_existing_tweet_ids()
            new_count = sum(
                1 for t in tweets
                if t["tweet_id"] not in existing_ids and db.add_event(t)
            )
            db.log_refresh("tweets", new_count)
            st.toast(f"✅ {new_count} new tweets added ({len(tweets)} fetched in range)")
            st.rerun()
        else:
            st.warning("No tweets found in the selected date range.")

    st.divider()

    # Last refresh timestamps
    st.caption(f"Prices last fetched: {db.get_last_refresh('price_history')[:16]}")
    st.caption(f"Tweets last fetched: {db.get_last_refresh('tweets')[:16]}")

# ── Load data ─────────────────────────────────────────────────────────────────
start_str = start_date.strftime("%Y-%m-%d")
end_str = end_date.strftime("%Y-%m-%d")

price_df = db.get_prices(start_str, end_str)
# Extend end by 3 days so correlation can look up forward prices (T+3d) near the range edge
_hourly_end = (end_date + timedelta(days=3)).strftime("%Y-%m-%d") + " 23:59"
hourly_df = db.get_hourly_prices(start_str, _hourly_end)
events_df = db.get_events(start_str, end_str)
exchange_df = db.get_latest_exchange_snapshots()

@st.cache_data(ttl=300)
def get_current_snapshot_cached():
    return fetcher.get_current_snapshot()

current = get_current_snapshot_cached()

# ── Cyber Dark colour palette ──────────────────────────────────────────────────
_ORANGE  = "#F5A623"   # KITE brand accent
_CYAN    = "#00E5FF"   # primary neon cyan
_BLUE    = "#00B4FF"   # electric blue
_GREEN   = "#00E5A0"   # neon teal-green (positive / up)
_RED     = "#FF2D6F"   # hot crimson-pink (negative / down)
_PURPLE  = "#9D4EDD"   # vivid purple
_PINK    = "#FF3DDA"   # hot magenta
_TEAL    = "#00BCD4"   # medium teal
_INDIGO  = "#7B61FF"   # electric violet
_GREY    = "#5B6FA8"   # blue-grey neutral
_BG      = "#07071A"   # page background
_PANEL   = "#0D0D28"   # chart panel background
_GRID    = "#1C1C42"   # subtle gridlines
_TMPL    = "plotly_dark"

# Chart hover: high-contrast on dark bg
_HOVER = dict(font_size=14, bgcolor="#0D0D28", bordercolor="#00E5FF", font_color="#E0EAFF")
# Plotly chart config: enable scroll-zoom & draggable pan
_PCFG  = {"scrollZoom": True, "displayModeBar": True, "modeBarButtonsToRemove": ["select2d","lasso2d"]}

# Sentiment colours
_SENT_CLR = {"positive": _GREEN, "negative": _PINK, "neutral": _GREY}

# Per-event-type marker colours — vivid neon palette for dark bg
_EVT_CLR = {
    "Partnership":    _CYAN,
    "Milestone":      _ORANGE,
    "Listing":        _GREEN,
    "Funding":        _PURPLE,
    "Announcement":   _GREY,
    "Community":      _PINK,
    "Product Launch": "#FF9100",
    "Airdrop":        "#00FF9C",
    "Security":       _RED,
    "Regulation":     "#E040FB",
}

# DEX contract address → readable ticker
_QUOTE_MAP = {
    "0X55D398326F99059FF775485246999027B3197955": "USDT(BSC)",
    "0XA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48": "USDC(ETH)",
}


_TABLE_STYLES = [
    {"selector": "thead tr th", "props": "background-color: #111135; color: #8AA8FF; border-bottom: 1px solid #1C1C42; font-family: 'Syne', sans-serif; font-size: 0.78rem; letter-spacing: 0.05em; text-transform: uppercase;"},
    {"selector": "tbody tr td", "props": "background-color: #0D0D28; color: #C0CCEE; border-bottom: 1px solid #141430;"},
    {"selector": "tbody tr:hover td", "props": "background-color: #141440 !important;"},
    {"selector": "table", "props": "border-collapse: collapse; width: 100%;"},
]


def _pct_styler(df, cols):
    """Return a dark-themed pandas Styler with ▲/▼ symbols on % columns."""
    present = [c for c in cols if c in df.columns]

    def _fmt(v):
        if not isinstance(v, (int, float)) or pd.isna(v):
            return "–"
        sym = "▲" if v > 0 else ("▼" if v < 0 else " ")
        return f"{sym} {abs(v):.2f}%"

    def _clr(v):
        if not isinstance(v, (int, float)) or pd.isna(v):
            return "background-color: #0D0D28; color: #5B6FA8"
        if v > 0:  return "background-color: #0D1F18; color: #00E5A0; font-weight: 600"
        if v < 0:  return "background-color: #1F0D18; color: #FF2D6F; font-weight: 600"
        return "background-color: #0D0D28; color: #5B6FA8"

    styler = df.style.set_properties(**{
        "background-color": "#0D0D28",
        "color": "#C0CCEE",
        "border-color": "#1C1C42",
    }).set_table_styles(_TABLE_STYLES)

    if present:
        styler = styler.format({c: _fmt for c in present}).map(_clr, subset=present)

    return styler


def _apply_dark(fig):
    """Apply consistent cyber-dark panel styling to every Plotly figure."""
    fig.update_layout(
        plot_bgcolor=_PANEL,
        paper_bgcolor=_BG,
        font_color="#E0EAFF",
        title_font_color="#E0EAFF",
        legend=dict(bgcolor="rgba(13,13,40,0.8)", bordercolor=_GRID, borderwidth=1, font_color="#C0CCEE"),
    )
    fig.update_xaxes(gridcolor=_GRID, zerolinecolor=_GRID, linecolor=_GRID, tickfont_color="#7A8FBF", title_font_color="#8AA8FF")
    fig.update_yaxes(gridcolor=_GRID, zerolinecolor=_GRID, linecolor=_GRID, tickfont_color="#7A8FBF", title_font_color="#8AA8FF")
    return fig


# ── AI Insight Panel ──────────────────────────────────────────────────────────

@st.dialog("🔍 AI Insights", width="large")
def _show_insight_dialog(chart_id: str, snapshot: str):
    reg  = _ins.CHART_REGISTRY.get(chart_id, {})
    title = reg.get("title", chart_id)
    st.markdown(f"### {title}")
    st.divider()

    st.markdown("#### 🎯 Objective")
    st.markdown(reg.get("objective", "—"))

    cache_key = f"_insight_cache_{chart_id}"
    if cache_key not in st.session_state:
        with st.spinner("Generating insights…"):
            st.session_state[cache_key] = _ins.generate_insights(chart_id, snapshot)

    raw = st.session_state[cache_key]
    parts = raw.split("---CTA---")
    insights_text = parts[0].strip()
    cta_text      = parts[1].strip() if len(parts) > 1 else ""

    st.divider()
    st.markdown("#### 💡 Key Insights")
    for line in insights_text.split("\n"):
        if line.strip():
            st.markdown(line)

    if cta_text:
        st.divider()
        st.markdown("#### 🚀 Priority Actions")
        st.caption("As a business & marketing decision-maker, act on these now:")
        for line in cta_text.split("\n"):
            if line.strip():
                st.markdown(line)

    st.divider()
    st.markdown("#### 🗄️ Data Source")
    st.caption(reg.get("data_source", "—"))

    if st.button("🔄 Regenerate", key=f"regen_{chart_id}"):
        del st.session_state[cache_key]
        st.rerun()


def chart_header(title: str, chart_id: str, snapshot: str = ""):
    """Render a clickable subheader. Click opens the AI insight panel."""
    if st.button(f"✨ {title}", key=f"ih_{chart_id}"):
        _show_insight_dialog(chart_id, snapshot)
    else:
        # Always render the visual subheader text so layout is consistent
        pass



# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_overview, tab_events, tab_corr, tab_exchanges, tab_analytics, tab_risk = st.tabs([
    "📊 Overview", "📅 Events", "🔗 Correlation",
    "🏦 Exchanges", "📈 Analytics", "⚠️ Risk & Signals"
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
with tab_overview:
    st.header("Current Snapshot")

    if current:
        def arrow(v):
            return "▲" if v >= 0 else "▼"

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Price (USD)", f"${current['price']:.4f}", f"{current['pct_24h']:+.2f}% 24h")
        col2.metric("Market Cap", f"${current['market_cap']/1e6:.1f}M")
        col3.metric("24h Volume", f"${current['volume_24h']/1e6:.1f}M")
        col4.metric("CEX Volume", f"${current['cex_volume_24h']/1e6:.1f}M")
        col5.metric("DEX Volume", f"${current['dex_volume_24h']/1e6:.1f}M")

        col6, col7, col8, col9 = st.columns(4)
        col6.metric("1h", f"{current['pct_1h']:+.2f}%")
        col7.metric("7d", f"{current['pct_7d']:+.2f}%")
        col8.metric("30d", f"{current['pct_30d']:+.2f}%")
        col9.metric("FDV", f"${current['fdv']/1e9:.2f}B")
    else:
        st.info("Could not fetch live price. Showing cached data only.")

    st.divider()

    if price_df.empty and hourly_df.empty:
        st.info("No price data yet. Click **Refresh** buttons in the sidebar.")
    else:
        # ── Chart resolution toggle ──
        chart_view = st.radio(
            "Chart resolution",
            ["Hourly (last 30d)", "Daily (full history)"],
            horizontal=True,
            index=0 if not hourly_df.empty else 1,
        )

        use_hourly = chart_view.startswith("Hourly") and not hourly_df.empty
        chart_df = hourly_df if use_hourly else price_df
        x_col    = "datetime" if use_hourly else "date"

        # ── Price chart with 7-day moving average and event markers ──
        chart_header("Price Trend with Event Impact", "price_trend",
                     _ins.get_data_snapshot("price_trend", chart_df=chart_df, current=current))

        fig = go.Figure()

        # Main price line — electric cyan with subtle glow fill
        fig.add_trace(go.Scatter(
            x=chart_df[x_col], y=chart_df["price_usd"],
            mode="lines", name="Price (USD)",
            line=dict(color=_CYAN, width=1.8 if use_hourly else 2.2),
            fill="tozeroy", fillcolor="rgba(0,229,255,0.07)",
        ))

        # 7-day / 168-hour moving average — purple dashed
        if len(chart_df) > 7:
            ma_window = 168 if use_hourly else 7
            ma = chart_df["price_usd"].rolling(window=ma_window).mean()
            fig.add_trace(go.Scatter(
                x=chart_df[x_col], y=ma,
                mode="lines", name="7-day MA",
                line=dict(color=_PURPLE, width=2, dash="dash"),
                fill=None,
            ))

        # Event markers with different shapes per event type
        sentiment_colors = _SENT_CLR
        event_type_symbols = {
            "Partnership": "diamond",
            "Milestone": "star",
            "Listing": "triangle-up",
            "Funding": "circle",
            "Announcement": "square",
            "Community": "cross",
            "Product Launch": "hexagon",
            "Airdrop": "bowtie",
            "Security": "x",
            "Regulation": "triangle-down",
        }

        if not events_df.empty:
            if use_hourly:
                h_indexed = hourly_df.set_index("datetime")["price_usd"]
                for _, ev in events_df.iterrows():
                    dt_str = (ev.get("datetime_str") or ev["date"] + " 00:00:00")
                    try:
                        from datetime import datetime as _dt
                        d = _dt.strptime(dt_str[:16], "%Y-%m-%d %H:%M")
                        if d.minute >= 30:
                            from datetime import timedelta as _td
                            d = d.replace(minute=0) + _td(hours=1)
                        else:
                            d = d.replace(minute=0)
                        ev_hour = d.strftime("%Y-%m-%d %H:00")
                    except Exception:
                        continue
                    if ev_hour not in h_indexed.index:
                        continue
                    color = _EVT_CLR.get(ev.get("event_type", ""), _GREY)
                    ev_type = ev.get("event_type", "")
                    symbol = event_type_symbols.get(ev_type, "star")
                    type_desc = EVENT_DESCRIPTIONS.get(ev_type, "")
                    fig.add_trace(go.Scatter(
                        x=[ev_hour], y=[h_indexed[ev_hour]],
                        mode="markers",
                        marker=dict(size=12, color=color, symbol=symbol,
                                    line=dict(color="white", width=1.5)),
                        name=ev_type,
                        text=[f"{ev.get('description','')[:60]}<br><i>{type_desc}</i>"],
                        hovertemplate=(
                            f"<b>{ev_type}</b><br>"
                            f"{ev_hour}<br>"
                            "%{text}<br>"
                            "Price: $%{y:.5f}<extra></extra>"
                        ),
                        showlegend=False,
                    ))
            else:
                price_indexed = price_df.set_index("date")["price_usd"]
                for _, ev in events_df.iterrows():
                    if ev["date"] not in price_indexed.index:
                        continue
                    color = _EVT_CLR.get(ev.get("event_type", ""), _GREY)
                    ev_type = ev.get("event_type", "")
                    symbol = event_type_symbols.get(ev_type, "star")
                    type_desc = EVENT_DESCRIPTIONS.get(ev_type, "")
                    fig.add_trace(go.Scatter(
                        x=[ev["date"]], y=[price_indexed[ev["date"]]],
                        mode="markers",
                        marker=dict(size=12, color=color, symbol=symbol,
                                    line=dict(color="white", width=1.5)),
                        name=ev_type,
                        text=[f"{ev.get('description','')[:60]}<br><i>{type_desc}</i>"],
                        hovertemplate=(
                            f"<b>{ev_type}</b><br>"
                            f"{ev['date']}<br>"
                            "%{text}<br>"
                            "Price: $%{y:.5f}<extra></extra>"
                        ),
                        showlegend=False,
                    ))

        title = "KITE Price (Hourly) with Event Markers" if use_hourly else "KITE Price History with Event Markers"
        fig.update_layout(
            title=title,
            xaxis_title="Datetime" if use_hourly else "Date",
            yaxis_title="Price (USD)",
            hovermode="x unified", template=_TMPL,
            hoverlabel=_HOVER,
            height=460, margin=dict(l=0, r=0, t=40, b=0),
            plot_bgcolor=_PANEL, paper_bgcolor=_BG,
            xaxis=dict(gridcolor=_GRID, zerolinecolor=_GRID),
            yaxis=dict(gridcolor=_GRID, zerolinecolor=_GRID),
        )
        _apply_dark(fig)
        st.plotly_chart(fig, use_container_width=True, config=_PCFG)

        # Event marker legend
        if not events_df.empty:
            present_types = events_df["event_type"].dropna().unique().tolist()
            legend_parts = [
                f"{k} = {v.replace('-', ' ')}"
                for k, v in event_type_symbols.items()
                if k in present_types
            ]
            if legend_parts:
                st.caption("**Event Shapes:** " + " | ".join(legend_parts) +
                           " · Colors: 🩵 Positive  🩷 Negative  ◼ Neutral")

        # ── Performance Summary ──
        if not chart_df.empty:
            chart_header("Period Performance Summary", "period_performance",
                         _ins.get_data_snapshot("period_performance", chart_df=chart_df, current=current))
            period_high = chart_df["price_usd"].max()
            period_low  = chart_df["price_usd"].min()

            try:
                if use_hourly:
                    _cd = chart_df.copy()
                    _cd["_day"] = pd.to_datetime(_cd["datetime"]).dt.date
                    daily_data = _cd.groupby("_day")["price_usd"].agg(["first", "last"])
                    daily_data["pct_change"] = (daily_data["last"] / daily_data["first"] - 1) * 100
                else:
                    _cd = chart_df[["price_usd"]].copy()
                    _cd["pct_change"] = _cd["price_usd"].pct_change() * 100
                    daily_data = _cd
                best_day_pct  = daily_data["pct_change"].max()
                worst_day_pct = daily_data["pct_change"].min()
            except Exception:
                best_day_pct = worst_day_pct = 0.0

            pc1, pc2, pc3, pc4 = st.columns(4)
            pc1.metric("Period High",  f"${period_high:.4f}")
            pc2.metric("Period Low",   f"${period_low:.4f}")
            pc3.metric("Best Day %",   f"{best_day_pct:+.2f}%")
            pc4.metric("Worst Day %",  f"{worst_day_pct:+.2f}%")

        # ── Volume chart — green up days, red down days ──
        chart_header("Trading Volume", "trading_volume",
                     _ins.get_data_snapshot("trading_volume", chart_df=chart_df, current=current))
        fig_vol = go.Figure()

        try:
            if use_hourly:
                _cv = chart_df.copy()
                _cv["_day"] = pd.to_datetime(_cv["datetime"]).dt.date
                daily_vol = _cv.groupby("_day").agg(
                    volume=("volume_24h", "sum"),
                    price_open=("price_usd", "first"),
                    price_close=("price_usd", "last"),
                ).reset_index()
                bar_colors = [
                    _CYAN if r["price_close"] >= r["price_open"] else _PURPLE
                    for _, r in daily_vol.iterrows()
                ]
                fig_vol.add_trace(go.Bar(x=daily_vol["_day"], y=daily_vol["volume"],
                                         marker_color=bar_colors, opacity=0.90,
                                         marker_line_width=0))
            else:
                _cv = chart_df.copy()
                _cv["price_prev"] = _cv["price_usd"].shift(1)
                bar_colors = [_CYAN] + [
                    _CYAN if r["price_usd"] >= r["price_prev"] else _PURPLE
                    for _, r in _cv.iloc[1:].iterrows()
                ]
                fig_vol.add_trace(go.Bar(x=_cv[x_col], y=_cv["volume_24h"],
                                         marker_color=bar_colors, opacity=0.90,
                                         marker_line_width=0))
        except Exception:
            fig_vol.add_trace(go.Bar(x=chart_df[x_col], y=chart_df["volume_24h"],
                                      marker_color=_CYAN, opacity=0.85, marker_line_width=0))

        fig_vol.update_layout(
            title="Hourly Trading Volume" if use_hourly else "Daily Trading Volume",
            xaxis_title="Datetime" if use_hourly else "Date",
            yaxis_title="Volume (USD)",
            template=_TMPL, hoverlabel=_HOVER, height=260,
            margin=dict(l=0, r=0, t=40, b=0),
            plot_bgcolor=_PANEL, paper_bgcolor=_BG,
            xaxis=dict(gridcolor=_GRID, zerolinecolor=_GRID),
            yaxis=dict(gridcolor=_GRID, zerolinecolor=_GRID),
            showlegend=False,
        )
        st.caption("■ Cyan = price up day  ■ Purple = price down day")
        _apply_dark(fig_vol)
        st.plotly_chart(fig_vol, use_container_width=True, config=_PCFG)

        # ── Trend summary ──
        if current:
            chart_header("Trend Overview", "trend_overview",
                         _ins.get_data_snapshot("trend_overview", current=current))
            tc1, tc2, tc3 = st.columns(3)
            tc1.metric("7 Day", f"{current['pct_7d']:+.2f}%",
                       "▲ Uptrend" if current['pct_7d'] > 0 else "▼ Downtrend")
            tc2.metric("30 Day", f"{current['pct_30d']:+.2f}%",
                       "▲ Uptrend" if current['pct_30d'] > 0 else "▼ Downtrend")
            if current.get('pct_60d') is not None:
                tc3.metric("60 Day", f"{current['pct_60d']:+.2f}%",
                           "▲ Uptrend" if current['pct_60d'] > 0 else "▼ Downtrend")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — EVENTS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_events:
    st.header("Business Event Timeline")

    left, right = st.columns([3, 1])

    with right:
        st.subheader("➕ Add Event")
        with st.form("add_event_form", clear_on_submit=True):
            ev_date = st.date_input("Date", value=date.today())
            ev_type = st.selectbox("Type", [
                "Partnership", "Product Launch", "Listing", "Funding",
                "Airdrop", "Milestone", "Community", "Security", "Regulation", "Announcement",
            ])
            ev_desc = st.text_area("Description", height=100,
                                   placeholder="Describe the event…")
            ev_impact = st.selectbox("Expected Impact", ["bullish", "neutral", "bearish"])
            ev_url = st.text_input("Source URL (optional)")
            if st.form_submit_button("Add Event", use_container_width=True, type="primary"):
                if ev_desc.strip():
                    score, label = scraper.score_sentiment(ev_desc)
                    db.add_event({
                        "date": ev_date.strftime("%Y-%m-%d"),
                        "datetime_str": ev_date.strftime("%Y-%m-%d 00:00:00"),
                        "event_type": ev_type,
                        "description": ev_desc.strip(),
                        "source": "manual",
                        "tweet_id": None,
                        "tweet_url": ev_url.strip() or None,
                        "tweet_text": ev_desc.strip(),
                        "sentiment_score": score,
                        "sentiment_label": label,
                        "expected_impact": ev_impact,
                    })
                    st.success("Event added!")
                    st.rerun()

        st.divider()
        st.subheader("🗑️ Delete Event")
        del_id = st.number_input("Event ID", min_value=1, step=1, label_visibility="collapsed")
        if st.button("Delete by ID", use_container_width=True, type="secondary"):
            db.delete_event(int(del_id))
            st.rerun()

    with left:
        if events_df.empty:
            st.info("No events yet. Fetch tweets from the sidebar or add events manually.")
        else:
            type_filter = st.selectbox(
                "Filter by type",
                ["All"] + sorted(events_df["event_type"].dropna().unique().tolist()),
            )
            display_df = events_df if type_filter == "All" else events_df[events_df["event_type"] == type_filter]

            show_cols = ["id", "date", "event_type", "description", "sentiment_label",
                         "sentiment_score", "expected_impact", "source"]
            show_cols = [c for c in show_cols if c in display_df.columns]

            st.dataframe(
                _pct_styler(display_df[show_cols], []),
                use_container_width=True, height=420,
                column_config={
                    "description": st.column_config.TextColumn("Description", width="large"),
                    "sentiment_score": st.column_config.ProgressColumn(
                        "Sentiment Score", min_value=-1.0, max_value=1.0, format="%.2f",
                    ),
                    "sentiment_label": st.column_config.TextColumn("Sentiment"),
                },
            )

    # ── Charts row ──
    if not events_df.empty:
        st.divider()
        chart_header("Event Activity Analysis", "event_activity",
                     _ins.get_data_snapshot("event_activity", events_df=events_df))

        # Event velocity
        try:
            _ev_dates = pd.to_datetime(events_df["date"], errors="coerce")
            _today_ts = pd.Timestamp.today()
            _last30 = (_ev_dates >= _today_ts - timedelta(days=30)).sum()
            _prior30 = ((_ev_dates >= _today_ts - timedelta(days=60)) &
                        (_ev_dates < _today_ts - timedelta(days=30))).sum()
            _last_wk  = _last30 / 4.29
            _prior_wk = _prior30 / 4.29
            _vel_chg  = ((_last_wk - _prior_wk) / _prior_wk * 100) if _prior_wk > 0 else 0

            vc1, vc2, vc3 = st.columns(3)
            vc1.metric("Events (last 30d)", int(_last30))
            vc2.metric("Events/Week (last 30d)", f"{_last_wk:.1f}")
            vc3.metric("Velocity vs Prior 30d", f"{_vel_chg:+.1f}%",
                       "▲ Accelerating" if _vel_chg > 0 else "▼ Decelerating")
        except Exception:
            pass

        ca, cb, cc = st.columns([1.5, 1.5, 1])

        with ca:
            chart_header("Events by Type", "events_by_type",
                         _ins.get_data_snapshot("events_by_type", events_df=events_df))
            type_counts = events_df["event_type"].value_counts().reset_index()
            type_counts.columns = ["Event Type", "Count"]
            fig_hbar = px.bar(
                type_counts, y="Event Type", x="Count", orientation="h",
                title="Events by Type",
                color="Count", color_continuous_scale=[_GREY, _ORANGE],
                template=_TMPL,
            )
            fig_hbar.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=0),
                                   showlegend=False, coloraxis_showscale=False)
            _apply_dark(fig_hbar)
            st.plotly_chart(fig_hbar, use_container_width=True, config=_PCFG)

        with cb:
            chart_header("Events per Month", "events_per_month",
                         _ins.get_data_snapshot("events_per_month", events_df=events_df))
            try:
                _ev2 = events_df.copy()
                _ev2["month"] = _ev2["date"].astype(str).str[:7]
                monthly = _ev2.groupby("month").size().reset_index(name="Count")
                dom_type = _ev2.groupby("month")["event_type"].apply(
                    lambda x: x.mode()[0] if not x.mode().empty else "Other"
                )
                type_color_map = {
                    "Partnership": _BLUE, "Product Launch": _GREEN, "Listing": _RED,
                    "Funding": _PURPLE, "Milestone": _ORANGE, "Community": _TEAL,
                }
                monthly_colors = [type_color_map.get(t, "#9aa0a6") for t in dom_type]
                fig_monthly = go.Figure(go.Bar(
                    x=monthly["month"], y=monthly["Count"],
                    marker_color=monthly_colors,
                    text=monthly["Count"], textposition="outside",
                ))
                fig_monthly.update_layout(
                    title="Events per Month", template=_TMPL, hoverlabel=_HOVER, height=320,
                    margin=dict(l=0, r=0, t=40, b=0), showlegend=False,
                )
                _apply_dark(fig_monthly)
                st.plotly_chart(fig_monthly, use_container_width=True, config=_PCFG)
            except Exception:
                st.caption("Monthly frequency chart unavailable.")

        with cc:
            chart_header("Sentiment Split", "sentiment_split",
                         _ins.get_data_snapshot("sentiment_split", events_df=events_df))
            sent_counts = events_df["sentiment_label"].value_counts().reset_index()
            sent_counts.columns = ["Sentiment", "Count"]
            fig_sent = px.pie(
                sent_counts, values="Count", names="Sentiment",
                title="Sentiment Split",
                color="Sentiment",
                color_discrete_map=_SENT_CLR,
                template=_TMPL, hole=0.4,
            )
            fig_sent.update_layout(height=320, margin=dict(l=0, r=0, t=40, b=0))
            _apply_dark(fig_sent)
            st.plotly_chart(fig_sent, use_container_width=True, config=_PCFG)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — CORRELATION
# ═══════════════════════════════════════════════════════════════════════════════
with tab_corr:
    st.header("Event → Price Correlation Analysis")

    if events_df.empty:
        st.info("No events yet. Fetch tweets or add events manually.")
    elif hourly_df.empty and price_df.empty:
        st.info("Need price data. Click a Refresh button in the sidebar.")
    else:
        if not hourly_df.empty:
            impact_df = compute_impact_rows_hourly(hourly_df, events_df, price_df)
            available_metrics = HOURLY_METRICS
            date_col = "Datetime"
            hourly_count = (impact_df["Resolution"] == "hourly").sum() if "Resolution" in impact_df.columns else 0
            daily_count  = (impact_df["Resolution"] == "daily").sum()  if "Resolution" in impact_df.columns else 0
            st.caption(f"{hourly_count} events matched hourly (T+1h–T+3d) · {daily_count} older events matched daily (T+24h, T+3d only)")
        else:
            impact_df = compute_impact_rows(price_df, events_df)
            available_metrics = DAILY_METRICS
            date_col = "Date"
            st.caption("Hourly data not loaded — using daily. Click **⏱️ Refresh Hourly Prices** for finer resolution.")

        valid_metrics = [m for m in available_metrics if m in impact_df.columns]

        if impact_df.empty:
            st.warning("No events overlap with the price data range. Try refreshing hourly prices.")
        else:
            metric = st.radio("Time window to analyse", valid_metrics, horizontal=True)

            st.divider()

            # ── Signal Quality Scorecard ──
            chart_header("Signal Quality Scorecard", "signal_scorecard",
                         _ins.get_data_snapshot("signal_scorecard", impact_df=impact_df))
            _score_metric = valid_metrics[0] if valid_metrics else None
            if _score_metric and not impact_df.empty:
                _avg_by_type = impact_df.groupby("Event Type")[_score_metric].mean().dropna()
                _best_type   = _avg_by_type.idxmax() if not _avg_by_type.empty else "N/A"
                _best_ret    = _avg_by_type.max()    if not _avg_by_type.empty else 0.0
                _t1d_metric  = next((m for m in valid_metrics if "1d" in m.lower() or "24h" in m.lower()), _score_metric)
                _t1d_col     = impact_df[_t1d_metric].dropna()
                _pos_count   = int((_t1d_col > 0).sum())
                _neg_count   = int((_t1d_col <= 0).sum())
                _total_cnt   = _pos_count + _neg_count
                _hit_rate    = (_pos_count / _total_cnt * 100) if _total_cnt > 0 else 0.0

                sc1, sc2, sc3, sc4 = st.columns(4)
                sc1.metric(f"Best Event Type ({_score_metric})", _best_type, f"{_best_ret:+.2f}%")
                sc2.metric("Events — Positive T+1d", str(_pos_count))
                sc3.metric("Events — Negative T+1d", str(_neg_count))
                sc4.metric("Overall Hit Rate", f"{_hit_rate:.1f}%")

            st.divider()

            # ── Summary table with Signal column ──
            chart_header("Average Price Change by Event Type", "avg_price_change",
                         _ins.get_data_snapshot("avg_price_change", impact_df=impact_df))
            _sum_cols = valid_metrics + ["Vol Spike %"]
            _summary  = impact_df.groupby("Event Type")[_sum_cols].mean().round(2)
            _summary["# Events"]     = impact_df.groupby("Event Type").size()
            _summary["What it means"] = _summary.index.map(lambda t: EVENT_DESCRIPTIONS.get(t, ""))

            def _signal_label(v):
                if pd.isna(v):   return "— N/A"
                if v > 2.0:      return "🟢 Strong"
                if v >= 0.0:     return "🟡 Weak"
                return "🔴 Negative"

            _summary["Signal"] = _summary[metric].apply(_signal_label)
            st.dataframe(
                _pct_styler(_summary.sort_values(metric, ascending=False), valid_metrics),
                use_container_width=True,
                column_config={
                    "What it means": st.column_config.TextColumn(width="large"),
                    "Signal": st.column_config.TextColumn("Signal"),
                },
            )
            st.caption("🟢 Strong: avg > 2% · 🟡 Weak: 0–2% · 🔴 Negative: < 0%")

            st.divider()

            # ── Return Heatmap ──
            chart_header("Return Heatmap by Event Type & Time Window", "return_heatmap",
                         _ins.get_data_snapshot("return_heatmap", impact_df=impact_df))
            _heatmap_df = impact_df.groupby("Event Type")[valid_metrics].mean().round(2)
            # Fill NaN cells with 0 so every cell renders — NaN causes blank patches
            _heatmap_df = _heatmap_df.fillna(0)
            if not _heatmap_df.empty and len(valid_metrics) >= 1:
                fig_heat = px.imshow(
                    _heatmap_df,
                    color_continuous_scale=[[0, _RED], [0.5, "#1A1A3F"], [1, _GREEN]],
                    zmin=-5, zmax=5, text_auto=".2f", aspect="auto",
                    title="Return Heatmap by Event Type & Time Window",
                    labels={"color": "Avg Return (%)"},
                    template=_TMPL,
                )
                fig_heat.update_layout(margin=dict(l=0, r=0, t=50, b=0))
                _apply_dark(fig_heat)
                st.plotly_chart(fig_heat, use_container_width=True, config=_PCFG)
                st.caption("Each cell = mean price change (%). Green = rose · Red = fell · Dark = no data (shown as 0)")
            else:
                st.info("Not enough data to render heatmap.")

            st.divider()

            # ── Horizontal average bar chart ──
            chart_header("Average Return by Event Type", "avg_return_bar",
                         _ins.get_data_snapshot("avg_return_bar", impact_df=impact_df))
            _avg_by_type2 = (
                impact_df.groupby("Event Type")[metric].mean().dropna()
                .reset_index().sort_values(metric, ascending=True)
            )
            _avg_by_type2["Type Info"] = _avg_by_type2["Event Type"].map(lambda t: EVENT_DESCRIPTIONS.get(t, ""))
            fig_bar = px.bar(
                _avg_by_type2, x=metric, y="Event Type", orientation="h",
                title=f"Average {metric} by Event Type",
                color=metric, color_continuous_scale=[_RED, "white", _GREEN],
                text=metric, template=_TMPL,
                hover_data={"Type Info": True},
            )
            fig_bar.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
            fig_bar.add_vline(x=0, line_dash="dash", line_color="gray")
            fig_bar.update_layout(margin=dict(l=0, r=0, t=50, b=0), coloraxis_showscale=False)
            _apply_dark(fig_bar)
            st.plotly_chart(fig_bar, use_container_width=True, config=_PCFG)

            st.divider()

            # ── Top 5 Best / Worst Events ──
            chart_header(f"Standout Events — {metric}", "standout_events",
                         _ins.get_data_snapshot("standout_events", impact_df=impact_df))
            _ranked = impact_df.dropna(subset=[metric]).copy()
            _ranked_show = _ranked[[date_col, "Event Type", "Description", metric]].copy()
            _ranked_show["Description"] = _ranked_show["Description"].str[:50]

            tb5, tw5 = st.columns(2)
            with tb5:
                st.markdown("**Top 5 Best Events**")
                st.dataframe(_pct_styler(_ranked_show.nlargest(5, metric), [metric]),
                             use_container_width=True)
            with tw5:
                st.markdown("**Top 5 Worst Events**")
                st.dataframe(_pct_styler(_ranked_show.nsmallest(5, metric), [metric]),
                             use_container_width=True)

            st.divider()

            # ── Early vs Late reaction dual bar ──
            # Prefer T+24h % vs T+3d % — available for both hourly and daily events.
            # Fall back to first two valid_metrics if those columns aren't present.
            if len(valid_metrics) >= 2:
                _prefer_early = next((m for m in ["T+24h %", "T+1d %"] if m in impact_df.columns), None)
                _prefer_late  = "T+3d %" if "T+3d %" in impact_df.columns else None
                if _prefer_early and _prefer_late:
                    _m_early, _m_late = _prefer_early, _prefer_late
                else:
                    _m_early, _m_late = valid_metrics[0], valid_metrics[-1]
                _both    = [c for c in [_m_early, _m_late] if c in impact_df.columns]
                if len(_both) == 2:
                    chart_header(f"Early ({_m_early}) vs Late ({_m_late}) Reaction per Event", "early_late_reaction",
                                 _ins.get_data_snapshot("early_late_reaction", impact_df=impact_df))
                    _valid = impact_df.dropna(subset=_both, how="all")
                    fig_dual = go.Figure()
                    fig_dual.add_trace(go.Bar(name=_m_early, x=_valid[date_col], y=_valid[_m_early], marker_color=_BLUE))
                    fig_dual.add_trace(go.Bar(name=_m_late,  x=_valid[date_col], y=_valid[_m_late],  marker_color=_GREEN))
                    fig_dual.update_layout(
                        barmode="group", template=_TMPL, hoverlabel=_HOVER,
                        title="Price Change Around Each Event",
                        xaxis_title="Event Datetime" if date_col == "Datetime" else "Event Date",
                        yaxis_title="Price Change (%)", height=320,
                        margin=dict(l=0, r=0, t=50, b=0),
                    )
                    _apply_dark(fig_dual)
                    st.plotly_chart(fig_dual, use_container_width=True, config=_PCFG)

            # ── Full impact table ──
            chart_header("Full Event Impact Table", "full_impact_table",
                         _ins.get_data_snapshot("full_impact_table", impact_df=impact_df))
            _full_cols = ([date_col, "Event Type", "Description", "Sentiment", "Price"]
                         + valid_metrics + ["Vol Spike %"])
            _full_cols = [c for c in _full_cols if c in impact_df.columns]
            _pct_cols_full = [c for c in valid_metrics + ["Vol Spike %"] if c in impact_df.columns]
            st.dataframe(
                _pct_styler(impact_df[_full_cols].sort_values(date_col, ascending=False), _pct_cols_full),
                use_container_width=True,
                column_config={
                    "Description": st.column_config.TextColumn(width="large"),
                },
            )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — EXCHANGES
# ═══════════════════════════════════════════════════════════════════════════════
with tab_exchanges:
    st.header("Exchange Analysis")

    if exchange_df.empty:
        st.info("No exchange data yet. Click **Refresh Price & Exchange Data** in the sidebar.")
    else:
        if "geography" not in exchange_df.columns:
            exchange_df["geography"] = exchange_df.apply(
                lambda r: fetcher._classify_geo(r["exchange"], r["quote"]), axis=1
            )

        _total_vol = exchange_df["volume_usd"].sum()
        _spot_vol  = exchange_df[exchange_df["market_type"] == "spot"]["volume_usd"].sum()
        _dex_vol   = exchange_df[exchange_df["market_type"] == "dex"]["volume_usd"].sum()
        _top3_vol  = exchange_df.groupby("exchange")["volume_usd"].sum().nlargest(3).sum()
        _conc_pct  = (_top3_vol / _total_vol * 100) if _total_vol > 0 else 0.0

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total Volume", f"${_total_vol/1e6:.1f}M")
        m2.metric("Spot CEX Volume", f"${_spot_vol/1e6:.1f}M",
                  f"{_spot_vol/_total_vol*100:.0f}%" if _total_vol > 0 else "–")
        m3.metric("DEX Volume", f"${_dex_vol/1e6:.1f}M",
                  f"{_dex_vol/_total_vol*100:.0f}%" if _total_vol > 0 else "–")
        m4.metric("# Markets Listed", str(len(exchange_df)))
        m5.metric("Top 3 Concentration", f"{_conc_pct:.1f}%")
        if _conc_pct > 80:
            st.warning(f"Top 3 exchanges control {_conc_pct:.1f}% of volume — high liquidity concentration risk.")

        st.divider()
        ca, cb = st.columns(2)

        with ca:
            chart_header("Top 20 Markets by Volume", "top20_markets",
                         _ins.get_data_snapshot("top20_markets", exchange_df=exchange_df))
            top20 = exchange_df.nlargest(20, "volume_usd")
            fig_ex = px.bar(
                top20, x="volume_usd", y="exchange", orientation="h",
                color="market_type",
                title="Top 20 Markets by Volume",
                color_discrete_map={"spot": _BLUE, "dex": _GREEN},
                template=_TMPL,
                labels={"volume_usd": "Volume (USD)", "exchange": ""},
            )
            fig_ex.update_layout(yaxis={"categoryorder": "total ascending"}, height=500,
                                  margin=dict(l=0, r=0, t=50, b=0))
            _apply_dark(fig_ex)
            st.plotly_chart(fig_ex, use_container_width=True, config=_PCFG)
            st.caption("Blue = centralised spot · Green = DEX")

        with cb:
            chart_header("Volume by Geography & Exchange", "volume_geography",
                         _ins.get_data_snapshot("volume_geography", exchange_df=exchange_df))
            fig_tree = px.treemap(
                exchange_df, path=["geography", "exchange"], values="volume_usd",
                title="Volume by Geography & Exchange",
                color="volume_usd", color_continuous_scale=[_BLUE, _GREEN],
                template=_TMPL,
            )
            fig_tree.update_traces(
                textinfo="label+percent root",
                hovertemplate="<b>%{label}</b><br>Volume: $%{value:,.0f}<br>%{percentRoot:.1%} of total<extra></extra>",
            )
            fig_tree.update_layout(margin=dict(l=0, r=0, t=50, b=0), coloraxis_showscale=False)
            _apply_dark(fig_tree)
            st.plotly_chart(fig_tree, use_container_width=True, config=_PCFG)
            st.caption("Outer = geography region · Inner = exchange · Size = USD volume")

        chart_header("Spot vs DEX Volume Split", "spot_dex_split",
                     _ins.get_data_snapshot("spot_dex_split", exchange_df=exchange_df))
        _type_vol = exchange_df.groupby("market_type")["volume_usd"].sum().reset_index()
        fig_type = px.pie(_type_vol, values="volume_usd", names="market_type",
                          title="Spot vs DEX Volume Split",
                          color="market_type",
                          color_discrete_map={"spot": _BLUE, "dex": _GREEN},
                          template=_TMPL, hole=0.4)
        fig_type.update_layout(margin=dict(l=0, r=0, t=50, b=0))
        _apply_dark(fig_type)
        st.plotly_chart(fig_type, use_container_width=True, config=_PCFG)

        st.divider()

        # Price consistency / arbitrage
        chart_header("Price Consistency", "price_consistency",
                     _ins.get_data_snapshot("price_consistency", exchange_df=exchange_df))
        if "price_usd" in exchange_df.columns:
            _prices = exchange_df["price_usd"].dropna()
            if not _prices.empty:
                _med = _prices.median()
                _arb_mask = ((exchange_df["price_usd"] - _med).abs() / _med) > 0.005
                _arb_rows = exchange_df[_arb_mask].copy()
                if not _arb_rows.empty:
                    _arb_rows["Deviation %"] = ((_arb_rows["price_usd"] - _med) / _med * 100).round(3)
                    _arb_rows["quote"] = _arb_rows["quote"].replace(_QUOTE_MAP)
                    _arb_rows["Note"] = "Arbitrage opportunity"
                    st.warning(f"{len(_arb_rows)} exchange(s) deviate >0.5% from median price (${_med:.5f}).")
                    _arb_show = (_arb_rows[["exchange", "base", "quote", "price_usd", "Deviation %", "Note"]]
                                 .sort_values("Deviation %", key=abs, ascending=False))
                    st.dataframe(
                        _pct_styler(_arb_show, ["Deviation %"]),
                        use_container_width=True,
                        column_config={
                            "price_usd": st.column_config.NumberColumn("Price (USD)", format="$%.5f"),
                        },
                    )
                else:
                    st.success(f"All exchanges within 0.5% of median price (${_med:.5f}). No significant arbitrage detected.")

        st.divider()
        chart_header("Volume by Quote Currency", "volume_quote_currency",
                     _ins.get_data_snapshot("volume_quote_currency", exchange_df=exchange_df))
        # Clean DEX contract addresses → readable ticker names
        _ex_clean = exchange_df.copy()
        _ex_clean["quote"] = _ex_clean["quote"].replace(_QUOTE_MAP)
        _quote_vol = (_ex_clean.groupby("quote")["volume_usd"].sum()
                      .sort_values(ascending=False).head(10).reset_index())
        fig_quote = px.bar(_quote_vol, x="quote", y="volume_usd",
                           title="Volume by Quote Currency",
                           color_discrete_sequence=[_BLUE], template=_TMPL,
                           text="volume_usd",
                           labels={"volume_usd": "Volume (USD)", "quote": "Quote Currency"})
        fig_quote.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
        fig_quote.update_layout(margin=dict(l=0, r=0, t=50, b=30))
        _apply_dark(fig_quote)
        st.plotly_chart(fig_quote, use_container_width=True, config=_PCFG)
        st.caption("USDT dominance = institutional depth · KRW/TRY spikes = retail momentum (Korea/Turkey)")

        chart_header("All Exchange Data", "all_exchange_data",
                     _ins.get_data_snapshot("all_exchange_data", exchange_df=exchange_df))
        st.dataframe(
            _pct_styler(_ex_clean[["exchange", "base", "quote", "volume_usd", "price_usd",
                        "market_type", "geography"]].sort_values("volume_usd", ascending=False), []),
            use_container_width=True,
            column_config={
                "volume_usd": st.column_config.NumberColumn("Volume (USD)", format="$%.0f"),
                "price_usd":  st.column_config.NumberColumn("Price (USD)",  format="$%.4f"),
            },
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — ANALYTICS (new)
# ═══════════════════════════════════════════════════════════════════════════════
with tab_analytics:
    st.header("Analytics")

    _has_price = not price_df.empty and len(price_df) >= 2
    if _has_price:
        _price_sorted = price_df.sort_values("date").copy()
        daily_returns = _price_sorted["price_usd"].pct_change() * 100
    else:
        daily_returns = pd.Series(dtype=float)
        _price_sorted = pd.DataFrame()

    try:
        if not hourly_df.empty and not events_df.empty:
            _tab_impact = compute_impact_rows_hourly(hourly_df, events_df, price_df)
            _t1_col = "T+24h %"
        elif not price_df.empty and not events_df.empty:
            _tab_impact = compute_impact_rows(price_df, events_df)
            _t1_col = "T+1d %"
        else:
            _tab_impact = pd.DataFrame()
            _t1_col = None
    except Exception:
        _tab_impact = pd.DataFrame()
        _t1_col = None

    # ── Section 1: Price Trend Analysis ──
    chart_header("Price Trend Analysis", "price_trend_analysis",
                 _ins.get_data_snapshot("price_trend_analysis", price_df=price_df))
    if not _has_price:
        st.info("No price data available. Click a Refresh button in the sidebar.")
    else:
        col_a, col_b = st.columns(2)

        with col_a:
            roll30 = _price_sorted["price_usd"].pct_change(30) * 100
            roll30_df = pd.DataFrame({"date": _price_sorted["date"].values,
                                      "return_30d": roll30.values}).dropna()
            fig_r30 = go.Figure()
            fig_r30.add_trace(go.Scatter(
                x=roll30_df["date"], y=roll30_df["return_30d"],
                mode="lines", name="30d Rolling Return",
                line=dict(color=_BLUE, width=2),
                fill="tozeroy", fillcolor="rgba(26,115,232,0.12)",
            ))
            fig_r30.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
            fig_r30.update_layout(title="Rolling 30-Day Return (%)", xaxis_title="Date",
                                   yaxis_title="Return (%)", template=_TMPL, hoverlabel=_HOVER, height=300,
                                   margin=dict(l=0, r=0, t=40, b=0))
            _apply_dark(fig_r30)
            st.plotly_chart(fig_r30, use_container_width=True, config=_PCFG)

        with col_b:
            vol7 = daily_returns.rolling(7).std()
            vol7_df = pd.DataFrame({"date": _price_sorted["date"].values,
                                    "volatility_7d": vol7.values}).dropna()
            fig_v7 = go.Figure()
            fig_v7.add_trace(go.Scatter(
                x=vol7_df["date"], y=vol7_df["volatility_7d"],
                mode="lines", name="7d Volatility",
                line=dict(color=_RED, width=2),
            ))
            fig_v7.update_layout(title="7-Day Rolling Volatility", xaxis_title="Date",
                                  yaxis_title="Std Dev of Daily Return (%)", template=_TMPL, hoverlabel=_HOVER,
                                  height=300, margin=dict(l=0, r=0, t=40, b=0))
            _apply_dark(fig_v7)
            st.plotly_chart(fig_v7, use_container_width=True, config=_PCFG)
            st.caption("Higher volatility = larger price swings expected")

    st.divider()

    # ── Section 2: Event Signal Intelligence ──
    chart_header("Event Signal Intelligence", "event_signal_intelligence",
                 _ins.get_data_snapshot("event_signal_intelligence", price_df=price_df, events_df=events_df, impact_df=_tab_impact))
    col_c, col_d = st.columns(2)

    with col_c:
        if _tab_impact.empty or _t1_col is None or _t1_col not in _tab_impact.columns:
            st.info("No event impact data. Refresh price data and ensure events exist.")
        elif daily_returns.empty:
            st.info("Need daily price data to compute baseline-adjusted returns.")
        else:
            mean_daily_return = round(float(daily_returns.dropna().mean()), 4)
            _excess_df = _tab_impact.dropna(subset=[_t1_col]).copy()
            _excess_df["Excess Return"] = _excess_df[_t1_col] - mean_daily_return
            _excess_by_type = (_excess_df.groupby("Event Type")["Excess Return"]
                               .mean().reset_index().sort_values("Excess Return", ascending=False))
            _bar_clrs = [_GREEN if v >= 0 else _RED for v in _excess_by_type["Excess Return"]]
            fig_exc = go.Figure(go.Bar(
                x=_excess_by_type["Event Type"], y=_excess_by_type["Excess Return"],
                marker_color=_bar_clrs,
                text=_excess_by_type["Excess Return"].round(2).astype(str) + "%",
                textposition="outside",
            ))
            fig_exc.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
            fig_exc.update_layout(
                title=f"Baseline-Adjusted {_t1_col} Return by Event Type",
                xaxis_title="Event Type", yaxis_title="Excess Return (%)",
                template=_TMPL, hoverlabel=_HOVER, height=340, margin=dict(l=0, r=0, t=40, b=0),
            )
            _apply_dark(fig_exc)
            st.plotly_chart(fig_exc, use_container_width=True, config=_PCFG)
            st.caption(f"Returns adjusted for daily market drift of {mean_daily_return:+.2f}%. "
                       "Positive = event outperformed baseline.")

    with col_d:
        if events_df.empty:
            st.info("No events to cluster.")
        elif not _has_price:
            st.info("Need price data for weekly overlay.")
        else:
            try:
                _ev_c = events_df.copy()
                _ev_c["date"] = pd.to_datetime(_ev_c["date"], errors="coerce")
                _ev_c = _ev_c.dropna(subset=["date"])
                _ev_weekly = _ev_c.set_index("date").resample("W").size().reset_index()
                _ev_weekly.columns = ["week", "event_count"]

                _pr_w = _price_sorted.copy()
                _pr_w["date"] = pd.to_datetime(_pr_w["date"], errors="coerce")
                _pr_w = _pr_w.dropna(subset=["date"]).set_index("date")
                _pr_w_ret = (_pr_w["price_usd"].resample("W").last().pct_change() * 100).reset_index()
                _pr_w_ret.columns = ["week", "weekly_return"]
                _merged_w = pd.merge(_ev_weekly, _pr_w_ret, on="week", how="outer").sort_values("week")

                fig_cl = go.Figure()
                fig_cl.add_trace(go.Bar(x=_merged_w["week"], y=_merged_w["event_count"],
                                         name="Events/Week", marker_color=_BLUE, opacity=0.7, yaxis="y1"))
                fig_cl.add_trace(go.Scatter(x=_merged_w["week"], y=_merged_w["weekly_return"],
                                             name="Weekly Return %", line=dict(color=_RED, width=2),
                                             mode="lines+markers", yaxis="y2"))
                fig_cl.update_layout(
                    title="Event Frequency vs Weekly Price Return",
                    xaxis_title="Week",
                    yaxis=dict(title="Events per Week", side="left"),
                    yaxis2=dict(title="Weekly Return (%)", overlaying="y", side="right",
                                showgrid=False, zeroline=False),
                    legend=dict(orientation="h", y=-0.2),
                    template=_TMPL, hoverlabel=_HOVER, height=340, margin=dict(l=0, r=0, t=40, b=40),
                )
                _apply_dark(fig_cl)
                st.plotly_chart(fig_cl, use_container_width=True, config=_PCFG)
            except Exception as _e:
                st.info(f"Could not render event clustering chart: {_e}")

    st.divider()

    # ── Section 3: Volume Intelligence ──
    chart_header("Volume Intelligence", "volume_intelligence",
                 _ins.get_data_snapshot("volume_intelligence", price_df=price_df))
    if not _has_price or "volume_24h" not in _price_sorted.columns:
        st.info("No volume data available. Refresh price data.")
    else:
        try:
            _vd = _price_sorted[["date", "volume_24h"]].copy().dropna(subset=["volume_24h"])
            _vd["vol_30d_avg"] = _vd["volume_24h"].rolling(30, min_periods=1).mean()
            _vd["is_anomaly"]  = _vd["volume_24h"] > (1.8 * _vd["vol_30d_avg"])
            _anom_count = int(_vd["is_anomaly"].sum())
            _normal = _vd[~_vd["is_anomaly"]]
            _anomaly = _vd[_vd["is_anomaly"]]

            fig_va = go.Figure()
            fig_va.add_trace(go.Bar(x=_normal["date"],  y=_normal["volume_24h"],
                                    name="Normal Volume", marker_color=_BLUE, opacity=0.75))
            fig_va.add_trace(go.Bar(x=_anomaly["date"], y=_anomaly["volume_24h"],
                                    name="Volume Anomaly (>1.8× 30d avg)",
                                    marker_color=_RED, opacity=0.9))
            fig_va.update_layout(title="Daily Volume with Anomaly Detection",
                                  barmode="overlay", template=_TMPL, hoverlabel=_HOVER, height=300,
                                  margin=dict(l=0, r=0, t=40, b=0))

            vc, vm = st.columns([4, 1])
            with vc:
                _apply_dark(fig_va)
                st.plotly_chart(fig_va, use_container_width=True, config=_PCFG)
            with vm:
                st.metric("Anomaly Days", _anom_count,
                          help="Days where volume exceeded 1.8× the 30-day rolling average")
        except Exception as _e:
            st.info(f"Volume anomaly chart unavailable: {_e}")

    st.divider()

    # ── Section 4: Data Quality Dashboard ──
    chart_header("Data Quality Dashboard", "data_quality",
                 _ins.get_data_snapshot("data_quality", price_df=price_df, hourly_df=hourly_df, events_df=events_df))
    _total_ev = len(events_df)
    _valid_dt = int(events_df["datetime_str"].notna().sum()) if not events_df.empty and "datetime_str" in events_df.columns else 0
    _price_days  = len(price_df)
    _hourly_rows = len(hourly_df)

    dq1, dq2, dq3, dq4 = st.columns(4)
    dq1.metric("Total Events", _total_ev)
    dq2.metric("Events with Valid Datetime", _valid_dt,
               delta=f"{_valid_dt - (_total_ev - _valid_dt)} vs missing")
    dq3.metric("Daily Price Rows", _price_days)
    dq4.metric("Hourly Price Rows", _hourly_rows)

    def _freshness_status(ts_str):
        if ts_str == "Never": return "Never refreshed"
        try:
            ts = datetime.fromisoformat(ts_str)
            if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)
            age_h = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            if age_h < 2:   return "✅ Fresh (< 2h)"
            if age_h < 24:  return f"🟡 OK ({age_h:.0f}h ago)"
            if age_h < 72:  return f"🟠 Stale ({age_h/24:.0f}d ago)"
            return f"🔴 Old ({age_h/24:.0f}d ago)"
        except Exception:
            return "Unknown"

    _dq_df = pd.DataFrame([
        {"Source": "Daily Price History", "Last Refresh": db.get_last_refresh("price_history"),
         "Status": _freshness_status(db.get_last_refresh("price_history"))},
        {"Source": "Hourly Prices",       "Last Refresh": db.get_last_refresh("price_hourly"),
         "Status": _freshness_status(db.get_last_refresh("price_hourly"))},
        {"Source": "Twitter / Posts",     "Last Refresh": db.get_last_refresh("tweets"),
         "Status": _freshness_status(db.get_last_refresh("tweets"))},
    ])
    st.dataframe(_pct_styler(_dq_df, []), use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6 — RISK & SIGNALS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_risk:
    st.header("Risk Signals & Predictive Insights")

    try:
        if not hourly_df.empty and not events_df.empty:
            _risk_impact = compute_impact_rows_hourly(hourly_df, events_df, price_df)
            _risk_t1_col = "T+24h %"
        elif not price_df.empty and not events_df.empty:
            _risk_impact = compute_impact_rows(price_df, events_df)
            _risk_t1_col = "T+1d %"
        else:
            _risk_impact = pd.DataFrame()
            _risk_t1_col = None
    except Exception:
        _risk_impact = pd.DataFrame()
        _risk_t1_col = None

    cl, cr = st.columns(2)

    with cl:
        st.subheader("⚠️ Active Risk Signals")
        signals = []
        if current:
            recent_events_2d = events_df[
                events_df["date"] >= (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
            ] if not events_df.empty else pd.DataFrame()
            if abs(current["pct_24h"]) > 5 and recent_events_2d.empty:
                signals.append(("High", f"Price moved {current['pct_24h']:+.1f}% in 24h with no logged events — possible untracked news"))
            if current["cex_volume_24h"] > 0:
                dex_ratio = current["dex_volume_24h"] / current["cex_volume_24h"]
                if dex_ratio > 0.2:
                    signals.append(("Medium", f"DEX is {dex_ratio*100:.0f}% of CEX volume — elevated on-chain activity"))
            if current["pct_7d"] < -10:
                signals.append(("High", f"7d price down {current['pct_7d']:.1f}% — sustained downtrend"))
            if current["pct_24h"] > 15:
                signals.append(("Medium", f"Price up {current['pct_24h']:+.1f}% in 24h — watch for reversal"))
        if not signals:
            signals.append(("Low", "No major risk signals detected currently"))
        for severity, msg in signals:
            if severity == "High":     st.error(f"**High**: {msg}")
            elif severity == "Medium": st.warning(f"**Medium**: {msg}")
            else:                      st.success(f"**Low**: {msg}")

        # Sentiment heatmap
        if not events_df.empty and "event_type" in events_df.columns and "sentiment_label" in events_df.columns:
            st.divider()
            chart_header("Sentiment Heatmap by Event Type", "sentiment_heatmap",
                         _ins.get_data_snapshot("sentiment_heatmap", events_df=events_df))
            try:
                _hm = events_df.groupby(["event_type", "sentiment_label"]).size().unstack(fill_value=0)
                for _sc in ["positive", "neutral", "negative"]:
                    if _sc not in _hm.columns: _hm[_sc] = 0
                _hm = _hm[["positive", "neutral", "negative"]]
                fig_hm = px.imshow(_hm, color_continuous_scale="RdYlGn",
                                   labels=dict(x="Sentiment", y="Event Type", color="Count"),
                                   title="Event Type × Sentiment Count",
                                   template=_TMPL, aspect="auto")
                fig_hm.update_layout(height=360, margin=dict(l=0, r=0, t=40, b=0))
                _apply_dark(fig_hm)
                st.plotly_chart(fig_hm, use_container_width=True, config=_PCFG)
            except Exception as _e:
                st.info(f"Could not render heatmap: {_e}")

    with cr:
        st.subheader("🔮 Predictive Signals")

        if current:
            momentum = "bullish" if current["pct_1h"] > 0 and current["pct_24h"] > 0 else \
                       "bearish" if current["pct_1h"] < 0 and current["pct_24h"] < 0 else "mixed"
            ms1, ms2 = st.columns(2)
            ms1.metric("Price (USD)", f"${current['price']:.4f}", f"{current['pct_24h']:+.2f}% 24h")
            ms2.metric("1h Momentum", f"{current['pct_1h']:+.2f}%", momentum.title())
            ms3, ms4 = st.columns(2)
            ms3.metric("24h Trend", f"{current['pct_24h']:+.2f}%", "Up" if current["pct_24h"] > 0 else "Down")
            ms4.metric("CMC Rank", f"#{current['cmc_rank']}")

        st.divider()
        st.markdown("""
**Leading Indicators to Watch:**
- 📊 Volume spike without news → possible unannounced event incoming
- 🐦 Increase in tweet frequency from @GoKiteAI → announcement likely
- 🌏 KRW/TRY exchange volume spike → retail-driven momentum (Korea/Turkey)
- 📉 DEX volume surge → on-chain activity, possible whale movement
""")

        st.divider()

        # Rolling signal quality
        chart_header("Rolling Signal Quality (last 30 days)", "rolling_signal_quality",
                     _ins.get_data_snapshot("rolling_signal_quality", impact_df=_risk_impact))
        _sq_rendered = False
        if (not _risk_impact.empty and _risk_t1_col is not None
                and _risk_t1_col in _risk_impact.columns and not events_df.empty):
            try:
                _sq = _risk_impact.dropna(subset=[_risk_t1_col]).copy()
                _sq_dcol = "Datetime" if "Datetime" in _sq.columns else "Date"
                _sq["_sq_date"] = pd.to_datetime(_sq[_sq_dcol], errors="coerce").dt.normalize()
                _sq = _sq.dropna(subset=["_sq_date"])
                if not _sq.empty:
                    _today_sq = pd.Timestamp.utcnow().normalize()
                    _sq_rows = []
                    for _off in range(29, -1, -1):
                        _day = _today_sq - pd.Timedelta(days=_off)
                        _win = _sq[(_sq["_sq_date"] >= _day - pd.Timedelta(days=7)) &
                                   (_sq["_sq_date"] < _day)]
                        if len(_win) >= 1:
                            _sq_rows.append({"day": _day,
                                             "signal_quality_pct": round((_win[_risk_t1_col] > 0).mean() * 100, 1),
                                             "n_events": len(_win)})
                    if _sq_rows:
                        _sq_df = pd.DataFrame(_sq_rows)
                        fig_sq = go.Figure()
                        fig_sq.add_trace(go.Scatter(
                            x=_sq_df["day"], y=_sq_df["signal_quality_pct"],
                            mode="lines+markers", line=dict(color=_BLUE, width=2),
                            fill="tozeroy", fillcolor="rgba(26,115,232,0.10)",
                            customdata=_sq_df["n_events"],
                            hovertemplate="%{x|%Y-%m-%d}<br>Signal Quality: %{y:.1f}%<br>Events: %{customdata}<extra></extra>",
                        ))
                        fig_sq.add_hline(y=50, line_dash="dash", line_color="gray", opacity=0.5,
                                         annotation_text="50% baseline")
                        fig_sq.update_layout(
                            title="% of Prior-7-Day Events with Positive Next-Day Return",
                            yaxis=dict(range=[0, 105]), template=_TMPL, hoverlabel=_HOVER, height=260,
                            margin=dict(l=0, r=0, t=40, b=0),
                        )
                        _apply_dark(fig_sq)
                        st.plotly_chart(fig_sq, use_container_width=True, config=_PCFG)
                        _sq_rendered = True
            except Exception:
                pass
        if not _sq_rendered:
            st.info("Not enough event impact data to compute rolling signal quality.")

        st.divider()

        # Pattern Library
        chart_header("Pattern Library", "pattern_library",
                     _ins.get_data_snapshot("pattern_library", price_df=price_df, events_df=events_df))
        try:
            _pat_i = compute_impact_rows(price_df, events_df) if not (price_df.empty or events_df.empty) else pd.DataFrame()
            if not _pat_i.empty and "T+1d %" in _pat_i.columns:
                _pat = _pat_i.groupby("Event Type").agg(
                    Count=("ID", "count"),
                    Avg_T1d=("T+1d %", "mean"),
                    Med_T1d=("T+1d %", "median"),
                    Hit_Rate=("T+1d %", lambda x: round((x > 0).mean() * 100, 1)),
                ).round(2).sort_values("Hit_Rate", ascending=True).reset_index()
                _pat.columns = ["Event Type", "# Events", "Avg T+1d %", "Median T+1d %", "% Days Price Rose"]
                _pcols = [_GREEN if v >= 50 else _RED for v in _pat["% Days Price Rose"]]
                fig_pat = go.Figure(go.Bar(
                    x=_pat["% Days Price Rose"], y=_pat["Event Type"], orientation="h",
                    marker_color=_pcols,
                    text=_pat["% Days Price Rose"].astype(str) + "%", textposition="outside",
                    customdata=_pat["# Events"],
                    hovertemplate="<b>%{y}</b><br>% Days Price Rose: %{x}%<br># Events: %{customdata}<extra></extra>",
                ))
                fig_pat.add_vline(x=50, line_dash="dash", line_color="gray", opacity=0.5,
                                   annotation_text="50% baseline")
                fig_pat.update_layout(
                    title="% of Events Where Price Rose the Next Day",
                    xaxis=dict(range=[0, 115]), template=_TMPL, hoverlabel=_HOVER,
                    height=max(260, 40 * len(_pat) + 80),
                    margin=dict(l=0, r=60, t=40, b=0),
                )
                _apply_dark(fig_pat)
                st.plotly_chart(fig_pat, use_container_width=True, config=_PCFG)
                st.dataframe(_pct_styler(_pat.sort_values("Avg T+1d %", ascending=False), ["Avg T+1d %"]),
                             use_container_width=True, hide_index=True)
                st.caption("⚠️ T+1d returns include overall market trend — does not prove causation.")
            else:
                st.info("Refresh Price & Exchange Data to populate this table.")
        except Exception:
            st.info("Refresh price data to see computed patterns.")

        if not events_df.empty:
            st.divider()
            chart_header("Latest 7 Events", "latest_events",
                         _ins.get_data_snapshot("latest_events", events_df=events_df))
            _rc = [c for c in ["date", "event_type", "description", "sentiment_label", "source"] if c in events_df.columns]
            st.dataframe(_pct_styler(events_df.head(7)[_rc], []), use_container_width=True, hide_index=True)


# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    f"🪁 KITE Dashboard · Data: CoinGecko + CoinMarketCap · Events: @GoKiteAI · "
    f"Updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC"
)
