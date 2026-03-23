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

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="KITE Token Dashboard",
    page_icon="🪁",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
[data-testid="metric-container"] { background: #1a1a2e; border-radius: 8px; padding: 12px; }
.stTabs [data-baseweb="tab-list"] { gap: 8px; }
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

    if st.button("⏱️ Refresh Hourly Prices (last 30d)", use_container_width=True, type="primary"):
        with st.spinner("Fetching hourly prices from CoinGecko…"):
            h_df = fetcher.get_historical_prices_hourly(days=30)
            if not h_df.empty:
                n = db.upsert_hourly_prices(h_df)
                db.log_refresh("price_hourly", n)
                st.toast(f"✅ {n} hourly price records saved")
            else:
                st.toast("⚠️ Hourly price fetch failed", icon="⚠️")
        st.rerun()

    if st.button("🔄 Refresh Price & Exchange Data", use_container_width=True):
        with st.spinner("Fetching price history from CoinGecko…"):
            df = fetcher.get_historical_prices(days=140)
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

    max_posts = st.select_slider(
        "Posts to fetch", options=[50, 100, 200, 300, 500], value=100
    )
    if st.button("🌐 Fetch Posts via Browser (Playwright)", use_container_width=True, type="primary"):
        with st.spinner(f"Scraping up to {max_posts} posts from @GoKiteAI…"):
            tweets, err = scraper.scrape_tweets_browser("GoKiteAI", max_tweets=max_posts)
        if err:
            st.error(f"Browser scrape error: {err}")
        elif tweets:
            existing_ids = db.get_existing_tweet_ids()
            new_count = sum(
                1 for t in tweets
                if t["tweet_id"] not in existing_ids and db.add_event(t)
            )
            db.log_refresh("tweets", new_count)
            st.toast(f"✅ {new_count} new posts added as events")
            st.rerun()
        else:
            st.warning("No posts returned — make sure Playwright is installed and cookies are set.")

    if st.button("🐦 Fetch Tweets (twikit fallback)", use_container_width=True):
        with st.spinner("Scraping tweets via twikit…"):
            tweets, err = scraper.scrape_tweets("GoKiteAI", max_tweets=100)
        if err:
            st.error(f"Scrape error: {err}")
        elif tweets:
            existing_ids = db.get_existing_tweet_ids()
            new_count = sum(
                1 for t in tweets
                if t["tweet_id"] not in existing_ids and db.add_event(t)
            )
            db.log_refresh("tweets", new_count)
            st.toast(f"✅ {new_count} new tweets added as events")
            st.rerun()
        else:
            st.warning("No tweets returned — check cookie values are correct.")

    st.divider()

    # Last refresh timestamps
    st.caption(f"Prices last fetched: {db.get_last_refresh('price_history')[:16]}")
    st.caption(f"Tweets last fetched: {db.get_last_refresh('tweets')[:16]}")
    st.divider()

    # X Cookie Import (recommended)
    with st.expander("🍪 X Cookie Import (recommended)", expanded=True):
        st.caption(
            "Get these from Chrome: open x.com → F12 → Application → "
            "Cookies → https://x.com — copy **auth_token** and **ct0** values."
        )
        auth_token = st.text_input("auth_token", type="password")
        ct0 = st.text_input("ct0", type="password")
        if st.button("Save Cookies", use_container_width=True, type="primary"):
            if auth_token and ct0:
                import json
                cookies = {"auth_token": auth_token, "ct0": ct0}
                scraper.COOKIES_PATH.write_text(json.dumps(cookies))
                st.success("Cookies saved! Click 'Fetch Tweets' now.")
            else:
                st.error("Both auth_token and ct0 are required.")

    # X Login (fallback)
    with st.expander("🔑 X Password Login (fallback)"):
        st.caption("May be blocked by X. Use cookie import above if this fails.")
        x_user = st.text_input("X Username")
        x_email = st.text_input("Email")
        x_pass = st.text_input("Password", type="password")
        if st.button("Login to X", use_container_width=True):
            try:
                scraper.login(x_user, x_email, x_pass)
                st.success("Logged in — cookies saved locally.")
            except Exception as e:
                st.error(f"Login failed: {e}")

# ── Load data ─────────────────────────────────────────────────────────────────
start_str = start_date.strftime("%Y-%m-%d")
end_str = end_date.strftime("%Y-%m-%d")

price_df = db.get_prices(start_str, end_str)
hourly_df = db.get_hourly_prices()
events_df = db.get_events(start_str, end_str)
exchange_df = db.get_latest_exchange_snapshots()
current = fetcher.get_current_snapshot()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_overview, tab_events, tab_corr, tab_exchanges, tab_risk = st.tabs([
    "📊 Overview", "📅 Events", "🔗 Correlation", "🏦 Exchanges", "⚠️ Risk & Signals"
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

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=chart_df[x_col], y=chart_df["price_usd"],
            mode="lines", name="Price (USD)",
            line=dict(color="#7c3aed", width=1.5 if use_hourly else 2),
            fill="tozeroy", fillcolor="rgba(124,58,237,0.08)",
        ))

        # Overlay event markers
        sentiment_colors = {"positive": "#00c853", "negative": "#ff1744", "neutral": "#f59e0b"}
        if not events_df.empty:
            if use_hourly:
                h_indexed = hourly_df.set_index("datetime")["price_usd"]
                for _, ev in events_df.iterrows():
                    dt_str = (ev.get("datetime_str") or ev["date"] + " 00:00:00")
                    # round to hour
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
                    color = sentiment_colors.get(ev.get("sentiment_label"), "#f59e0b")
                    ev_type = ev.get("event_type", "")
                    type_desc = EVENT_DESCRIPTIONS.get(ev_type, "")
                    fig.add_trace(go.Scatter(
                        x=[ev_hour], y=[h_indexed[ev_hour]],
                        mode="markers",
                        marker=dict(size=14, color=color, symbol="star",
                                    line=dict(color="white", width=1)),
                        name=ev_type,
                        text=[f"{ev.get('description','')[:80]}<br><i>{type_desc}</i>"],
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
                    color = sentiment_colors.get(ev.get("sentiment_label"), "#f59e0b")
                    ev_type = ev.get("event_type", "")
                    type_desc = EVENT_DESCRIPTIONS.get(ev_type, "")
                    fig.add_trace(go.Scatter(
                        x=[ev["date"]], y=[price_indexed[ev["date"]]],
                        mode="markers",
                        marker=dict(size=14, color=color, symbol="star",
                                    line=dict(color="white", width=1)),
                        name=ev_type,
                        text=[f"{ev.get('description','')[:80]}<br><i>{type_desc}</i>"],
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
            hovermode="x unified", template="plotly_dark",
            height=460, margin=dict(l=0, r=0, t=40, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Volume chart ──
        fig_vol = go.Figure()
        fig_vol.add_trace(go.Bar(
            x=chart_df[x_col], y=chart_df["volume_24h"],
            name="Volume (USD)", marker_color="#7c3aed", opacity=0.8,
        ))
        fig_vol.update_layout(
            title="Hourly Trading Volume" if use_hourly else "Daily Trading Volume",
            xaxis_title="Datetime" if use_hourly else "Date",
            yaxis_title="Volume (USD)",
            template="plotly_dark", height=220,
            margin=dict(l=0, r=0, t=40, b=0),
        )
        st.plotly_chart(fig_vol, use_container_width=True)

        # ── Trend summary ──
        if current:
            st.subheader("Trend Overview")
            tc1, tc2, tc3 = st.columns(3)
            tc1.metric("7 Day", f"{current['pct_7d']:+.2f}%",
                       "↑ Uptrend" if current['pct_7d'] > 0 else "↓ Downtrend")
            tc2.metric("30 Day", f"{current['pct_30d']:+.2f}%",
                       "↑ Uptrend" if current['pct_30d'] > 0 else "↓ Downtrend")
            if current.get('pct_60d') is not None:
                tc3.metric("60 Day", f"{current['pct_60d']:+.2f}%",
                           "↑ Uptrend" if current['pct_60d'] > 0 else "↓ Downtrend")


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
                "Airdrop", "Milestone", "Community", "Regulation", "Announcement",
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
                display_df[show_cols],
                use_container_width=True, height=420,
                column_config={
                    "description": st.column_config.TextColumn("Description", width="large"),
                    "sentiment_score": st.column_config.NumberColumn("Sentiment Score", format="%.2f"),
                },
            )

    # Charts row
    if not events_df.empty:
        st.divider()
        ca, cb = st.columns(2)
        with ca:
            type_counts = events_df["event_type"].value_counts().reset_index()
            type_counts.columns = ["Event Type", "Count"]
            fig_pie = px.pie(type_counts, values="Count", names="Event Type",
                             title="Event Type Distribution", template="plotly_dark", hole=0.3)
            st.plotly_chart(fig_pie, use_container_width=True)
        with cb:
            sent_counts = events_df["sentiment_label"].value_counts().reset_index()
            sent_counts.columns = ["Sentiment", "Count"]
            fig_bar = px.bar(
                sent_counts, x="Sentiment", y="Count",
                title="Sentiment Distribution",
                color="Sentiment",
                color_discrete_map={"positive": "#00c853", "negative": "#ff1744", "neutral": "#f59e0b"},
                template="plotly_dark",
            )
            st.plotly_chart(fig_bar, use_container_width=True)


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
        # Choose hourly if available, fall back to daily
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

        if impact_df.empty:
            st.warning("No events overlap with the price data range. Try refreshing hourly prices.")
        else:
            # ── Metric filter ──
            metric = st.radio(
                "Time window to analyse",
                [m for m in available_metrics if m in impact_df.columns],
                horizontal=True,
            )

            st.divider()

            # ── Summary table ──
            st.subheader("Average Price Change by Event Type")
            sum_cols = [c for c in available_metrics if c in impact_df.columns] + ["Vol Spike %"]
            summary = impact_df.groupby("Event Type")[sum_cols].mean().round(2)
            summary["# Events"] = impact_df.groupby("Event Type").size()
            # Add event type descriptions as a column
            summary["What it means"] = summary.index.map(lambda t: EVENT_DESCRIPTIONS.get(t, ""))
            st.dataframe(
                summary.sort_values(metric, ascending=False),
                use_container_width=True,
                column_config={"What it means": st.column_config.TextColumn(width="large")},
            )

            st.divider()

            # ── Charts ──
            ca, cb = st.columns(2)
            with ca:
                scatter_df = impact_df.dropna(subset=[metric]).copy()
                scatter_df["Vol Spike %"] = scatter_df["Vol Spike %"].clip(lower=0)
                # Add type description for hover
                scatter_df["Type Info"] = scatter_df["Event Type"].map(
                    lambda t: EVENT_DESCRIPTIONS.get(t, "")
                )
                fig_scatter = px.scatter(
                    scatter_df,
                    x="Event Type", y=metric,
                    color="Sentiment", size="Vol Spike %",
                    title=f"Price Impact ({metric}) by Event Type",
                    color_discrete_map={"positive": "#00c853", "negative": "#ff1744", "neutral": "#f59e0b"},
                    template="plotly_dark",
                    hover_data={
                        date_col: True,
                        "Description": True,
                        "Type Info": True,
                        "Vol Spike %": True,
                    },
                )
                fig_scatter.add_hline(y=0, line_dash="dash", line_color="gray")
                st.plotly_chart(fig_scatter, use_container_width=True)

            with cb:
                avg_by_type = impact_df.groupby("Event Type")[metric].mean().dropna().reset_index()
                avg_by_type["Type Info"] = avg_by_type["Event Type"].map(
                    lambda t: EVENT_DESCRIPTIONS.get(t, "")
                )
                fig_bar = px.bar(
                    avg_by_type, x="Event Type", y=metric,
                    title=f"Average {metric} by Event Type",
                    color=metric,
                    color_continuous_scale=["#ff1744", "#f59e0b", "#00c853"],
                    template="plotly_dark",
                    hover_data={"Type Info": True},
                )
                fig_bar.add_hline(y=0, line_dash="dash", line_color="gray")
                st.plotly_chart(fig_bar, use_container_width=True)

            # ── Early vs late reaction dual bar ──
            if len(available_metrics) >= 2:
                m_early = available_metrics[0]   # T+1h or T+1d
                m_late  = available_metrics[-2]  # T+24h or T+3d
                both_avail = [c for c in [m_early, m_late] if c in impact_df.columns]
                if len(both_avail) == 2:
                    st.subheader(f"Early ({m_early}) vs Late ({m_late}) Reaction per Event")
                    valid = impact_df.dropna(subset=both_avail)
                    fig_dual = go.Figure()
                    fig_dual.add_trace(go.Bar(name=m_early, x=valid[date_col], y=valid[m_early],
                                              marker_color="#7c3aed"))
                    fig_dual.add_trace(go.Bar(name=m_late,  x=valid[date_col], y=valid[m_late],
                                              marker_color="#06b6d4"))
                    fig_dual.update_layout(
                        barmode="group", template="plotly_dark",
                        title="Price Change Around Each Event",
                        xaxis_title="Event Datetime" if date_col == "Datetime" else "Event Date",
                        yaxis_title="Price Change (%)", height=320,
                    )
                    st.plotly_chart(fig_dual, use_container_width=True)

            # ── Full impact table ──
            st.subheader("Full Event Impact Table")
            show_cols = [date_col, "Event Type", "Description", "Sentiment",
                         "Price"] + [m for m in available_metrics if m in impact_df.columns] + ["Vol Spike %"]
            st.dataframe(
                impact_df[show_cols].sort_values(date_col, ascending=False),
                use_container_width=True,
                column_config={
                    "Description": st.column_config.TextColumn(width="large"),
                    **{m: st.column_config.NumberColumn(format="%.2f%%")
                       for m in available_metrics if m in impact_df.columns},
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
        # Ensure geography column
        if "geography" not in exchange_df.columns:
            exchange_df["geography"] = exchange_df.apply(
                lambda r: fetcher._classify_geo(r["exchange"], r["quote"]), axis=1
            )

        total_vol = exchange_df["volume_usd"].sum()
        spot_vol = exchange_df[exchange_df["market_type"] == "spot"]["volume_usd"].sum()
        dex_vol = exchange_df[exchange_df["market_type"] == "dex"]["volume_usd"].sum()

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Volume (all markets)", f"${total_vol/1e6:.1f}M")
        m2.metric("Spot CEX Volume", f"${spot_vol/1e6:.1f}M", f"{spot_vol/total_vol*100:.0f}%")
        m3.metric("DEX Volume", f"${dex_vol/1e6:.1f}M", f"{dex_vol/total_vol*100:.0f}%")
        m4.metric("# Markets Listed", str(len(exchange_df)))

        st.divider()
        ca, cb = st.columns(2)

        with ca:
            top20 = exchange_df.nlargest(20, "volume_usd")
            fig_ex = px.bar(
                top20, x="volume_usd", y="exchange",
                orientation="h",
                color="market_type",
                title="Top 20 Markets by Volume",
                color_discrete_map={"spot": "#7c3aed", "dex": "#06b6d4"},
                template="plotly_dark",
                labels={"volume_usd": "Volume (USD)", "exchange": ""},
            )
            fig_ex.update_layout(yaxis={"categoryorder": "total ascending"}, height=500)
            st.plotly_chart(fig_ex, use_container_width=True)

        with cb:
            geo_vol = exchange_df.groupby("geography")["volume_usd"].sum().reset_index()
            fig_geo = px.pie(
                geo_vol, values="volume_usd", names="geography",
                title="Volume by Geography",
                template="plotly_dark", hole=0.3,
            )
            st.plotly_chart(fig_geo, use_container_width=True)

            type_vol = exchange_df.groupby("market_type")["volume_usd"].sum().reset_index()
            fig_type = px.pie(
                type_vol, values="volume_usd", names="market_type",
                title="Spot vs DEX Volume",
                color="market_type",
                color_discrete_map={"spot": "#7c3aed", "dex": "#06b6d4"},
                template="plotly_dark", hole=0.3,
            )
            st.plotly_chart(fig_type, use_container_width=True)

        # Quote currency breakdown
        st.subheader("Volume by Quote Currency (top 10)")
        quote_vol = (
            exchange_df.groupby("quote")["volume_usd"]
            .sum().sort_values(ascending=False).head(10).reset_index()
        )
        fig_quote = px.bar(
            quote_vol, x="quote", y="volume_usd",
            title="Volume by Quote Currency",
            color_discrete_sequence=["#7c3aed"],
            template="plotly_dark",
            labels={"volume_usd": "Volume (USD)", "quote": "Quote Currency"},
        )
        st.plotly_chart(fig_quote, use_container_width=True)

        st.subheader("All Exchange Data")
        st.dataframe(
            exchange_df[["exchange", "base", "quote", "volume_usd", "price_usd",
                          "market_type", "geography"]]
            .sort_values("volume_usd", ascending=False),
            use_container_width=True,
            column_config={
                "volume_usd": st.column_config.NumberColumn("Volume (USD)", format="$%.0f"),
                "price_usd": st.column_config.NumberColumn("Price (USD)", format="$%.4f"),
            },
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — RISK & SIGNALS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_risk:
    st.header("Risk Signals & Predictive Insights")

    cl, cr = st.columns(2)

    with cl:
        st.subheader("⚠️ Active Risk Signals")

        signals = []
        if current:
            # Significant move with no recent events
            recent_events_2d = events_df[
                events_df["date"] >= (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            ] if not events_df.empty else pd.DataFrame()

            if abs(current["pct_24h"]) > 5 and recent_events_2d.empty:
                signals.append(("🔴 High", f"Price moved {current['pct_24h']:+.1f}% in 24h with no logged events — possible untracked news or speculation"))

            if current["cex_volume_24h"] > 0:
                dex_ratio = current["dex_volume_24h"] / current["cex_volume_24h"]
                if dex_ratio > 0.2:
                    signals.append(("🟡 Medium", f"DEX is {dex_ratio*100:.0f}% of CEX volume — elevated on-chain trading activity"))

            if current["pct_7d"] < -10:
                signals.append(("🔴 High", f"7d price down {current['pct_7d']:.1f}% — sustained downtrend"))

            if current["pct_24h"] > 15:
                signals.append(("🟡 Medium", f"Price up {current['pct_24h']:+.1f}% in 24h — watch for reversal"))

        if not signals:
            signals.append(("🟢 Low", "No major risk signals detected currently"))

        for severity, msg in signals:
            level = severity.split()[0]
            if "🔴" in level:
                st.error(f"**{severity}**: {msg}")
            elif "🟡" in level:
                st.warning(f"**{severity}**: {msg}")
            else:
                st.success(f"**{severity}**: {msg}")

        # Repeated negative patterns
        if not events_df.empty:
            st.divider()
            st.subheader("📊 Sentiment by Event Type")
            sent_matrix = (
                events_df.groupby(["event_type", "sentiment_label"])
                .size().unstack(fill_value=0).reset_index()
            )
            st.dataframe(sent_matrix, use_container_width=True)

    with cr:
        st.subheader("🔮 Predictive Signals")

        if current:
            momentum = "bullish" if current["pct_1h"] > 0 and current["pct_24h"] > 0 else \
                       "bearish" if current["pct_1h"] < 0 and current["pct_24h"] < 0 else "mixed"
            momentum_icon = "🟢" if momentum == "bullish" else ("🔴" if momentum == "bearish" else "🟡")

            st.markdown(f"""
**Current Market State:**
| Metric | Value |
|--------|-------|
| Price | ${current['price']:.4f} |
| 1h Momentum | {current['pct_1h']:+.2f}% |
| 24h Trend | {current['pct_24h']:+.2f}% |
| Short Momentum | {momentum_icon} {momentum.title()} |
| CMC Rank | #{current['cmc_rank']} |
""")

        st.divider()
        st.markdown("""
**Leading Indicators to Watch:**
- 📊 Volume spike without news → possible unannounced event incoming
- 🐦 Increase in tweet frequency from @GoKiteAI → announcement likely
- 🌏 KRW/TRY exchange volume spike → retail-driven momentum (Korea/Turkey)
- 📉 DEX volume surge → on-chain activity, possible whale movement

**Observed Pattern Library:**
| Pattern | Typical Outcome |
|---------|----------------|
| Partnership tweet → | Short spike +3–8%, then correction |
| Listing announcement → | 24h spike, normalizes in 3–5d |
| Airdrop / reward event → | Sell pressure post-claim period |
| Regulatory news → | Immediate drop, slow recovery |
""")

        # Recent events summary
        if not events_df.empty:
            st.divider()
            st.subheader("📅 Latest 7 Events")
            recent = events_df.head(7)[["date", "event_type", "description", "sentiment_label", "source"]]
            st.dataframe(recent, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.caption(
    f"🪁 KITE Dashboard · Data: CoinGecko + CoinMarketCap · Events: @GoKiteAI · "
    f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC"
)
