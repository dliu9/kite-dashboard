"""
LLM Insight Generator for KITE Dashboard.
Registry maps chart_id → metadata; generate_insights() calls Claude Haiku.
"""
import json
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── Chart metadata registry ────────────────────────────────────────────────────
CHART_REGISTRY = {
    "price_trend": {
        "title": "Price Trend with Event Impact",
        "objective": "Tracks KITE token price over time (hourly or daily) with a 7-day moving average, overlaid with business event markers. Shows how price behaves around key announcements, partnerships, and launches.",
        "data_source": "CoinGecko API (hourly/daily OHLC) + Events from @GoKiteAI Twitter",
    },
    "trading_volume": {
        "title": "Trading Volume",
        "objective": "Daily or hourly trading volume coloured by price direction — cyan for up days, purple for down days. Helps identify volume spikes and their relationship to price momentum.",
        "data_source": "CoinGecko API (daily/hourly volume)",
    },
    "period_performance": {
        "title": "Period Performance Summary",
        "objective": "Summarises the high, low, best day, and worst day within the selected date range — a quick snapshot of price range and volatility for the period.",
        "data_source": "CoinGecko API (daily price history)",
    },
    "trend_overview": {
        "title": "Trend Overview",
        "objective": "7-day, 30-day, and 60-day percentage returns for KITE, giving a quick read on short, medium, and longer-term momentum.",
        "data_source": "CoinMarketCap API (live quote)",
    },
    "event_activity": {
        "title": "Event Activity Analysis",
        "objective": "Analyses event rate over time: events in last 30 days vs prior 30, events per week, and velocity trend. High event frequency often precedes price moves.",
        "data_source": "Events from @GoKiteAI Twitter (via twitterapi.io)",
    },
    "events_by_type": {
        "title": "Events by Type",
        "objective": "Breaks down all logged events into categories (Partnership, Product Launch, Listing, etc.) to show which type of activity is most frequent.",
        "data_source": "Events from @GoKiteAI Twitter (via twitterapi.io)",
    },
    "events_per_month": {
        "title": "Events per Month",
        "objective": "Monthly event frequency coloured by the dominant event type. Useful for spotting periods of high activity and seasonal patterns in event types.",
        "data_source": "Events from @GoKiteAI Twitter (via twitterapi.io)",
    },
    "sentiment_split": {
        "title": "Sentiment Split",
        "objective": "Distribution of positive, neutral, and negative sentiment across all logged events. High positive sentiment can correlate with bullish price action.",
        "data_source": "Events from @GoKiteAI Twitter — sentiment scored using keyword analysis",
    },
    "signal_scorecard": {
        "title": "Signal Quality Scorecard",
        "objective": "Summarises which event type historically produces the best next-day price return, how many events led to a price rise vs fall, and the overall hit rate.",
        "data_source": "CoinGecko prices cross-referenced with @GoKiteAI events",
    },
    "avg_price_change": {
        "title": "Average Price Change by Event Type",
        "objective": "Average price return at multiple time horizons (T+1h, T+24h, T+3d) broken down by event category. Reveals which event types are strongest price catalysts.",
        "data_source": "CoinGecko prices matched to @GoKiteAI events",
    },
    "return_heatmap": {
        "title": "Return Heatmap by Event Type & Time Window",
        "objective": "Colour matrix of average price return across event types and time windows. Green = positive average return, red = negative. Shows catalyst strength at a glance.",
        "data_source": "CoinGecko prices matched to @GoKiteAI events",
    },
    "avg_return_bar": {
        "title": "Average Return by Event Type",
        "objective": "Average return for the selected time window sorted by event type. Makes it easy to rank which events are the strongest catalysts at the chosen horizon.",
        "data_source": "CoinGecko prices matched to @GoKiteAI events",
    },
    "standout_events": {
        "title": "Standout Events",
        "objective": "Top 5 best and worst individual events at the selected time horizon — the specific events that caused the largest price moves.",
        "data_source": "CoinGecko prices matched to @GoKiteAI events",
    },
    "early_late_reaction": {
        "title": "Early vs Late Reaction per Event",
        "objective": "Compares market reaction speed: T+24h (quick) vs T+3d (slow). Strong T+24h but weak T+3d means the market priced in the news fast with no sustained follow-through.",
        "data_source": "CoinGecko prices matched to @GoKiteAI events",
    },
    "full_impact_table": {
        "title": "Full Event Impact Table",
        "objective": "Complete view of every event with all time-window returns, sentiment, and price at the time — the source data underlying all correlation analysis.",
        "data_source": "CoinGecko prices matched to @GoKiteAI events",
    },
    "top20_markets": {
        "title": "Top 20 Markets by Volume",
        "objective": "Which exchanges and trading pairs carry the most KITE volume. CEX (blue) vs DEX (green) coloring highlights where institutional vs on-chain liquidity sits.",
        "data_source": "CoinGecko Tickers API",
    },
    "volume_geography": {
        "title": "Volume by Geography & Exchange",
        "objective": "Treemap of trading volume by geography and exchange. Heavy Korea volume signals retail-driven momentum; USA dominance signals institutional participation.",
        "data_source": "CoinGecko Tickers API",
    },
    "spot_dex_split": {
        "title": "Spot vs DEX Volume Split",
        "objective": "Proportion of volume on centralised exchanges (spot) vs decentralised protocols. A high DEX ratio signals elevated on-chain and DeFi activity.",
        "data_source": "CoinGecko Tickers API",
    },
    "price_consistency": {
        "title": "Price Consistency",
        "objective": "Identifies exchanges where KITE trades at a price deviating more than 0.5% from the median — potential arbitrage opportunities or thin liquidity on those venues.",
        "data_source": "CoinGecko Tickers API",
    },
    "volume_quote_currency": {
        "title": "Volume by Quote Currency",
        "objective": "Which quote currencies (USDT, BTC, KRW, etc.) dominate KITE trading. KRW/TRY spikes indicate retail momentum in South Korea or Turkey.",
        "data_source": "CoinGecko Tickers API",
    },
    "all_exchange_data": {
        "title": "All Exchange Data",
        "objective": "Full table of every KITE trading pair across all exchanges, with volume, price, market type, and geography for each.",
        "data_source": "CoinGecko Tickers API",
    },
    "price_trend_analysis": {
        "title": "Price Trend Analysis",
        "objective": "30-day rolling return shows medium-term momentum; 7-day rolling volatility shows turbulence. Together they characterise the current trend regime.",
        "data_source": "CoinGecko API (daily price history)",
    },
    "event_signal_intelligence": {
        "title": "Event Signal Intelligence",
        "objective": "Baseline-adjusted returns isolate the pure event effect above daily drift; Event Frequency vs Weekly Return overlays how busy event weeks relate to price performance.",
        "data_source": "CoinGecko prices + @GoKiteAI events",
    },
    "volume_intelligence": {
        "title": "Volume Intelligence",
        "objective": "Highlights days where trading volume exceeded 1.8× the 30-day rolling average (red anomaly days). Volume anomalies often precede or coincide with major price moves.",
        "data_source": "CoinGecko API (daily volume)",
    },
    "data_quality": {
        "title": "Data Quality Dashboard",
        "objective": "Freshness and completeness of all data: events loaded, daily and hourly price rows, and when each data source was last refreshed.",
        "data_source": "Internal database refresh log",
    },
    "sentiment_heatmap": {
        "title": "Sentiment Heatmap by Event Type",
        "objective": "Colour matrix showing how many events of each type fall into positive, neutral, or negative sentiment. Reveals whether certain event categories are consistently bullish or mixed.",
        "data_source": "Events from @GoKiteAI Twitter — sentiment scored using keyword analysis",
    },
    "rolling_signal_quality": {
        "title": "Rolling Signal Quality",
        "objective": "7-day rolling hit rate (% of events followed by a positive next-day return) over the past 30 days. Above 50% = events are currently bullish catalysts.",
        "data_source": "CoinGecko prices + @GoKiteAI events",
    },
    "pattern_library": {
        "title": "Pattern Library",
        "objective": "Historical hit rate showing how often the price rose the next day after each event type. Green bars (≥50%) = event types that historically precede price rises more than not.",
        "data_source": "CoinGecko daily prices + @GoKiteAI events",
    },
    "latest_events": {
        "title": "Latest 7 Events",
        "objective": "Quick-glance table of the 7 most recent events from @GoKiteAI: date, type, description, and sentiment.",
        "data_source": "Events from @GoKiteAI Twitter (via twitterapi.io)",
    },
}


# ── Data snapshot extractor ────────────────────────────────────────────────────

def get_data_snapshot(chart_id: str, **dfs) -> str:
    """Extract key stats from relevant dataframes for a given chart_id."""
    try:
        price_df    = dfs.get("price_df",    pd.DataFrame())
        hourly_df   = dfs.get("hourly_df",   pd.DataFrame())
        events_df   = dfs.get("events_df",   pd.DataFrame())
        exchange_df = dfs.get("exchange_df", pd.DataFrame())
        chart_df    = dfs.get("chart_df",    pd.DataFrame())
        impact_df   = dfs.get("impact_df",   pd.DataFrame())
        current     = dfs.get("current",     {}) or {}

        snap: list[str] = []

        if chart_id in ("price_trend", "trading_volume", "period_performance", "trend_overview"):
            df = chart_df if not chart_df.empty else price_df
            if not df.empty and "price_usd" in df.columns:
                snap.append(f"Price range: ${df['price_usd'].min():.4f} – ${df['price_usd'].max():.4f}")
                snap.append(f"Latest price: ${df['price_usd'].iloc[-1]:.4f}")
                snap.append(f"Data points: {len(df)}")
                if "volume_24h" in df.columns:
                    snap.append(f"Avg 24h volume: ${df['volume_24h'].mean():,.0f}")
            if current:
                snap.append(
                    f"Live: ${current.get('price', 0):.4f} | "
                    f"24h: {current.get('pct_24h', 0):+.2f}% | "
                    f"7d: {current.get('pct_7d', 0):+.2f}% | "
                    f"30d: {current.get('pct_30d', 0):+.2f}%"
                )

        elif chart_id in ("event_activity", "events_by_type", "events_per_month",
                          "sentiment_split", "latest_events"):
            if not events_df.empty:
                snap.append(f"Total events: {len(events_df)}")
                snap.append(f"By type: {json.dumps(events_df['event_type'].value_counts().to_dict())}")
                snap.append(f"Sentiment: {json.dumps(events_df['sentiment_label'].value_counts().to_dict())}")
                snap.append(f"Date range: {events_df['date'].min()} to {events_df['date'].max()}")
                if chart_id == "latest_events" and not events_df.empty:
                    latest = events_df.head(7)[["date", "event_type", "description"]].to_dict("records")
                    snap.append(f"Latest 7: {json.dumps(latest)}")

        elif chart_id in ("signal_scorecard", "avg_price_change", "return_heatmap", "avg_return_bar",
                          "standout_events", "early_late_reaction", "full_impact_table",
                          "rolling_signal_quality", "pattern_library", "event_signal_intelligence"):
            if not impact_df.empty:
                num_cols = [c for c in impact_df.select_dtypes(include="number").columns if "%" in c]
                snap.append(f"Events analysed: {len(impact_df)}")
                for col in num_cols[:4]:
                    v = impact_df[col].dropna()
                    snap.append(f"{col}: avg={v.mean():.2f}%, min={v.min():.2f}%, max={v.max():.2f}%")
                if "Event Type" in impact_df.columns and num_cols:
                    by_type = impact_df.groupby("Event Type")[num_cols[0]].mean().round(2).to_dict()
                    snap.append(f"Avg {num_cols[0]} by event type: {json.dumps(by_type)}")
                    positive_pct = round((impact_df[num_cols[0]].dropna() > 0).mean() * 100, 1)
                    snap.append(f"Hit rate ({num_cols[0]} > 0): {positive_pct}%")

        elif chart_id in ("top20_markets", "volume_geography", "spot_dex_split",
                          "price_consistency", "volume_quote_currency", "all_exchange_data"):
            if not exchange_df.empty:
                total_vol = exchange_df["volume_usd"].sum()
                snap.append(f"Total volume: ${total_vol:,.0f}")
                snap.append(f"Exchanges: {exchange_df['exchange'].nunique()}, Markets: {len(exchange_df)}")
                spot = exchange_df[exchange_df["market_type"] == "spot"]["volume_usd"].sum()
                dex  = exchange_df[exchange_df["market_type"] == "dex"]["volume_usd"].sum()
                snap.append(
                    f"Spot: ${spot:,.0f} ({spot / total_vol * 100:.1f}%), "
                    f"DEX: ${dex:,.0f} ({dex / total_vol * 100:.1f}%)"
                )
                top3 = exchange_df.groupby("exchange")["volume_usd"].sum().nlargest(3)
                snap.append(f"Top 3 exchanges: {json.dumps({k: round(v) for k, v in top3.items()})}")
                if "geography" in exchange_df.columns:
                    geo = exchange_df.groupby("geography")["volume_usd"].sum().sort_values(ascending=False).to_dict()
                    snap.append(f"By geography: {json.dumps({k: round(v) for k, v in geo.items()})}")

        elif chart_id in ("price_trend_analysis", "volume_intelligence"):
            if not price_df.empty and "price_usd" in price_df.columns:
                returns = price_df["price_usd"].pct_change() * 100
                snap.append(f"Daily return: avg {returns.mean():.2f}%, std {returns.std():.2f}%")
                snap.append(f"Best day: {returns.max():.2f}%, Worst day: {returns.min():.2f}%")
                if "volume_24h" in price_df.columns:
                    vols = price_df["volume_24h"].dropna()
                    anomalies = int((vols > 1.8 * vols.rolling(30).mean()).sum())
                    snap.append(f"Volume anomaly days (>1.8× 30d avg): {anomalies}")
                roll30 = price_df["price_usd"].pct_change(30) * 100
                snap.append(f"Latest 30d rolling return: {roll30.iloc[-1]:.2f}%")

        elif chart_id == "sentiment_heatmap":
            if not events_df.empty:
                hm = events_df.groupby(["event_type", "sentiment_label"]).size().unstack(fill_value=0)
                snap.append(f"Sentiment heatmap:\n{hm.to_string()}")

        elif chart_id == "data_quality":
            snap.append(f"Events: {len(events_df)}")
            snap.append(f"Daily price rows: {len(price_df)}")
            snap.append(f"Hourly price rows: {len(hourly_df)}")

        return "\n".join(snap) if snap else "No data currently available for this chart."
    except Exception as exc:
        return f"Data snapshot unavailable: {exc}"


# ── Azure OpenAI (AI Foundry) call ───────────────────────────────────────────

def generate_insights(chart_id: str, data_snapshot: str) -> str:
    """Call Azure OpenAI via AI Foundry Responses API to generate 4 bullet insights."""
    import requests as _req

    endpoint = os.environ.get("AZURE_OPENAI_ENDPOINT", "")
    api_key  = os.environ.get("AZURE_OPENAI_KEY", "")
    model    = os.environ.get("AZURE_OPENAI_MODEL", "gpt-4o-mini")

    if not endpoint or not api_key:
        return "⚠️ AZURE_OPENAI_ENDPOINT or AZURE_OPENAI_KEY not set. Check your .env file."

    chart_info = CHART_REGISTRY.get(chart_id, {})
    title      = chart_info.get("title", chart_id)
    objective  = chart_info.get("objective", "")

    prompt = (
        f"You are a senior crypto business and marketing strategist advising the KITE token team.\n\n"
        f"Chart: {title}\n"
        f"Objective: {objective}\n\n"
        f"Current data snapshot:\n{data_snapshot}\n\n"
        "Respond in exactly two sections separated by the line ---CTA---\n\n"
        "SECTION 1 — KEY INSIGHTS (4 bullets):\n"
        "- Reference actual numbers from the data where possible\n"
        "- Use ▲ for positive signals, ▼ for negative, ◆ for neutral\n"
        "- 1-2 sentences per bullet\n\n"
        "---CTA---\n\n"
        "SECTION 2 — PRIORITY ACTIONS (2-5 items):\n"
        "Pose the user as a business and marketing decision-maker for KITE token.\n"
        "Give only the most critical, specific actions they should take RIGHT NOW based on this data.\n"
        "Format each as: 🎯 Priority [N]: [Bold action verb] — [specific what/why in 1-2 sentences]\n"
        "Focus on: community campaigns, exchange outreach, partnership activation, "
        "announcement timing, or risk mitigation — whichever is most relevant.\n\n"
        "Output ONLY the two sections. No extra headers, no preamble."
    )

    try:
        resp = _req.post(
            endpoint,
            headers={"api-key": api_key, "Content-Type": "application/json"},
            json={
                "model": model,
                "input": prompt,
                "max_output_tokens": 450,
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        # Responses API: output[0].content[0].text
        return data["output"][0]["content"][0]["text"]
    except Exception as exc:
        return f"⚠️ Insight generation failed: {exc}"
