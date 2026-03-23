"""
Correlation logic: daily (legacy) and hourly (preferred).
"""
import pandas as pd
from datetime import datetime, timedelta, date as date_type

# ── Event type human descriptions (shown in hover tooltips) ──────────────────

EVENT_DESCRIPTIONS = {
    "Partnership":    "Collaboration or integration with another company / protocol",
    "Product Launch": "New feature, mainnet, or product going live",
    "Listing":        "KITE token added to a new exchange or trading market",
    "Funding":        "Investment round or fundraise announced",
    "Airdrop":        "Free token distribution or reward claim event",
    "Milestone":      "Achievement: user count, transaction record, etc.",
    "Community":      "AMA, governance vote, hackathon, or community event",
    "Security":       "Hack, exploit, vulnerability disclosure, or security audit",
    "Regulation":     "Regulatory news, compliance update, or legal development",
    "Announcement":   "General update or news from the team",
}

HOURLY_METRICS = ["T+1h %", "T+4h %", "T+12h %", "T+24h %", "T+3d %"]
DAILY_METRICS  = ["T+1d %", "T+3d %", "T+7d %"]


# ── Daily correlation (fallback for older data) ───────────────────────────────

def compute_impact_rows(price_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    if price_df.empty or events_df.empty:
        return pd.DataFrame()

    price_idx = price_df.sort_values("date").set_index("date")

    def price_at_day(base_date: str, n_days: int):
        """Look up price exactly n_days after base_date using calendar arithmetic."""
        target = (datetime.strptime(base_date, "%Y-%m-%d") + timedelta(days=n_days)).strftime("%Y-%m-%d")
        return price_idx.loc[target, "price_usd"] if target in price_idx.index else None

    dates_sorted = sorted(price_idx.index.tolist())

    impact_rows = []
    for _, ev in events_df.iterrows():
        ev_date = ev["date"]
        if ev_date not in price_idx.index:
            continue

        ev_price = price_idx.loc[ev_date, "price_usd"]
        ev_vol   = price_idx.loc[ev_date, "volume_24h"]
        ev_pos   = dates_sorted.index(ev_date)

        p1 = price_at_day(ev_date, 1)
        p3 = price_at_day(ev_date, 3)
        p7 = price_at_day(ev_date, 7)

        pre_vols = [
            price_idx.loc[d, "volume_24h"]
            for d in dates_sorted[max(0, ev_pos - 7):ev_pos]
            if pd.notna(price_idx.loc[d, "volume_24h"])
        ]
        avg_vol = sum(pre_vols) / len(pre_vols) if pre_vols else ev_vol
        vol_spike_pct = ((ev_vol / avg_vol) - 1) * 100 if avg_vol and avg_vol > 0 else 0

        ev_type = ev.get("event_type", "")
        impact_rows.append({
            "ID":          ev.get("id"),
            "Date":        ev_date,
            "Event Type":  ev_type,
            "Type Info":   EVENT_DESCRIPTIONS.get(ev_type, ""),
            "Description": (ev.get("description") or "")[:70],
            "Sentiment":   ev.get("sentiment_label", "neutral"),
            "Price":       round(ev_price, 5),
            "T+1d %":      round((p1 / ev_price - 1) * 100, 2) if p1 else None,
            "T+3d %":      round((p3 / ev_price - 1) * 100, 2) if p3 else None,
            "T+7d %":      round((p7 / ev_price - 1) * 100, 2) if p7 else None,
            "Vol Spike %": round(vol_spike_pct, 1),
        })

    return pd.DataFrame(impact_rows)


# ── Hourly correlation (preferred) ───────────────────────────────────────────

def _round_to_hour(dt_str: str):
    """Parse 'YYYY-MM-DD HH:MM:SS' and round to nearest hour → 'YYYY-MM-DD HH:00'."""
    try:
        dt = datetime.strptime(dt_str[:16], "%Y-%m-%d %H:%M")
        if dt.minute >= 30:
            dt = dt.replace(minute=0, second=0) + timedelta(hours=1)
        else:
            dt = dt.replace(minute=0, second=0)
        return dt.strftime("%Y-%m-%d %H:00")
    except Exception:
        return None


def compute_impact_rows_hourly(price_hourly_df: pd.DataFrame, events_df: pd.DataFrame,
                               price_daily_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Hourly-resolution correlation with daily fallback for older events.
    - Events within hourly range: T+1h, T+4h, T+12h, T+24h, T+3d
    - Events outside hourly range: T+24h (≈T+1d) and T+3d from daily data
    Resolution column indicates which path was used.
    """
    if events_df.empty:
        return pd.DataFrame()
    if price_hourly_df.empty and (price_daily_df is None or price_daily_df.empty):
        return pd.DataFrame()

    price_idx    = price_hourly_df.sort_values("datetime").set_index("datetime") if not price_hourly_df.empty else pd.DataFrame()
    hours_sorted = sorted(price_idx.index.tolist()) if not price_idx.empty else []

    daily_df  = price_daily_df if price_daily_df is not None else pd.DataFrame()
    daily_idx = daily_df.sort_values("date").set_index("date") if not daily_df.empty else pd.DataFrame()
    dates_sorted = sorted(daily_idx.index.tolist()) if not daily_idx.empty else []

    def price_at_hour(base_hour: str, n_hours: int):
        """Look up price exactly n_hours after base_hour using calendar arithmetic."""
        target = (datetime.strptime(base_hour, "%Y-%m-%d %H:00") + timedelta(hours=n_hours)).strftime("%Y-%m-%d %H:00")
        return price_idx.loc[target, "price_usd"] if target in price_idx.index else None

    def pct(p, base):
        return round((p / base - 1) * 100, 2) if p and base else None

    impact_rows = []
    for _, ev in events_df.iterrows():
        # Try datetime_str first (has hour precision), fall back to date at 00:00
        raw_dt = ev.get("datetime_str") or ""
        if not raw_dt or raw_dt.strip() == "":
            raw_dt = (ev.get("date", "") or "") + " 00:00:00"

        ev_hour = _round_to_hour(raw_dt)
        if not ev_hour or ev_hour not in price_idx.index:
            ev_hour = None  # handled below as daily fallback

        ev_type = ev.get("event_type", "")
        base_row = {
            "ID":          ev.get("id"),
            "Event Type":  ev_type,
            "Type Info":   EVENT_DESCRIPTIONS.get(ev_type, ""),
            "Description": (ev.get("description") or "")[:70],
            "Sentiment":   ev.get("sentiment_label", "neutral"),
        }

        if ev_hour:
            # ── Hourly path ──
            # Use the price 1 hour BEFORE the event as T+0 anchor to avoid
            # same-hour contamination (event-day price already embeds intraday move)
            anchor_hour = price_at_hour(ev_hour, -1) or ev_hour
            ev_price = price_idx.loc[anchor_hour, "price_usd"]
            ev_vol   = price_idx.loc[ev_hour, "volume_24h"]

            # Precompute prev-hour keys once to avoid redundant string ops
            prev_hour_keys = [
                (datetime.strptime(ev_hour, "%Y-%m-%d %H:00") - timedelta(hours=i)).strftime("%Y-%m-%d %H:00")
                for i in range(1, 25)
            ]
            pre_vols = [
                price_idx.loc[h, "volume_24h"]
                for h in prev_hour_keys
                if h in price_idx.index and pd.notna(price_idx.loc[h, "volume_24h"])
            ]
            avg_vol = sum(pre_vols) / len(pre_vols) if pre_vols else ev_vol
            vol_spike_pct = ((ev_vol / avg_vol) - 1) * 100 if avg_vol and avg_vol > 0 else 0

            impact_rows.append({
                **base_row,
                "Datetime":    ev_hour,
                "Resolution":  "hourly",
                "Price":       round(ev_price, 5),
                "T+1h %":      pct(price_at_hour(ev_hour, 1),  ev_price),
                "T+4h %":      pct(price_at_hour(ev_hour, 4),  ev_price),
                "T+12h %":     pct(price_at_hour(ev_hour, 12), ev_price),
                "T+24h %":     pct(price_at_hour(ev_hour, 24), ev_price),
                "T+3d %":      pct(price_at_hour(ev_hour, 72), ev_price),
                "Vol Spike %": round(vol_spike_pct, 1),
            })
        else:
            # ── Daily fallback for events outside hourly range ──
            ev_date = ev.get("date", "")
            if ev_date not in daily_idx.index:
                continue
            ev_price = daily_idx.loc[ev_date, "price_usd"]
            ev_vol   = daily_idx.loc[ev_date, "volume_24h"]

            def dprice(n):
                target = (datetime.strptime(ev_date, "%Y-%m-%d") + timedelta(days=n)).strftime("%Y-%m-%d")
                return daily_idx.loc[target, "price_usd"] if target in daily_idx.index else None

            pre_vols = [
                daily_idx.loc[(datetime.strptime(ev_date, "%Y-%m-%d") - timedelta(days=i)).strftime("%Y-%m-%d"), "volume_24h"]
                for i in range(1, 8)
                if (datetime.strptime(ev_date, "%Y-%m-%d") - timedelta(days=i)).strftime("%Y-%m-%d") in daily_idx.index
                and pd.notna(daily_idx.loc[(datetime.strptime(ev_date, "%Y-%m-%d") - timedelta(days=i)).strftime("%Y-%m-%d"), "volume_24h"])
            ]
            avg_vol = sum(pre_vols) / len(pre_vols) if pre_vols else ev_vol
            vol_spike_pct = ((ev_vol / avg_vol) - 1) * 100 if avg_vol and avg_vol > 0 else 0

            impact_rows.append({
                **base_row,
                "Datetime":    ev_date,
                "Resolution":  "daily",
                "Price":       round(ev_price, 5),
                "T+1h %":      None,
                "T+4h %":      None,
                "T+12h %":     None,
                "T+24h %":     pct(dprice(1), ev_price),   # T+1d ≈ T+24h
                "T+3d %":      pct(dprice(3), ev_price),
                "Vol Spike %": round(vol_spike_pct, 1),
            })

    return pd.DataFrame(impact_rows)
