"""
Correlation logic: daily (legacy) and hourly (preferred).
"""
import pandas as pd
from datetime import datetime, timedelta

# ── Event type human descriptions (shown in hover tooltips) ──────────────────

EVENT_DESCRIPTIONS = {
    "Partnership":    "Collaboration or integration with another company / protocol",
    "Product Launch": "New feature, mainnet, or product going live",
    "Listing":        "KITE token added to a new exchange or trading market",
    "Funding":        "Investment round or fundraise announced",
    "Airdrop":        "Free token distribution or reward claim event",
    "Milestone":      "Achievement: user count, transaction record, etc.",
    "Community":      "AMA, governance vote, hackathon, or community event",
    "Announcement":   "General update or news from the team",
}

HOURLY_METRICS = ["T+1h %", "T+4h %", "T+12h %", "T+24h %", "T+3d %"]
DAILY_METRICS  = ["T+1d %", "T+3d %", "T+7d %"]


# ── Daily correlation (fallback for older data) ───────────────────────────────

def compute_impact_rows(price_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    if price_df.empty or events_df.empty:
        return pd.DataFrame()

    price_idx = price_df.sort_values("date").set_index("date")
    dates_sorted = sorted(price_idx.index.tolist())

    def price_at_offset(ev_pos: int, n: int):
        idx = ev_pos + n
        if 0 <= idx < len(dates_sorted):
            return price_idx.loc[dates_sorted[idx], "price_usd"]
        return None

    impact_rows = []
    for _, ev in events_df.iterrows():
        ev_date = ev["date"]
        if ev_date not in price_idx.index:
            continue

        ev_price = price_idx.loc[ev_date, "price_usd"]
        ev_vol   = price_idx.loc[ev_date, "volume_24h"]
        ev_pos   = dates_sorted.index(ev_date)

        p1 = price_at_offset(ev_pos, 1)
        p3 = price_at_offset(ev_pos, 3)
        p7 = price_at_offset(ev_pos, 7)

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


def compute_impact_rows_hourly(price_hourly_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """
    Hourly-resolution correlation.
    Offsets: T+1h, T+4h, T+12h, T+24h, T+3d (72h).
    Matches each event's datetime to the nearest hourly price bucket.
    Vol spike is vs 24-hour pre-event average.
    """
    if price_hourly_df.empty or events_df.empty:
        return pd.DataFrame()

    price_idx = price_hourly_df.sort_values("datetime").set_index("datetime")
    hours_sorted = sorted(price_idx.index.tolist())

    def price_at_offset(ev_pos: int, n: int):
        idx = ev_pos + n
        if 0 <= idx < len(hours_sorted):
            return price_idx.loc[hours_sorted[idx], "price_usd"]
        return None

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
            continue

        ev_pos   = hours_sorted.index(ev_hour)
        ev_price = price_idx.loc[ev_hour, "price_usd"]
        ev_vol   = price_idx.loc[ev_hour, "volume_24h"]

        p1h  = price_at_offset(ev_pos, 1)
        p4h  = price_at_offset(ev_pos, 4)
        p12h = price_at_offset(ev_pos, 12)
        p24h = price_at_offset(ev_pos, 24)
        p3d  = price_at_offset(ev_pos, 72)

        # Vol spike vs 24h pre-event average
        pre_vols = [
            price_idx.loc[h, "volume_24h"]
            for h in hours_sorted[max(0, ev_pos - 24):ev_pos]
            if pd.notna(price_idx.loc[h, "volume_24h"])
        ]
        avg_vol = sum(pre_vols) / len(pre_vols) if pre_vols else ev_vol
        vol_spike_pct = ((ev_vol / avg_vol) - 1) * 100 if avg_vol and avg_vol > 0 else 0

        ev_type = ev.get("event_type", "")
        impact_rows.append({
            "ID":          ev.get("id"),
            "Datetime":    ev_hour,
            "Event Type":  ev_type,
            "Type Info":   EVENT_DESCRIPTIONS.get(ev_type, ""),
            "Description": (ev.get("description") or "")[:70],
            "Sentiment":   ev.get("sentiment_label", "neutral"),
            "Price":       round(ev_price, 5),
            "T+1h %":      pct(p1h,  ev_price),
            "T+4h %":      pct(p4h,  ev_price),
            "T+12h %":     pct(p12h, ev_price),
            "T+24h %":     pct(p24h, ev_price),
            "T+3d %":      pct(p3d,  ev_price),
            "Vol Spike %": round(vol_spike_pct, 1),
        })

    return pd.DataFrame(impact_rows)
