"""
Pure correlation logic extracted from app.py for testability.
Computes price impact rows for each event against price history.
"""
import pandas as pd


def compute_impact_rows(price_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    """
    For each event, compute price change at T+1d, T+3d, T+7d and volume spike.

    Offsets are positional (next available trading day), not strict calendar days.
    An event whose date has no matching price row is skipped.

    Parameters
    ----------
    price_df   : DataFrame with columns [date, price_usd, volume_24h]
    events_df  : DataFrame with columns [id, date, event_type, description,
                                         sentiment_label]

    Returns
    -------
    DataFrame with columns:
        ID, Date, Event Type, Description, Sentiment,
        Price, T+1d %, T+3d %, T+7d %, Vol Spike %
    """
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
        ev_vol = price_idx.loc[ev_date, "volume_24h"]
        ev_pos = dates_sorted.index(ev_date)

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

        impact_rows.append({
            "ID": ev.get("id"),
            "Date": ev_date,
            "Event Type": ev.get("event_type", ""),
            "Description": (ev.get("description") or "")[:70],
            "Sentiment": ev.get("sentiment_label", "neutral"),
            "Price": round(ev_price, 5),
            "T+1d %": round((p1 / ev_price - 1) * 100, 2) if p1 else None,
            "T+3d %": round((p3 / ev_price - 1) * 100, 2) if p3 else None,
            "T+7d %": round((p7 / ev_price - 1) * 100, 2) if p7 else None,
            "Vol Spike %": round(vol_spike_pct, 1),
        })

    return pd.DataFrame(impact_rows)
