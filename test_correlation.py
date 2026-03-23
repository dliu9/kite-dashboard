"""
Tests for Event → Price Correlation Analysis.

Verifies that when a post is captured with a timestamp, the price window
(T+1d, T+3d, T+7d) correctly aligns to the days AFTER the event date,
and that volume spike is computed against the 7-day pre-event average.

Run with:  python -m pytest test_correlation.py -v
"""
import pytest
import pandas as pd
from correlation import compute_impact_rows


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_prices(start: str, n_days: int, base_price: float = 1.0,
                price_fn=None, base_vol: float = 1_000_000) -> pd.DataFrame:
    """
    Build a price DataFrame with n_days consecutive daily rows starting from `start`.
    price_fn(i) -> price for day i (0-indexed).  Default: base_price + i * 0.01.
    """
    dates = pd.date_range(start, periods=n_days, freq="D").strftime("%Y-%m-%d").tolist()
    if price_fn is None:
        price_fn = lambda i: round(base_price + i * 0.01, 5)
    return pd.DataFrame({
        "date": dates,
        "price_usd": [price_fn(i) for i in range(n_days)],
        "volume_24h": [base_vol] * n_days,
    })


def make_event(date: str, event_type: str = "Announcement",
               description: str = "test event",
               sentiment_label: str = "neutral", eid: int = 1) -> pd.DataFrame:
    return pd.DataFrame([{
        "id": eid,
        "date": date,
        "event_type": event_type,
        "description": description,
        "sentiment_label": sentiment_label,
    }])


# ── 1. Date alignment ─────────────────────────────────────────────────────────

class TestDateAlignment:
    """
    Core check: the date a post is captured maps to the right price windows.
    """

    def test_t1_is_next_day_in_price_data(self):
        """T+1d price should be the row immediately after the event date."""
        prices = make_prices("2025-01-01", 10, base_price=1.0)
        # Day 0 = 1.00, Day 1 = 1.01, Day 2 = 1.02, ...
        event = make_event("2025-01-04")  # index 3 → price 1.03

        result = compute_impact_rows(prices, event)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["Date"] == "2025-01-04"
        assert row["Price"] == pytest.approx(1.03, abs=1e-5)

        # T+1d should use 2025-01-05 (price 1.04)
        expected_t1 = round((1.04 / 1.03 - 1) * 100, 2)
        assert row["T+1d %"] == pytest.approx(expected_t1, abs=0.01)

    def test_t3_is_three_positions_after_event(self):
        """T+3d should use the price 3 rows after the event in sorted date list."""
        prices = make_prices("2025-01-01", 15, base_price=1.0)
        event = make_event("2025-01-05")  # index 4 → price 1.04

        result = compute_impact_rows(prices, event)
        row = result.iloc[0]

        # T+3d → index 7 → 2025-01-08 → price 1.07
        expected_t3 = round((1.07 / 1.04 - 1) * 100, 2)
        assert row["T+3d %"] == pytest.approx(expected_t3, abs=0.01)

    def test_t7_is_seven_positions_after_event(self):
        """T+7d should use the price 7 rows after the event."""
        prices = make_prices("2025-01-01", 20, base_price=1.0)
        event = make_event("2025-01-05")  # index 4 → price 1.04

        result = compute_impact_rows(prices, event)
        row = result.iloc[0]

        # T+7d → index 11 → 2025-01-12 → price 1.11
        expected_t7 = round((1.11 / 1.04 - 1) * 100, 2)
        assert row["T+7d %"] == pytest.approx(expected_t7, abs=0.01)

    def test_offset_is_positional_not_calendar(self):
        """
        If price data has a gap (missing calendar day), T+1d is the next
        AVAILABLE price row, not necessarily the next calendar day.

        This is expected behaviour: crypto data from CoinGecko may have gaps.
        If this test fails it means the logic has been changed to calendar-based.
        """
        # Build prices skipping 2025-01-06 (a gap)
        rows = [
            {"date": "2025-01-04", "price_usd": 1.00, "volume_24h": 1_000_000},
            {"date": "2025-01-05", "price_usd": 1.10, "volume_24h": 1_000_000},
            # gap: 2025-01-06 missing
            {"date": "2025-01-07", "price_usd": 1.20, "volume_24h": 1_000_000},
            {"date": "2025-01-08", "price_usd": 1.30, "volume_24h": 1_000_000},
        ]
        prices = pd.DataFrame(rows)
        event = make_event("2025-01-05")  # positional index 1

        result = compute_impact_rows(prices, event)
        row = result.iloc[0]

        # T+1d → positional index 2 → 2025-01-07 (skips the gap)
        expected_t1 = round((1.20 / 1.10 - 1) * 100, 2)
        assert row["T+1d %"] == pytest.approx(expected_t1, abs=0.01)


# ── 2. Event timestamp → price window matching ────────────────────────────────

class TestTimestampToWindow:
    """
    Simulate a real scenario: a @GoKiteAI post is captured on a specific date
    and we verify the price windows point to the right calendar dates.
    """

    def test_partnership_post_price_window(self):
        """
        Scenario: Partnership post on 2025-03-01.
        Price goes up 5% next day, then another 3% by day 3, then -1% by day 7.
        Verify each window captures the right move.
        """
        prices = pd.DataFrame([
            {"date": "2025-02-22", "price_usd": 0.050, "volume_24h": 500_000},
            {"date": "2025-02-23", "price_usd": 0.051, "volume_24h": 500_000},
            {"date": "2025-02-24", "price_usd": 0.052, "volume_24h": 500_000},
            {"date": "2025-02-25", "price_usd": 0.053, "volume_24h": 500_000},
            {"date": "2025-02-26", "price_usd": 0.054, "volume_24h": 500_000},
            {"date": "2025-02-27", "price_usd": 0.055, "volume_24h": 500_000},
            {"date": "2025-02-28", "price_usd": 0.056, "volume_24h": 500_000},
            {"date": "2025-03-01", "price_usd": 0.100, "volume_24h": 2_000_000},  # event day
            {"date": "2025-03-02", "price_usd": 0.105, "volume_24h": 1_800_000},  # T+1
            {"date": "2025-03-03", "price_usd": 0.102, "volume_24h": 1_500_000},
            {"date": "2025-03-04", "price_usd": 0.103, "volume_24h": 1_200_000},  # T+3
            {"date": "2025-03-05", "price_usd": 0.098, "volume_24h": 900_000},
            {"date": "2025-03-06", "price_usd": 0.097, "volume_24h": 800_000},
            {"date": "2025-03-07", "price_usd": 0.096, "volume_24h": 750_000},
            {"date": "2025-03-08", "price_usd": 0.099, "volume_24h": 700_000},  # T+7
        ])
        event = make_event("2025-03-01", event_type="Partnership", sentiment_label="positive")

        result = compute_impact_rows(prices, event)
        assert len(result) == 1
        row = result.iloc[0]

        assert row["Date"] == "2025-03-01"
        assert row["Price"] == pytest.approx(0.100, rel=1e-4)

        # T+1d = 2025-03-02: +5%
        assert row["T+1d %"] == pytest.approx(5.0, abs=0.1)

        # T+3d = 2025-03-04: +3%
        assert row["T+3d %"] == pytest.approx(3.0, abs=0.1)

        # T+7d = 2025-03-08: -1%
        assert row["T+7d %"] == pytest.approx(-1.0, abs=0.1)

    def test_window_dates_are_after_event_not_before(self):
        """
        Sanity check: T+1, T+3, T+7 must all be AFTER the event date.
        No look-ahead bias — the analysis only uses future prices.
        """
        prices = make_prices("2025-01-01", 20, base_price=1.0)
        event_date = "2025-01-10"
        event = make_event(event_date)

        result = compute_impact_rows(prices, event)
        row = result.iloc[0]

        # All offsets should produce price changes from a future date
        # i.e. if price strictly increases, all T+Nd should be positive
        # prices[i] = 1.0 + i * 0.01, so prices always go up
        assert row["T+1d %"] > 0
        assert row["T+3d %"] > 0
        assert row["T+7d %"] > 0


# ── 3. Edge cases ─────────────────────────────────────────────────────────────

class TestEdgeCases:

    def test_event_date_not_in_price_data_is_skipped(self):
        """An event whose date has no price row must be silently skipped."""
        prices = make_prices("2025-01-01", 10)
        event = make_event("2025-02-01")  # outside price range

        result = compute_impact_rows(prices, event)
        assert result.empty

    def test_event_near_end_has_none_for_missing_offsets(self):
        """If T+7d falls beyond available price data, it should be None."""
        prices = make_prices("2025-01-01", 5)  # only 5 days
        event = make_event("2025-01-03")  # index 2; T+7 would need index 9

        result = compute_impact_rows(prices, event)
        row = result.iloc[0]

        assert row["T+1d %"] is not None   # index 3 exists
        assert row["T+3d %"] is None       # index 5 doesn't exist
        assert row["T+7d %"] is None       # index 9 doesn't exist

    def test_event_at_very_end_has_all_none_offsets(self):
        """Event on the last price day → all T+Nd are None."""
        prices = make_prices("2025-01-01", 5)
        event = make_event("2025-01-05")  # last row

        result = compute_impact_rows(prices, event)
        row = result.iloc[0]

        assert row["T+1d %"] is None
        assert row["T+3d %"] is None
        assert row["T+7d %"] is None

    def test_multiple_events_same_date(self):
        """Two posts on the same date should each produce their own row."""
        prices = make_prices("2025-01-01", 15)
        events = pd.DataFrame([
            {"id": 1, "date": "2025-01-05", "event_type": "Partnership",
             "description": "post A", "sentiment_label": "positive"},
            {"id": 2, "date": "2025-01-05", "event_type": "Airdrop",
             "description": "post B", "sentiment_label": "neutral"},
        ])
        result = compute_impact_rows(prices, events)

        assert len(result) == 2
        assert set(result["Event Type"]) == {"Partnership", "Airdrop"}
        # Both rows share the same Date and Price
        assert result.iloc[0]["Price"] == result.iloc[1]["Price"]

    def test_no_price_data_returns_empty(self):
        event = make_event("2025-01-05")
        result = compute_impact_rows(pd.DataFrame(), event)
        assert result.empty

    def test_no_events_returns_empty(self):
        prices = make_prices("2025-01-01", 10)
        result = compute_impact_rows(prices, pd.DataFrame())
        assert result.empty


# ── 4. Volume spike calculation ───────────────────────────────────────────────

class TestVolSpike:

    def test_vol_spike_is_vs_7day_pre_event_average(self):
        """
        Vol spike % = (event_vol / avg_of_prev_7_days - 1) * 100.
        """
        rows = []
        for i, d in enumerate(pd.date_range("2025-01-01", periods=10, freq="D")):
            rows.append({
                "date": d.strftime("%Y-%m-%d"),
                "price_usd": 1.0 + i * 0.01,
                "volume_24h": 1_000_000,  # flat baseline vol
            })
        # Override event day (index 7 = 2025-01-08) with 3x volume
        rows[7]["volume_24h"] = 3_000_000

        prices = pd.DataFrame(rows)
        event = make_event("2025-01-08")

        result = compute_impact_rows(prices, event)
        row = result.iloc[0]

        # avg of prev 7 days (indices 0-6) = 1_000_000; spike = (3/1 - 1)*100 = 200%
        assert row["Vol Spike %"] == pytest.approx(200.0, abs=0.5)

    def test_below_average_volume_is_negative_spike(self):
        """Vol spike can be negative (below-average volume on event day)."""
        rows = [{"date": f"2025-01-0{i+1}", "price_usd": 1.0, "volume_24h": 2_000_000}
                for i in range(8)]
        rows[7]["volume_24h"] = 500_000  # half the average
        prices = pd.DataFrame(rows)
        event = make_event("2025-01-08")

        result = compute_impact_rows(prices, event)
        row = result.iloc[0]

        assert row["Vol Spike %"] < 0  # below average → negative

    def test_vol_spike_uses_up_to_7_pre_event_days(self):
        """When fewer than 7 days of history exist, uses what's available."""
        rows = [{"date": f"2025-01-0{i+1}", "price_usd": 1.0, "volume_24h": 1_000_000}
                for i in range(3)]
        rows.append({"date": "2025-01-04", "price_usd": 1.03, "volume_24h": 4_000_000})
        prices = pd.DataFrame(rows)
        event = make_event("2025-01-04")  # only 3 days of history

        result = compute_impact_rows(prices, event)
        row = result.iloc[0]

        # avg of 3 pre-days = 1_000_000; spike = (4/1 - 1)*100 = 300%
        assert row["Vol Spike %"] == pytest.approx(300.0, abs=0.5)

    def test_first_ever_event_no_history_spike_is_zero(self):
        """Event on the very first price day → no pre-history → spike = 0."""
        prices = make_prices("2025-01-01", 5)
        event = make_event("2025-01-01")

        result = compute_impact_rows(prices, event)
        row = result.iloc[0]

        assert row["Vol Spike %"] == pytest.approx(0.0, abs=0.01)


# ── 5. Real-world scenario: $18M Series A announcement ───────────────────────

class TestRealWorldScenario:
    """
    Simulates the @GoKiteAI $18M Series A post (pinned tweet, 2025-09-02).
    Verifies the full pipeline: post captured → correct price window aligned.
    """

    def setup_method(self):
        # Simulated prices around announcement
        self.prices = pd.DataFrame([
            {"date": "2025-08-26", "price_usd": 0.0420, "volume_24h": 800_000},
            {"date": "2025-08-27", "price_usd": 0.0415, "volume_24h": 750_000},
            {"date": "2025-08-28", "price_usd": 0.0418, "volume_24h": 780_000},
            {"date": "2025-08-29", "price_usd": 0.0422, "volume_24h": 820_000},
            {"date": "2025-08-30", "price_usd": 0.0419, "volume_24h": 790_000},
            {"date": "2025-08-31", "price_usd": 0.0421, "volume_24h": 810_000},
            {"date": "2025-09-01", "price_usd": 0.0425, "volume_24h": 900_000},
            # Announcement day
            {"date": "2025-09-02", "price_usd": 0.0450, "volume_24h": 5_000_000},
            # Post-announcement
            {"date": "2025-09-03", "price_usd": 0.0480, "volume_24h": 4_200_000},  # T+1
            {"date": "2025-09-04", "price_usd": 0.0510, "volume_24h": 3_800_000},
            {"date": "2025-09-05", "price_usd": 0.0495, "volume_24h": 3_100_000},  # T+3
            {"date": "2025-09-06", "price_usd": 0.0488, "volume_24h": 2_500_000},
            {"date": "2025-09-07", "price_usd": 0.0472, "volume_24h": 2_000_000},
            {"date": "2025-09-08", "price_usd": 0.0461, "volume_24h": 1_800_000},
            {"date": "2025-09-09", "price_usd": 0.0455, "volume_24h": 1_600_000},  # T+7
        ])
        self.event = make_event(
            "2025-09-02",
            event_type="Funding",
            description="$18M Series A fundraise led by PayPal Ventures and General Catalyst",
            sentiment_label="positive",
        )

    def test_event_date_captured_correctly(self):
        result = compute_impact_rows(self.prices, self.event)
        assert result.iloc[0]["Date"] == "2025-09-02"

    def test_price_on_announcement_day(self):
        result = compute_impact_rows(self.prices, self.event)
        assert result.iloc[0]["Price"] == pytest.approx(0.0450, rel=1e-4)

    def test_t1_is_day_after_announcement(self):
        """T+1d must use 2025-09-03 price, not any earlier date."""
        result = compute_impact_rows(self.prices, self.event)
        row = result.iloc[0]
        # 0.0480 / 0.0450 - 1 = ~6.67%
        assert row["T+1d %"] == pytest.approx(6.67, abs=0.1)

    def test_t3_is_three_days_after_announcement(self):
        """T+3d must use 2025-09-05."""
        result = compute_impact_rows(self.prices, self.event)
        row = result.iloc[0]
        # 0.0495 / 0.0450 - 1 = 10%
        assert row["T+3d %"] == pytest.approx(10.0, abs=0.1)

    def test_t7_is_seven_days_after_announcement(self):
        """T+7d must use 2025-09-09."""
        result = compute_impact_rows(self.prices, self.event)
        row = result.iloc[0]
        # 0.0455 / 0.0450 - 1 = ~1.11%
        assert row["T+7d %"] == pytest.approx(1.11, abs=0.1)

    def test_vol_spike_on_announcement_day(self):
        """High volume on announcement day should show big positive spike."""
        result = compute_impact_rows(self.prices, self.event)
        row = result.iloc[0]
        # avg 7-day pre vol ≈ 807k; event vol = 5_000_000 → spike ≈ +519%
        assert row["Vol Spike %"] > 400

    def test_no_future_price_used_for_t0(self):
        """Price on event day must NOT include future information."""
        result = compute_impact_rows(self.prices, self.event)
        # T+0 (Price column) should equal the price on 2025-09-02 exactly
        assert result.iloc[0]["Price"] == pytest.approx(0.0450, rel=1e-4)
