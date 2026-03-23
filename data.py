import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone

try:
    import streamlit as st
    CMC_API_KEY = st.secrets.get("CMC_API_KEY", os.getenv("CMC_API_KEY", ""))
except Exception:
    CMC_API_KEY = os.getenv("CMC_API_KEY", "")
CMC_BASE = "https://pro-api.coinmarketcap.com/v1"
CG_BASE = "https://api.coingecko.com/api/v3"

KITE_CMC_ID = 38828
KITE_CG_ID = "kite-2"

# Exchanges known to be in specific regions based on quote currency / name
GEO_MAP = {
    "KRW": "South Korea",
    "TRY": "Turkey",
    "BRL": "Brazil",
    "RUB": "Russia",
    "JPY": "Japan",
    "EUR": "Europe",
    "GBP": "Europe",
    "AUD": "Australia",
}
USA_EXCHANGES = {"Coinbase Exchange", "Coinbase Pro", "Kraken", "Gemini", "Bitstamp"}
DEX_KEYWORDS = ["uniswap", "pancake", "velodrome", "curve", "balancer", "sushi",
                "camelot", "trader joe", "aerodrome", "lynex", "ramses"]


class DataFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "KiteDashboard/1.0"

    # ---- CMC: Current Snapshot ----

    def get_current_snapshot(self):
        try:
            r = self.session.get(
                f"{CMC_BASE}/cryptocurrency/quotes/latest",
                params={"id": KITE_CMC_ID},
                headers={"X-CMC_PRO_API_KEY": CMC_API_KEY},
                timeout=10,
            )
            d = r.json()["data"][str(KITE_CMC_ID)]
            q = d["quote"]["USD"]
            return {
                "price": q["price"],
                "volume_24h": q["volume_24h"],
                "cex_volume_24h": q.get("cex_volume_24h", 0) or 0,
                "dex_volume_24h": q.get("dex_volume_24h", 0) or 0,
                "pct_1h": q["percent_change_1h"],
                "pct_24h": q["percent_change_24h"],
                "pct_7d": q["percent_change_7d"],
                "pct_30d": q["percent_change_30d"],
                "pct_60d": q.get("percent_change_60d"),
                "market_cap": q["market_cap"],
                "fdv": q["fully_diluted_market_cap"],
                "circulating_supply": d["circulating_supply"],
                "max_supply": d["max_supply"],
                "cmc_rank": d["cmc_rank"],
                "last_updated": q["last_updated"],
            }
        except Exception:
            return None

    # ---- CoinGecko: Historical Prices ----

    def get_historical_prices(self, days: int = 140) -> pd.DataFrame:
        try:
            r = self.session.get(
                f"{CG_BASE}/coins/{KITE_CG_ID}/market_chart",
                params={"vs_currency": "usd", "days": days, "interval": "daily"},
                timeout=20,
            )
            r.raise_for_status()
            data = r.json()
            prices = data.get("prices", [])
            volumes = data.get("total_volumes", [])
            market_caps = data.get("market_caps", [])
            rows = []
            for i, (ts, price) in enumerate(prices):
                date = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                rows.append({
                    "date": date,
                    "price_usd": price,
                    "volume_24h": volumes[i][1] if i < len(volumes) else None,
                    "market_cap": market_caps[i][1] if i < len(market_caps) else None,
                })
            return pd.DataFrame(rows).drop_duplicates("date")
        except Exception:
            return pd.DataFrame()

    # ---- CoinGecko: Exchange Tickers ----

    def get_exchange_tickers(self, max_pages: int = 4) -> pd.DataFrame:
        all_tickers = []
        for page in range(1, max_pages + 1):
            try:
                r = self.session.get(
                    f"{CG_BASE}/coins/{KITE_CG_ID}/tickers",
                    params={"page": page, "order": "volume_desc", "depth": "false"},
                    timeout=15,
                )
                r.raise_for_status()
                tickers = r.json().get("tickers", [])
                if not tickers:
                    break
                all_tickers.extend(tickers)
                time.sleep(1.2)  # CoinGecko free rate limit
            except Exception:
                break

        rows = []
        for t in all_tickers:
            exchange = t.get("market", {}).get("name", "Unknown")
            base = t.get("base", "")
            quote = t.get("target", "")
            vol_usd = t.get("converted_volume", {}).get("usd") or 0
            price_usd = t.get("converted_last", {}).get("usd") or 0
            market_type = "dex" if any(k in exchange.lower() for k in DEX_KEYWORDS) else "spot"
            geography = self._classify_geo(exchange, quote)
            rows.append({
                "exchange": exchange,
                "base": base,
                "quote": quote,
                "volume_usd": float(vol_usd),
                "price_usd": float(price_usd),
                "market_type": market_type,
                "geography": geography,
            })
        return pd.DataFrame(rows)

    def _classify_geo(self, exchange: str, quote: str) -> str:
        if quote in GEO_MAP:
            return GEO_MAP[quote]
        if exchange in USA_EXCHANGES:
            return "USA"
        return "Global"
