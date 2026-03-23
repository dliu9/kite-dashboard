import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = Path(__file__).parent / "kite.db"


class Database:
    def __init__(self):
        self.conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        self._init_tables()

    def _init_tables(self):
        self.conn.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS price_history (
                date TEXT PRIMARY KEY,
                price_usd REAL,
                volume_24h REAL,
                market_cap REAL
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                datetime_str TEXT,
                event_type TEXT,
                description TEXT,
                source TEXT DEFAULT 'manual',
                tweet_id TEXT UNIQUE,
                tweet_url TEXT,
                tweet_text TEXT,
                sentiment_score REAL DEFAULT 0,
                sentiment_label TEXT DEFAULT 'neutral',
                expected_impact TEXT DEFAULT 'neutral',
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS exchange_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fetched_date TEXT,
                exchange TEXT,
                base TEXT,
                quote TEXT,
                volume_usd REAL,
                price_usd REAL,
                market_type TEXT DEFAULT 'spot',
                geography TEXT
            );
            CREATE TABLE IF NOT EXISTS price_hourly (
                datetime TEXT PRIMARY KEY,
                price_usd REAL,
                volume_24h REAL
            );
            CREATE TABLE IF NOT EXISTS refresh_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT DEFAULT (datetime('now')),
                data_type TEXT,
                records_updated INTEGER DEFAULT 0,
                status TEXT DEFAULT 'success',
                error_message TEXT,
                duration_ms INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_events_date        ON events(date);
            CREATE INDEX IF NOT EXISTS idx_events_type        ON events(event_type);
            CREATE INDEX IF NOT EXISTS idx_events_source      ON events(source);
            CREATE INDEX IF NOT EXISTS idx_exchange_fetched   ON exchange_snapshots(fetched_date);
            CREATE INDEX IF NOT EXISTS idx_refresh_log_type   ON refresh_log(data_type, timestamp);
        """)
        self.conn.commit()

    # ---- Price History ----

    def upsert_prices(self, df: pd.DataFrame) -> int:
        rows = df[["date", "price_usd", "volume_24h", "market_cap"]].to_dict("records")
        cur = self.conn.cursor()
        for row in rows:
            cur.execute(
                "INSERT OR REPLACE INTO price_history (date, price_usd, volume_24h, market_cap) VALUES (?,?,?,?)",
                (row["date"], row["price_usd"], row["volume_24h"], row["market_cap"]),
            )
        self.conn.commit()
        return len(rows)

    def get_prices(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        if start_date and end_date:
            return pd.read_sql(
                "SELECT * FROM price_history WHERE date BETWEEN ? AND ? ORDER BY date ASC",
                self.conn, params=(start_date, end_date),
            )
        return pd.read_sql("SELECT * FROM price_history ORDER BY date ASC", self.conn)

    # ---- Hourly Prices ----

    def upsert_hourly_prices(self, df: pd.DataFrame) -> int:
        rows = df[["datetime", "price_usd", "volume_24h"]].to_dict("records")
        cur = self.conn.cursor()
        for row in rows:
            cur.execute(
                "INSERT OR REPLACE INTO price_hourly (datetime, price_usd, volume_24h) VALUES (?,?,?)",
                (row["datetime"], row["price_usd"], row["volume_24h"]),
            )
        self.conn.commit()
        return len(rows)

    def get_hourly_prices(self, start_dt: str = None, end_dt: str = None) -> pd.DataFrame:
        if start_dt and end_dt:
            return pd.read_sql(
                "SELECT * FROM price_hourly WHERE datetime BETWEEN ? AND ? ORDER BY datetime ASC",
                self.conn, params=(start_dt, end_dt),
            )
        return pd.read_sql("SELECT * FROM price_hourly ORDER BY datetime ASC", self.conn)

    # ---- Events ----

    def add_event(self, ev: dict) -> int:
        cur = self.conn.cursor()
        cur.execute(
            """INSERT OR IGNORE INTO events
               (date, datetime_str, event_type, description, source,
                tweet_id, tweet_url, tweet_text,
                sentiment_score, sentiment_label, expected_impact)
               VALUES (:date,:datetime_str,:event_type,:description,:source,
                       :tweet_id,:tweet_url,:tweet_text,
                       :sentiment_score,:sentiment_label,:expected_impact)""",
            ev,
        )
        self.conn.commit()
        return cur.lastrowid

    def get_events(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        if start_date and end_date:
            return pd.read_sql(
                "SELECT * FROM events WHERE date BETWEEN ? AND ? ORDER BY date DESC",
                self.conn, params=(start_date, end_date),
            )
        return pd.read_sql("SELECT * FROM events ORDER BY date DESC", self.conn)

    def delete_event(self, event_id: int):
        self.conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
        self.conn.commit()

    def get_existing_tweet_ids(self) -> set:
        cur = self.conn.execute("SELECT tweet_id FROM events WHERE tweet_id IS NOT NULL")
        return {row[0] for row in cur.fetchall()}

    # ---- Exchange Snapshots ----

    def upsert_exchange_snapshots(self, df: pd.DataFrame, fetched_date: str) -> int:
        self.conn.execute("DELETE FROM exchange_snapshots WHERE fetched_date = ?", (fetched_date,))
        rows = df.to_dict("records")
        cur = self.conn.cursor()
        for row in rows:
            cur.execute(
                """INSERT INTO exchange_snapshots
                   (fetched_date, exchange, base, quote, volume_usd, price_usd, market_type, geography)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (fetched_date, row["exchange"], row["base"], row["quote"],
                 row["volume_usd"], row["price_usd"], row["market_type"],
                 row.get("geography", "Global")),
            )
        self.conn.commit()
        return len(rows)

    def get_latest_exchange_snapshots(self) -> pd.DataFrame:
        return pd.read_sql(
            """SELECT * FROM exchange_snapshots
               WHERE fetched_date = (SELECT MAX(fetched_date) FROM exchange_snapshots)
               ORDER BY volume_usd DESC""",
            self.conn,
        )

    # ---- Refresh Log ----

    def log_refresh(self, data_type: str, records: int, status: str = "success",
                    error_message: str = None, duration_ms: int = None):
        self.conn.execute(
            "INSERT INTO refresh_log (data_type, records_updated, status, error_message, duration_ms) VALUES (?,?,?,?,?)",
            (data_type, records, status, error_message, duration_ms),
        )
        self.conn.commit()

    def get_last_refresh(self, data_type: str) -> str:
        cur = self.conn.execute(
            "SELECT timestamp FROM refresh_log WHERE data_type=? ORDER BY timestamp DESC LIMIT 1",
            (data_type,),
        )
        row = cur.fetchone()
        return row[0] if row else "Never"
