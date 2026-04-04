"""
Twitter/X scraper for @GoKiteAI.
- scrape_tweets_api: uses twitterapi.io (budget-safe, date-range-aware) — preferred
- scrape_tweets_browser: Playwright fallback
- scrape_tweets: twikit fallback
"""
import os
import re
import requests as _requests
from datetime import datetime, timezone

_WORD_RE = re.compile(r"\b\w+\b")

# ---- Sentiment ----

BULLISH_WORDS = {
    "partnership", "partner", "launch", "mainnet", "integration", "integrate",
    "milestone", "listing", "listed", "growth", "adoption", "funding", "invest",
    "collaboration", "collaborate", "expand", "deploy", "release", "upgrade",
    "announce", "proud", "excited", "thrilled", "huge", "major", "breakthrough",
    "record", "achievement", "reward", "airdrop", "staking", "live", "new", "first",
    "join", "welcome", "support", "build", "innovate", "scale", "leading",
}

BEARISH_WORDS = {
    "hack", "exploit", "bug", "delay", "suspend", "halt", "regulatory",
    "scam", "breach", "vulnerability", "attack", "issue", "problem",
    "concern", "warning", "risk", "crash", "dump",
}

EVENT_KEYWORDS = {
    "Partnership": ["partner", "partnership", "collaboration", "integrate", "alliance", "x with", "join forces"],
    "Product Launch": ["launch", "release", "deploy", "mainnet", "live", "new feature", "now available", "introducing"],
    "Listing": ["listing", "listed", "now trading", "available on", "new exchange", "new market"],
    "Funding": ["funding", "investment", "raise", "series", "round", "backed", "capital", "seed"],
    "Airdrop": ["airdrop", "drop", "reward", "claim", "eligible", "snapshot"],
    "Milestone": ["milestone", "record", "achievement", "users", "transactions", "first", "100k", "1m"],
    "Community": ["ama", "community", "vote", "governance", "proposal", "hackathon", "meetup"],
    "Security": ["hack", "exploit", "bug", "vulnerability", "breach", "attack", "scam", "phishing",
                 "security", "audit", "patch", "fix", "incident", "compromised"],
}


def score_sentiment(text: str) -> tuple[float, str]:
    words = set(_WORD_RE.findall(text.lower()))
    bullish = len(words & BULLISH_WORDS)
    bearish = len(words & BEARISH_WORDS)
    total = bullish + bearish
    if total == 0:
        return 0.0, "neutral"
    score = round((bullish - bearish) / total, 3)
    label = "positive" if score > 0.15 else ("negative" if score < -0.15 else "neutral")
    return score, label


def classify_event(text: str) -> str:
    text_lower = text.lower()
    for event_type, keywords in EVENT_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return event_type
    return "Announcement"


# ---- twitterapi.io (preferred, budget-safe) ----

def _get_twitter_api_key() -> str:
    key = os.environ.get("TWITTERAPI_IO_KEY", "")
    if not key:
        try:
            import streamlit as st
            key = st.secrets.get("TWITTERAPI_IO_KEY", "")
        except Exception:
            pass
    return key


def _parse_twitter_date(date_str: str):
    """Parse Twitter's createdAt: 'Mon Apr 04 12:00:00 +0000 2026'"""
    try:
        clean = re.sub(r"\s+\+\d{4}\s+", " ", date_str)
        return datetime.strptime(clean, "%a %b %d %H:%M:%S %Y").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def scrape_tweets_api(
    username: str = "GoKiteAI",
    start_date: str = "2025-11-01",
    end_date: str = None,
    max_tweets: int = 100,
) -> tuple[list, str | None]:
    """Fetch tweets via twitterapi.io filtered to [start_date, end_date].

    Paginates in small batches (20) and stops as soon as tweets older than
    start_date appear — minimising API credit usage.
    """
    api_key = _get_twitter_api_key()
    if not api_key:
        return [], "TWITTERAPI_IO_KEY not configured. Add it to .env or Streamlit secrets."

    if end_date is None:
        end_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )

    headers = {"X-API-Key": api_key}
    results: list[dict] = []
    cursor = None

    while len(results) < max_tweets:
        batch_size = min(20, max_tweets - len(results))
        params: dict = {"userName": username, "count": batch_size}
        if cursor:
            params["cursor"] = cursor

        try:
            r = _requests.get(
                "https://api.twitterapi.io/twitter/user/tweets",
                headers=headers,
                params=params,
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            return results, f"API error: {e}"

        tweets = data.get("tweets", [])
        if not tweets:
            break

        reached_start = False
        for t in tweets:
            tweet_dt = _parse_twitter_date(t.get("createdAt", ""))
            if tweet_dt is None:
                continue
            if tweet_dt > end_dt:
                continue  # too recent; skip (shouldn't normally happen)
            if tweet_dt < start_dt:
                reached_start = True
                break  # passed start boundary — stop paging

            text = (t.get("text") or "").strip()
            tweet_id = str(t.get("id") or "")
            sentiment_score, sentiment_label = score_sentiment(text)
            event_type = classify_event(text)

            results.append({
                "date": tweet_dt.strftime("%Y-%m-%d"),
                "datetime_str": tweet_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "event_type": event_type,
                "description": text[:300],
                "source": "twitter",
                "tweet_id": tweet_id,
                "tweet_url": f"https://x.com/{username}/status/{tweet_id}" if tweet_id else None,
                "tweet_text": text,
                "sentiment_score": sentiment_score,
                "sentiment_label": sentiment_label,
                "expected_impact": (
                    "bullish" if sentiment_score > 0.15
                    else ("bearish" if sentiment_score < -0.15 else "neutral")
                ),
                "likes": t.get("likeCount") or 0,
                "retweets": t.get("retweetCount") or 0,
                "replies": t.get("replyCount") or 0,
            })

            if len(results) >= max_tweets:
                break

        if reached_start or len(results) >= max_tweets:
            break

        cursor = data.get("next_cursor") or data.get("nextCursor") or data.get("cursor")
        if not cursor:
            break

    return results, None


