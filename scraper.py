"""
Twitter/X scraper for @GoKiteAI using twikit.
Supports guest mode (no login) and authenticated mode (login with X account).
Cookies are saved after first login so you only need to log in once.
"""
import asyncio
import re
from pathlib import Path

COOKIES_PATH = Path(__file__).parent / "x_cookies.json"

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
}


def score_sentiment(text: str) -> tuple[float, str]:
    words = set(re.findall(r"\b\w+\b", text.lower()))
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


# ---- Async scraping ----

async def _scrape_async(username: str = "GoKiteAI", max_tweets: int = 100) -> list:
    try:
        from twikit import Client
    except ImportError:
        return []

    if not COOKIES_PATH.exists():
        return []

    client = Client("en-US")
    client.load_cookies(str(COOKIES_PATH))
    user = await client.get_user_by_screen_name(username)
    tweets = await user.get_tweets("Tweets", count=max_tweets)
    return _parse_tweets(tweets)


async def _scrape_sync_api(username: str = "GoKiteAI", max_tweets: int = 100) -> list:
    """Fallback for twikit versions where methods are not coroutines."""
    try:
        from twikit import Client
    except ImportError:
        return []

    if not COOKIES_PATH.exists():
        return []

    client = Client("en-US")
    client.load_cookies(str(COOKIES_PATH))
    user = client.get_user_by_screen_name(username)
    if asyncio.iscoroutine(user):
        user = await user
    tweets = user.get_tweets("Tweets", count=max_tweets)
    if asyncio.iscoroutine(tweets):
        tweets = await tweets
    return _parse_tweets(tweets)


def _parse_tweets(tweets) -> list[dict]:
    results = []
    for tweet in tweets:
        text = getattr(tweet, "text", "") or ""
        if not text:
            continue
        created = getattr(tweet, "created_at", None)
        date_str = str(created)[:10] if created else ""
        datetime_str = str(created)[:19] if created else ""
        tweet_id = str(getattr(tweet, "id", ""))
        sentiment_score, sentiment_label = score_sentiment(text)
        event_type = classify_event(text)
        results.append({
            "date": date_str,
            "datetime_str": datetime_str,
            "event_type": event_type,
            "description": text[:300],
            "source": "twitter",
            "tweet_id": tweet_id,
            "tweet_url": f"https://x.com/GoKiteAI/status/{tweet_id}" if tweet_id else None,
            "tweet_text": text,
            "sentiment_score": sentiment_score,
            "sentiment_label": sentiment_label,
            "expected_impact": "bullish" if sentiment_score > 0.15 else ("bearish" if sentiment_score < -0.15 else "neutral"),
        })
    return results


def scrape_tweets(username: str = "GoKiteAI", max_tweets: int = 100):
    """Synchronous wrapper — returns (list, error_str). error_str is None on success."""
    try:
        results = asyncio.run(_scrape_sync_api(username, max_tweets))
        return results, None
    except Exception as e:
        return [], str(e)


# ---- Browser-based scraping (Playwright) ----

async def _scrape_browser_async(username: str = "GoKiteAI", max_tweets: int = 50) -> list:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        raise RuntimeError("playwright not installed — run: pip install playwright && playwright install chromium")

    results = []
    seen_ids = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # Inject cookies if available
        if COOKIES_PATH.exists():
            import json
            raw = json.loads(COOKIES_PATH.read_text())
            playwright_cookies = [
                {"name": k, "value": v, "domain": ".x.com", "path": "/", "secure": True, "httpOnly": True}
                for k, v in raw.items()
                if isinstance(v, str)
            ]
            if playwright_cookies:
                await context.add_cookies(playwright_cookies)

        page = await context.new_page()
        await page.goto(f"https://x.com/{username}", wait_until="domcontentloaded")

        # Wait for timeline — if cookies are expired X loads a blank page,
        # so we retry without cookies as a fallback
        try:
            await page.wait_for_selector("article", timeout=10000)
        except Exception:
            # Cookies likely expired — close and retry without them
            await browser.close()
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            await page.goto(f"https://x.com/{username}", wait_until="domcontentloaded")
            await page.wait_for_selector("article", timeout=15000)

        no_new_count = 0  # consecutive scrolls with no new tweets
        while len(results) < max_tweets:
            articles = await page.query_selector_all("article")
            batch_new = 0

            for article in articles:
                # Tweet URL / ID
                links = await article.query_selector_all('a[href*="/status/"]')
                tweet_url, tweet_id = None, None
                for link in links:
                    href = await link.get_attribute("href")
                    if href and "/status/" in href:
                        parts = href.split("/status/")
                        if len(parts) == 2:
                            tweet_id = parts[1].split("/")[0].split("?")[0]
                            tweet_url = f"https://x.com{href}" if href.startswith("/") else href
                            break

                if not tweet_id or tweet_id in seen_ids:
                    continue
                seen_ids.add(tweet_id)
                batch_new += 1

                # Timestamp
                date_str, datetime_str = "", ""
                time_el = await article.query_selector("time")
                if time_el:
                    dt_attr = await time_el.get_attribute("datetime")
                    if dt_attr:
                        date_str = dt_attr[:10]
                        datetime_str = dt_attr[:19].replace("T", " ")

                # Tweet text
                text_el = await article.query_selector('[data-testid="tweetText"]')
                if text_el:
                    text = (await text_el.inner_text()).strip()
                else:
                    continue  # skip non-tweet articles

                if not text:
                    continue

                text = text[:300]
                sentiment_score, sentiment_label = score_sentiment(text)
                event_type = classify_event(text)

                results.append({
                    "date": date_str,
                    "datetime_str": datetime_str,
                    "event_type": event_type,
                    "description": text,
                    "source": "twitter",
                    "tweet_id": tweet_id,
                    "tweet_url": tweet_url,
                    "tweet_text": text,
                    "sentiment_score": sentiment_score,
                    "sentiment_label": sentiment_label,
                    "expected_impact": "bullish" if sentiment_score > 0.15 else ("bearish" if sentiment_score < -0.15 else "neutral"),
                })

                if len(results) >= max_tweets:
                    break

            if batch_new == 0:
                no_new_count += 1
                if no_new_count >= 3:
                    break  # X stopped loading new content
            else:
                no_new_count = 0

            # Scroll down and wait for X to load more tweets
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(3000)

        await browser.close()

    return results


def scrape_tweets_browser(username: str = "GoKiteAI", max_tweets: int = 50):
    """Browser-based scraper via Playwright. Returns (list, error_str)."""
    import concurrent.futures

    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_scrape_browser_async(username, max_tweets)), None
        except Exception as e:
            return [], str(e)
        finally:
            loop.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_run)
        results, err = future.result(timeout=300)  # 5 min for large fetches
    return results, err


# ---- Login ----

async def _login_async(username: str, email: str, password: str):
    from twikit import Client
    client = Client("en-US")
    await client.login(auth_info_1=username, auth_info_2=email, password=password)
    client.save_cookies(str(COOKIES_PATH))
    return True


def login(username: str, email: str, password: str):
    asyncio.run(_login_async(username, email, password))
