import requests
import pandas as pd
import re
from config import COMPANY_NAMES, FINNHUB_KEY

ARTICLE_CACHE = {}

POSITIVE_KEYWORDS = [
    "beats", "record revenue", "record profit", "strong earnings", "new product",
    "growth", "surge", "upgrade", "launch", "partnership", "boost",
    "strong demand", "revenue jump", "expansion", "acquisition", "positive outlook"
]

NEGATIVE_KEYWORDS = [
    "downgrade", "ceo steps down", "missed estimates", "lawsuit", "slump", "drop",
    "cut jobs", "cut forecast", "probe", "investigation", "recall", "problem",
    "may not boost", "concern", "struggle", "negative outlook", "regulatory risk"
]

def score_sentiment(text):
    text = text.lower()
    score = 0.0
    for word in POSITIVE_KEYWORDS:
        if re.search(rf"\b{re.escape(word)}\b", text):
            score += 0.2
    for word in NEGATIVE_KEYWORDS:
        if re.search(rf"\b{re.escape(word)}\b", text):
            score -= 0.2
    return max(min(score, 1.0), -1.0)

def fetch_articles(symbol, entry_date):
    cache_key = f"{symbol}_{entry_date.date()}"
    if cache_key in ARTICLE_CACHE:
        return ARTICLE_CACHE[cache_key]

    from_date = (entry_date - pd.Timedelta(days=5)).date().isoformat()
    to_date = entry_date.date().isoformat()
    url = f"https://finnhub.io/api/v1/company-news?symbol={symbol}&from={from_date}&to={to_date}&token={FINNHUB_KEY}"

    try:
        response = requests.get(url, timeout=10)
        articles = response.json()
        for article in articles:
            title = article.get("headline", "")
            summary = article.get("summary", "")
            published = pd.to_datetime(article.get("datetime"), unit='s').date()
            if published > pd.to_datetime(to_date).date():
                continue
            content = f"{title} {summary}".lower()
            if symbol.lower() in content or COMPANY_NAMES.get(symbol, symbol).lower() in content:
                score = score_sentiment(content)
                result = (score, title, article.get("url", "https://www.marketwatch.com"))
                ARTICLE_CACHE[cache_key] = result
                print(f"[MATCHED] {title} → Sentiment Score: {score}")
                return result
        ARTICLE_CACHE[cache_key] = (0.0, "No relevant article found", "https://www.marketwatch.com")
        print("[NO MATCH] No relevant article found")
        return ARTICLE_CACHE[cache_key]
    except Exception as e:
        ARTICLE_CACHE[cache_key] = (0.0, "Finnhub fetch failed", "https://www.marketwatch.com")
        print(f"[ERROR] Finnhub fetch failed: {e}")
        return ARTICLE_CACHE[cache_key]

