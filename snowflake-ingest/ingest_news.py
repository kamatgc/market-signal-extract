from config import get_snowflake_session
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas
import re

load_dotenv()

NEWS_API_KEY = "6d64d928177b4e13a102a58cc67eb494"
NEWS_API_URL = "https://newsapi.org/v2/everything"

def fetch_news(symbol):
    params = {
        "q": symbol,
        "sortBy": "publishedAt",
        "language": "en",
        "apiKey": NEWS_API_KEY
    }
    response = requests.get(NEWS_API_URL, params=params)
    if response.status_code != 200:
        raise Exception(f"News API error: {response.status_code}")

    articles = response.json().get("articles", [])
    if not articles:
        raise Exception("No news articles returned")

    records = []
    for article in articles:
        headline = article.get("title", "")
        sentiment = score_sentiment(headline)
        records.append({
            "symbol": symbol,
            "headline": headline,
            "sentiment": sentiment,
            "source": article.get("source", {}).get("name", ""),
            "url": article.get("url", ""),
            "published_at": article.get("publishedAt", ""),
            "ingested_at": datetime.utcnow().isoformat()
        })

    df = pd.DataFrame(records)
    df.columns = [sanitize_column_name(col) for col in df.columns]
    df.reset_index(drop=True, inplace=True)
    return df

def sanitize_column_name(name):
    name = str(name).strip().lower()
    name = re.sub(r"[^\w]", "_", name)
    return name

def score_sentiment(text):
    text = text.lower()
    if any(word in text for word in ["gain", "rise", "surge", "beat", "strong", "positive"]):
        return "Positive"
    elif any(word in text for word in ["drop", "fall", "miss", "weak", "negative", "loss"]):
        return "Negative"
    else:
        return "Neutral"

def ensure_schema_exists(session, schema_name):
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}").collect()

def write_to_snowflake(df, table_name="NEWS_RAW"):
    session = get_snowflake_session()
    schema = session.get_current_schema()
    ensure_schema_exists(session, schema)

    conn = session._conn._conn
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name,
        database=session.get_current_database(),
        schema=schema,
        overwrite=True
    )

    if success:
        print(f"✅ {nrows} articles written to {schema}.{table_name}")
    else:
        raise Exception("❌ Failed to write to Snowflake")

if __name__ == "__main__":
    symbol = "IBM"
    df = fetch_news(symbol)
    write_to_snowflake(df)

