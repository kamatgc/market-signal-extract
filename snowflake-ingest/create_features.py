from config import get_snowflake_session
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import re

load_dotenv()

def fetch_dataframes():
    session = get_snowflake_session()
    session.use_schema("RAW")

    stock_df = session.table("STOCK_DATA").to_pandas()
    news_df = session.table("NEWS_RAW").to_pandas()

    stock_df["date"] = pd.to_datetime(stock_df["date"]).dt.date
    news_df["published_at"] = pd.to_datetime(news_df["published_at"]).dt.date

    return stock_df, news_df, session

def fuzzy_join(stock_df, news_df):
    records = []

    for _, row in stock_df.iterrows():
        symbol = row["symbol"]
        date = row["date"]

        news_window = news_df[
            (news_df["symbol"] == symbol) &
            (news_df["published_at"] >= date - timedelta(days=1)) &
            (news_df["published_at"] <= date + timedelta(days=1))
        ]

        sentiment_score = sum(
            1 if s.lower() == "positive" else -1 if s.lower() == "negative" else 0
            for s in news_window["sentiment"]
        )

        price_delta = row["close"] - row["open"]

        records.append({
            "symbol": symbol,
            "date": date,
            "open": row["open"],
            "close": row["close"],
            "volume": row["volume"],
            "price_trend": "up" if row["close"] > row["open"] else "down",
            "sentiment_score": sentiment_score,
            "news_count": len(news_window),
            "price_delta": price_delta,
            "ingested_at": datetime.utcnow().isoformat()
        })

    df = pd.DataFrame(records)
    df.columns = [sanitize(col) for col in df.columns]
    df.reset_index(drop=True, inplace=True)
    return df

def inject_synthetic_sentiment(df, mode="random"):
    if mode == "positive":
        df["sentiment_score"] = 1
    elif mode == "negative":
        df["sentiment_score"] = -1
    elif mode == "random":
        df["sentiment_score"] = np.random.choice([-1, 0, 1], size=len(df))
    return df

def sanitize(name):
    name = str(name).strip().lower()
    return re.sub(r"[^\w]", "_", name)

def write_features(session, df, table_name="FEATURE_SET"):
    session.use_schema("RAW")
    session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()

    snow_df = session.create_dataframe(df)
    snow_df.write.mode("overwrite").save_as_table(table_name)

    print(f"✅ {len(df)} feature rows written to RAW.{table_name}")

if __name__ == "__main__":
    stock_df, news_df, session = fetch_dataframes()
    feature_df = fuzzy_join(stock_df, news_df)
    feature_df = inject_synthetic_sentiment(feature_df, mode="random")  # 🔧 Toggle mode here
    write_features(session, feature_df)

