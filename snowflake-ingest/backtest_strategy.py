from config import get_snowflake_session
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import re

load_dotenv()

# 🔧 Strategy tuning parameters
SENTIMENT_MODE = "negative"  # options: "positive", "negative", "random"
MIN_SIGNAL_STRENGTH = 1.2  # suppress weak entries
FALLBACK_SELL_THRESHOLD = -1.2  # price drop multiplier
MOMENTUM_BUY_THRESHOLD = 1.5    # strong upward delta
MOMENTUM_SELL_THRESHOLD = -1.5  # strong downward delta

def fetch_feature_set():
    session = get_snowflake_session()
    session.use_schema("RAW")

    df = session.table("FEATURE_SET").to_pandas()
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values("date", inplace=True)

    # Inject synthetic sentiment
    if SENTIMENT_MODE == "positive":
        df["sentiment_score"] = 1
    elif SENTIMENT_MODE == "negative":
        df["sentiment_score"] = -1
    elif SENTIMENT_MODE == "random":
        df["sentiment_score"] = np.random.choice([-1, 0, 1], size=len(df))

    df["sentiment_threshold"] = df["sentiment_score"].rolling(window=10, min_periods=1).mean()
    df["price_threshold"] = df["price_delta"].rolling(window=10, min_periods=1).mean()
    return df, session

def simulate_strategy(df):
    trades = []
    capital = 100000
    position = 0
    entry_date = None
    trigger_type = None
    signal_strength = 0
    fallback_triggered = 0
    momentum_triggered = 0
    primary_triggered = 0
    fallback_sell_triggered = 0
    momentum_sell_triggered = 0
    momentum_buy_triggered = 0

    for _, row in df.iterrows():
        signal = None
        confidence = 0
        signal_strength = round(row["price_delta"] / row["price_threshold"], 2)

        if signal_strength < MIN_SIGNAL_STRENGTH:
            continue  # suppress weak entries

        # BUY logic
        if row["sentiment_score"] > row["sentiment_threshold"] and row["price_delta"] > row["price_threshold"]:
            signal = "BUY"
            trigger_type = "primary"
            position = row["close"]
            entry_date = row["date"]
            confidence = (row["sentiment_score"] - row["sentiment_threshold"]) + (row["price_delta"] - row["price_threshold"])
            primary_triggered += 1
            print(f"📌 BUY triggered on {row['date'].date()} via PRIMARY")

        elif row["sentiment_score"] >= 0 and row["price_delta"] >= 0.8 * row["price_threshold"]:
            signal = "BUY"
            trigger_type = "fallback"
            position = row["close"]
            entry_date = row["date"]
            confidence = (row["price_delta"] / row["price_threshold"]) * 0.5
            fallback_triggered += 1
            print(f"📌 BUY triggered on {row['date'].date()} via FALLBACK")

        elif row["price_delta"] >= MOMENTUM_BUY_THRESHOLD * row["price_threshold"]:
            signal = "BUY"
            trigger_type = "momentum"
            position = row["close"]
            entry_date = row["date"]
            confidence = row["price_delta"]
            momentum_buy_triggered += 1
            print(f"📌 BUY triggered on {row['date'].date()} via MOMENTUM")

        # SELL logic
        elif row["sentiment_score"] < 0 and row["price_trend"] == "down":
            signal = "SELL"

        elif row["price_delta"] < FALLBACK_SELL_THRESHOLD * row["price_threshold"]:
            signal = "SELL (fallback)"
            fallback_sell_triggered += 1

        elif row["price_delta"] < MOMENTUM_SELL_THRESHOLD * row["price_threshold"]:
            signal = "SELL (momentum)"
            momentum_sell_triggered += 1

        if signal and "SELL" in signal and position:
            pnl = row["close"] - position
            holding_days = (row["date"] - entry_date).days
            capital += pnl
            trades.append({
                "entry_date": entry_date,
                "date": row["date"],
                "entry": position,
                "exit": row["close"],
                "pnl": pnl,
                "capital": capital,
                "holding_days": holding_days,
                "signal": signal,
                "confidence": round(confidence, 2),
                "trigger_type": trigger_type,
                "entry_signal_strength": signal_strength
            })
            position = 0
            entry_date = None
            trigger_type = None
            signal_strength = 0

    print(f"\n📊 Primary triggers used: {primary_triggered}")
    print(f"📊 Fallback triggers used: {fallback_triggered}")
    print(f"📊 Momentum BUYs used: {momentum_buy_triggered}")
    print(f"📊 Fallback SELLs used: {fallback_sell_triggered}")
    print(f"📊 Momentum SELLs used: {momentum_sell_triggered}")
    print(f"📊 Trades executed: {len(trades)}")
    return pd.DataFrame(trades)

def sanitize(name):
    name = str(name).strip().lower()
    return re.sub(r"[^\w]", "_", name)

def write_backtest_results(session, df, table_name="BACKTEST_RESULTS"):
    session.use_schema("RAW")
    session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()

    if df.empty:
        print("⚠️ No trades generated. Skipping write.")
        return

    df.columns = [sanitize(col) for col in df.columns]
    df.reset_index(drop=True, inplace=True)

    snow_df = session.create_dataframe(df)
    snow_df.write.mode("overwrite").save_as_table(table_name)

    print(f"✅ {len(df)} trades written to RAW.{table_name}")

if __name__ == "__main__":
    feature_df, session = fetch_feature_set()
    trades_df = simulate_strategy(feature_df)
    write_backtest_results(session, trades_df)

