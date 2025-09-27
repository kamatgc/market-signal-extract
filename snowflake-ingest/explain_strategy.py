from config import get_snowflake_session
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import matplotlib.pyplot as plt

load_dotenv()

def fetch_data():
    session = get_snowflake_session()
    session.use_schema("RAW")

    features = session.table("FEATURE_SET").to_pandas()
    features["date"] = pd.to_datetime(features["date"])
    features.sort_values("date", inplace=True)
    features["price_threshold"] = features["price_delta"].rolling(window=10, min_periods=1).mean()

    try:
        trades = session.table("BACKTEST_RESULTS").to_pandas()
        trades["date"] = pd.to_datetime(trades["date"], errors="coerce")
        trades["entry_date"] = pd.to_datetime(trades["entry_date"], errors="coerce")
        trades.sort_values("date", inplace=True)
    except Exception:
        print("⚠️ BACKTEST_RESULTS table not found. Proceeding with features only.")
        trades = pd.DataFrame()

    return features, trades

def summarize(features, trades):
    print("\n📊 Strategy Summary:")
    print(f"Total signals scanned: {len(features)}")
    print(f"Trades executed: {len(trades)}")
    print(f"Skipped signals: {len(features) - len(trades)}")

    if not trades.empty:
        avg_hold = trades["holding_days"].mean()
        final_capital = trades["capital"].iloc[-1]
        print(f"Average holding duration: {avg_hold:.2f} days")
        print(f"Final capital: ₹{final_capital:,.2f}")
    else:
        print("No trades executed—capital unchanged.")

def narrate(features, trades):
    if trades.empty:
        print("\n🗣️ No trade narratives available—no trades executed.")
    else:
        print("\n🗣️ Trade Narratives:")
        for _, row in trades.iterrows():
            print(
                f"✅ {row['date'].date()}: Exited after {row['holding_days']} days "
                f"with ₹{row['pnl']:.2f} P&L. Confidence: {row['confidence']} "
                f"via {row['trigger_type'].upper()} (Signal Strength: {row['entry_signal_strength']})"
            )

    print("\n⚠️ Skipped Signals:")
    for _, row in features.iterrows():
        if row["sentiment_score"] == 0 and row["price_delta"] < row["price_threshold"]:
            print(f"Skipped {row['date'].date()} — Neutral sentiment and weak price delta ({row['price_delta']:.2f})")

def plot_thresholds(features):
    plt.figure(figsize=(12, 6))
    plt.plot(features["date"], features["sentiment_score"], label="Sentiment Score", color="blue")
    plt.plot(features["date"], features["sentiment_score"].rolling(window=10, min_periods=1).mean(),
             label="Sentiment Threshold", color="orange", linestyle="--")
    plt.title("Sentiment Score vs Threshold")
    plt.xlabel("Date")
    plt.ylabel("Score")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(12, 6))
    plt.plot(features["date"], features["price_delta"], label="Price Δ", color="green")
    plt.plot(features["date"], features["price_threshold"], label="Price Threshold", color="red", linestyle="--")
    plt.title("Price Delta vs Threshold")
    plt.xlabel("Date")
    plt.ylabel("Price Movement")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

def export_dashboard_data(features, trades):
    features.to_csv("features_dashboard.csv", index=False)
    if not trades.empty:
        trades.to_csv("trades_dashboard.csv", index=False)

        trigger_counts = trades["trigger_type"].value_counts().reset_index()
        trigger_counts.columns = ["trigger_type", "count"]
        trigger_counts.to_csv("trigger_distribution.csv", index=False)

        trigger_pnl = trades.groupby("trigger_type")["pnl"].sum().reset_index()
        trigger_pnl.columns = ["trigger_type", "total_pnl"]
        trigger_pnl.to_csv("trigger_pnl.csv", index=False)

    print("📁 Dashboard data exported.")

if __name__ == "__main__":
    features, trades = fetch_data()
    summarize(features, trades)
    narrate(features, trades)
    plot_thresholds(features)
    export_dashboard_data(features, trades)

