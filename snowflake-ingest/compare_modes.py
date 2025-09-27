import pandas as pd
import matplotlib.pyplot as plt

def load_trades(mode):
    try:
        return pd.read_csv(f"trades_dashboard_{mode}.csv")
    except FileNotFoundError:
        print(f"⚠️ No trades found for mode: {mode}")
        return pd.DataFrame()

def summarize_trades(df, mode):
    if df.empty:
        return {
            "mode": mode,
            "trades": 0,
            "total_pnl": 0,
            "avg_hold": 0
        }
    return {
        "mode": mode,
        "trades": len(df),
        "total_pnl": round(df["pnl"].sum(), 2),
        "avg_hold": round(df["holding_days"].mean(), 2)
    }

def plot_trade_density(trade_data):
    plt.figure(figsize=(12, 6))
    for mode, df in trade_data.items():
        if df.empty:
            continue
        df["date"] = pd.to_datetime(df["date"])
        density = df["date"].dt.to_period("M").value_counts().sort_index()
        plt.plot(density.index.to_timestamp(), density.values, label=mode.capitalize())

    plt.title("Trade Density Over Time")
    plt.xlabel("Date")
    plt.ylabel("Number of Trades")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("trade_density.png")
    plt.close()

def plot_trigger_distribution(trade_data):
    trigger_counts = {}
    for mode, df in trade_data.items():
        if df.empty:
            continue
        counts = df["trigger_type"].value_counts()
        trigger_counts[mode] = counts

    trigger_df = pd.DataFrame(trigger_counts).fillna(0).astype(int)
    trigger_df.plot(kind="bar", figsize=(10, 6))
    plt.title("Trigger Type Distribution Across Modes")
    plt.xlabel("Trigger Type")
    plt.ylabel("Count")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("trigger_distribution.png")
    plt.close()

def plot_signal_vs_pnl(trade_data):
    plt.figure(figsize=(12, 6))
    for mode, df in trade_data.items():
        if df.empty:
            continue
        plt.scatter(df["entry_signal_strength"], df["pnl"], label=mode.capitalize(), alpha=0.7)

    plt.axhline(0, color="gray", linestyle="--")
    plt.title("Signal Strength vs P&L")
    plt.xlabel("Entry Signal Strength")
    plt.ylabel("P&L")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("signal_vs_pnl.png")
    plt.close()

def export_summary(summary_df):
    summary_df.to_csv("mode_comparison_summary.csv", index=False)
    print("📁 Summary exported to mode_comparison_summary.csv")

def compare_modes():
    modes = ["positive", "random", "negative"]
    trade_data = {mode: load_trades(mode) for mode in modes}
    summaries = [summarize_trades(df, mode) for mode, df in trade_data.items()]
    summary_df = pd.DataFrame(summaries)

    print("\n📋 Comparative Summary:")
    print(summary_df.to_string(index=False))

    plot_trade_density(trade_data)
    plot_trigger_distribution(trade_data)
    plot_signal_vs_pnl(trade_data)
    export_summary(summary_df)

if __name__ == "__main__":
    compare_modes()

