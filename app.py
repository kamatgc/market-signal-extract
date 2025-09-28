import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
import matplotlib.pyplot as plt

load_dotenv()

# 🔐 Load Snowflake credentials
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# ✅ Validate credentials
required_vars = [
    SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
]
if not all(required_vars):
    st.error("❌ Missing Snowflake credentials. Please check your secrets configuration.")
    st.stop()

# 📡 Load BACKTEST_RESULTS from Snowflake
@st.cache_data
def load_snowflake_data():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM BACKTEST_RESULTS")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    conn.close()
    return df

# 📁 Load summary CSV
@st.cache_data
def load_summary():
    df = pd.read_csv("mode_comparison_summary.csv")
    df.columns = [col.strip().lower() for col in df.columns]
    return df

# 📊 Load data
df = load_snowflake_data()
summary_df = load_summary()

# 🎨 Sidebar filters
st.sidebar.title("🔍 Filter Trades")
mode_filter = st.sidebar.selectbox("Sentiment Mode", ["positive", "random", "negative"])
trigger_filter = st.sidebar.multiselect("Trigger Type", ["primary", "fallback", "momentum"], default=["primary", "fallback", "momentum"])

# 🧠 Symbol filter (only if column exists)
if "symbol" in df.columns:
    available_symbols = sorted(df["symbol"].dropna().unique())
    symbol_filter = st.sidebar.selectbox("Symbol", available_symbols)
    df = df[df["symbol"] == symbol_filter]
    st.markdown(f"**Symbol:** `{symbol_filter}`")
else:
    symbol_filter = None
    st.warning("⚠️ Symbol column not found in data. Showing all trades.")

# 🧼 Filter data
filtered_df = df[df["trigger_type"].str.lower().isin(trigger_filter)]
filtered_summary = summary_df[summary_df["mode"] == mode_filter]

# 📋 Dashboard Title
st.title(f"📈 Strategy Dashboard — Mode: {mode_filter.capitalize()}")

# 📊 Summary metrics
st.metric("Total Trades", len(filtered_df))
st.metric("Total P&L", round(filtered_df["pnl"].sum(), 2))
st.metric("Avg Holding Duration (days)", round(filtered_df["holding_days"].mean(), 2))

# 🚨 Anomaly Count
if "anomaly_flag" in filtered_df.columns:
    st.metric("Anomalies", int(filtered_df["anomaly_flag"].sum()))

# 📌 Latest Signal Display
if not filtered_df.empty and "signal_type" in filtered_df.columns:
    latest = filtered_df.sort_values("entry_date", ascending=False).iloc[0]
    st.subheader(f"📌 Latest Signal")
    st.write(f"**Action:** `{latest['signal_type']}`")
    st.write(f"**Date:** `{latest['entry_date']}`")
    st.write(f"**Signal Strength:** `{latest['entry_signal_strength']}`")

# 📋 Trade Details Table
st.subheader("📋 Trade Details")

if not filtered_df.empty:
    trade_df = filtered_df.copy()

    # Define expected columns and friendly names
    display_map = {
        "symbol": "Symbol",
        "entry_date": "Buy Date",
        "buy_price": "Buy Price",
        "exit_date": "Sell Date",
        "sell_price": "Sell Price",
        "signal_type": "Signal Type",
        "entry_signal_strength": "Signal Strength"
    }

    # Detect available columns
    available_cols = [col for col in display_map if col in trade_df.columns]
    missing_cols = [col for col in display_map if col not in trade_df.columns]

    # Compute P&L if possible
    if "buy_price" in trade_df.columns and "sell_price" in trade_df.columns:
        trade_df["Absolute P&L"] = trade_df["sell_price"] - trade_df["buy_price"]
        trade_df["% P&L"] = ((trade_df["sell_price"] - trade_df["buy_price"]) / trade_df["buy_price"]) * 100
        available_cols += ["Absolute P&L", "% P&L"]

    # Rename columns
    rename_map = {col: display_map[col] for col in available_cols if col in display_map}
    trade_df.rename(columns=rename_map, inplace=True)

    # Show missing column info
    if missing_cols:
        st.info(f"ℹ️ Some trade details could not be shown due to missing columns: {', '.join(missing_cols)}")

    # Display table safely
    safe_cols = [col for col in available_cols if col in trade_df.columns]

    if "Absolute P&L" in trade_df.columns:
        def highlight_pnl(row):
            color = "#d4f4dd" if row["Absolute P&L"] > 0 else "#fddddd"
            return [f"background-color: {color}"] * len(row)
        styled_df = trade_df[safe_cols].style.apply(highlight_pnl, axis=1)
        st.dataframe(styled_df, use_container_width=True)
    else:
        st.dataframe(trade_df[safe_cols], use_container_width=True)

    # Download button
    st.download_button("Download Trade Details", trade_df[safe_cols].to_csv(index=False), file_name="trade_details.csv")
else:
    st.warning("⚠️ No trades found for the selected filters.")

# 📊 Trigger Distribution
st.subheader("Trigger Type Distribution")
trigger_counts = filtered_df["trigger_type"].value_counts()
st.bar_chart(trigger_counts)

# 🎯 Signal Strength vs P&L
st.subheader("Signal Strength vs P&L")
fig, ax = plt.subplots()
ax.scatter(filtered_df["entry_signal_strength"], filtered_df["pnl"], alpha=0.7)
ax.axhline(0, color="gray", linestyle="--")
ax.set_xlabel("Signal Strength")
ax.set_ylabel("P&L")
st.pyplot(fig)

# 📁 Download summary
st.subheader("📁 Download Summary")
st.download_button("Download Summary", filtered_summary.to_csv(index=False), file_name="mode_comparison_summary_filtered.csv")

