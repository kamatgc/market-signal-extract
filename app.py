import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import os
from dotenv import load_dotenv

# 🔄 Load environment variables from .env
load_dotenv()

# ❄️ Load data from Snowflake
@st.cache_data
def load_data():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cur = conn.cursor()
    cur.execute("SELECT * FROM BACKTEST_RESULTS")
    df = cur.fetch_pandas_all()
    cur.close()
    conn.close()
    return df

@st.cache_data
def load_summary():
    return pd.read_csv("mode_comparison_summary.csv")

df = load_data()
summary_df = load_summary()

# 🎨 Sidebar filters
st.sidebar.title("Filter Trades")
mode_filter = st.sidebar.selectbox("Sentiment Mode", ["positive", "random", "negative"])

trigger_options = sorted(df["trigger_type"].dropna().unique())
selected_triggers = st.sidebar.multiselect("Trigger Type", trigger_options, default=trigger_options)

symbol_options = sorted(df["SYMBOL"].dropna().unique()) if "SYMBOL" in df.columns else []
selected_symbols = st.sidebar.multiselect("Symbol", symbol_options, default=symbol_options)

# 🧼 Apply filters
filtered_df = df[
    (df["trigger_type"].isin(selected_triggers)) &
    (df["SYMBOL"].isin(selected_symbols))
]

# 📊 Summary metrics
filtered_summary = summary_df[summary_df["mode"] == mode_filter]
st.title(f"Strategy Dashboard – Mode: {mode_filter.capitalize()}")
st.subheader("Summary Metrics")
col1, col2, col3 = st.columns(3)
col1.metric("Total Trades", len(filtered_df))
col2.metric("Net PnL", round(filtered_df["pnl"].sum(), 2))
col3.metric("Avg Holding Duration", round(filtered_df["holding_days"].mean(), 2))

# 📋 Trade table
st.subheader("Trade Details")
expected_cols = ["SYMBOL", "entry_date", "entry", "signal", "exit_date", "exit", "capital", "holding_days", "pnl", "trigger_type"]
available_cols = [col for col in expected_cols if col in filtered_df.columns]
styled_df = filtered_df[available_cols].rename(columns={"SYMBOL": "Symbol"}).style.format("{:.2f}", subset=["entry", "exit", "capital", "pnl"])
styled_df = styled_df.set_table_styles([{"selector": "th", "props": [("font-weight", "bold")]}])
st.dataframe(styled_df, use_container_width=True)

# 📥 Download button
csv = filtered_df[available_cols].to_csv(index=False).encode("utf-8")
st.download_button("Download Trade Details", csv, "filtered_trades.csv", "text/csv")

# 📊 Trigger distribution
st.subheader("Trigger Type Distribution")
trigger_counts = filtered_df["trigger_type"].value_counts()
st.bar_chart(trigger_counts)

# 📈 Signal Strength vs P&L
if "signal_strength" in filtered_df.columns:
    st.subheader("Signal Strength vs P&L")
    fig = px.scatter(filtered_df, x="signal_strength", y="pnl", color="trigger_type", title="Signal Strength vs P&L")
    st.plotly_chart(fig, use_container_width=True)

# 📊 Symbol distribution
if "SYMBOL" in filtered_df.columns:
    st.subheader("Symbol Distribution")
    symbol_counts = filtered_df["SYMBOL"].value_counts()
    st.bar_chart(symbol_counts)

# 🟢 Latest signal
if "signal" in filtered_df.columns and not filtered_df["signal"].isnull().all():
    latest_signal = filtered_df.iloc[-1]["signal"]
    st.subheader("Latest Signal")
    st.write(f"**{latest_signal}**")

