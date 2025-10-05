import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import datetime, timedelta
from config import COMPANY_NAMES
from data_loader import fetch_price_data
from trade_logic import generate_trades

st.title("📊 Strategy Dashboard – Modular Cockpit")
symbols = list(COMPANY_NAMES.keys())
today = datetime.today()
selected_symbol = st.sidebar.selectbox("Select Symbol", symbols)
start_date = st.sidebar.date_input("Start Date", datetime(2025, 1, 1), max_value=today - timedelta(days=1))
end_date = st.sidebar.date_input("End Date", today)

start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)

st.sidebar.markdown("### Signal Thresholds")
st.sidebar.write("📈 Momentum: > 0.5% = BUY, < –0.5% = SELL")
st.sidebar.write("🧠 Sentiment: > 5% = BUY, < –5% = SELL")

price_df = fetch_price_data(selected_symbol, start_date, end_date)
df = generate_trades(selected_symbol, COMPANY_NAMES[selected_symbol], price_df)

st.subheader(f"📌 Summary Metrics for {selected_symbol}")
st.metric("Total Trades", len(df))
st.metric("Net PnL", f"${df['pnl'].sum():.2f}" if not df.empty else "N/A")

if not df.empty:
    st.metric("Avg Holding Duration", f"{df['holding_days'].mean():.2f} days")
    st.metric("Latest Signal", df["final_signal"].iloc[-1])
else:
    st.metric("Avg Holding Duration", "N/A")
    st.metric("Latest Signal", "N/A")

st.subheader("📊 Visual Analysis")
if not df.empty:
    st.plotly_chart(px.histogram(df, x="final_signal", title="Signal Distribution"), use_container_width=True)
    st.plotly_chart(px.scatter(df, x="momentum_score", y="pnl", color="final_signal", title="Momentum vs PnL"), use_container_width=True)

st.subheader("📋 Trade Details")
if not df.empty:
    for i, row in df.iterrows():
        with st.expander(f"Trade {i+1} – {row['final_signal']}"):
            st.write(f"Symbol: {row['symbol']}")
            st.write(f"Entry Date: {row['entry_date'].date()} | Exit Date: {row['exit_date'].date()}")
            st.write(f"Entry Price: ${row['entry_price']:.2f} | Exit Price: ${row['exit_price']:.2f}")
            st.write(f"Capital: ${row['capital']:.2f} | PnL: ${row['pnl']:.2f}")
            st.write(f"Holding Days: {row['holding_days']} days")
            st.write(f"Momentum Score: {row['momentum_score']:.2f}%")
            st.write(f"Sentiment Score: {row['sentiment_score']:.2f}%")
            st.write(f"Source Article: [{row['article_title']}]({row['article_url']})")
            st.write(f"Audit Reason: {row['reason']}")
else:
    st.warning("No BUY or SELL trades found for the selected symbol and date range.")

