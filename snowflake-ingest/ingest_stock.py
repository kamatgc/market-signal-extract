from config import get_snowflake_session
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

ALPHA_VANTAGE_API_KEY = "GYU01LI0918H1AYZ"
ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"

def fetch_stock_data(symbol):
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    response = requests.get(ALPHA_VANTAGE_URL, params=params)
    if response.status_code != 200:
        raise Exception(f"Alpha Vantage error: {response.status_code}")

    data = response.json().get("Time Series (Daily)", {})
    if not data:
        raise Exception("No stock data returned")

    records = []
    for date, values in data.items():
        records.append({
            "symbol": symbol,
            "date": date,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"]),
            "ingested_at": datetime.utcnow().isoformat()
        })

    return pd.DataFrame(records)

def write_to_snowflake(df, table_name="STOCK_DATA"):
    session = get_snowflake_session()
    conn = session._conn._conn  # Get raw connector from Snowpark session

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name,
        database=session.get_current_database(),
        schema=session.get_current_schema(),
        overwrite=True
    )

    if success:
        print(f"✅ {nrows} rows written to {table_name}")
    else:
        raise Exception("❌ Failed to write to Snowflake")

if __name__ == "__main__":
    symbol = "IBM"  # You can change this or make it dynamic
    df = fetch_stock_data(symbol)
    write_to_snowflake(df)

