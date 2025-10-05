import pandas as pd
import yfinance as yf

def fetch_price_data(symbol, start_date, end_date):
    df = yf.download(symbol, start=start_date, end=end_date, auto_adjust=False)
    df = df.rename(columns={"Open": "open", "Close": "close"})
    df = df[["open", "close"]]
    df.index = pd.to_datetime(df.index)
    return df

