import pandas as pd
from sentiment_engine import fetch_articles
from config import THRESHOLDS

def generate_trades(symbol, company_name, price_df):
    if price_df.empty:
        return pd.DataFrame()

    dates = price_df.index.tolist()
    trades = []
    i = 0
    while i < len(dates) - 1:
        entry_date = dates[i]
        entry_price = float(price_df.loc[entry_date, "open"])
        sentiment_score, article_title, article_url = fetch_articles(symbol, entry_date)

        exit_index = i + 1
        while exit_index < len(dates):
            exit_date = dates[exit_index]
            exit_price = float(price_df.loc[exit_date, "close"])
            momentum_score = (exit_price - entry_price) / entry_price

            # Volatility exit: price moves > 2%
            if abs(momentum_score) >= 0.02:
                break

            # Signal exit: sentiment flips or momentum reverses
            if (momentum_score > 0 and sentiment_score < 0) or (momentum_score < 0 and sentiment_score > 0):
                break

            exit_index += 1

        if exit_index >= len(dates):
            break

        exit_date = dates[exit_index]
        exit_price = float(price_df.loc[exit_date, "close"])
        quantity = 1
        capital = entry_price * quantity
        pnl = (exit_price - entry_price) * quantity
        holding_days = (exit_date - entry_date).days
        momentum_score = (exit_price - entry_price) / entry_price
        momentum_pct = round(momentum_score * 100, 2)
        sentiment_pct = round(sentiment_score * 100, 2)

        if momentum_score > THRESHOLDS["momentum_buy"] and sentiment_score >= THRESHOLDS["sentiment_buy"]:
            final_signal = "BUY"
            reason = "Signal passed"
        elif momentum_score < THRESHOLDS["momentum_sell"] and sentiment_score <= THRESHOLDS["sentiment_sell"]:
            final_signal = "SELL"
            reason = "Signal passed"
        else:
            final_signal = "HOLD"
            reason = "Signal suppressed"

        print(f"[DEBUG] {symbol} | {entry_date.date()} → {final_signal} | Momentum: {momentum_pct}% | Sentiment: {sentiment_pct}% | Reason: {reason}")

        trades.append({
            "symbol": symbol,
            "entry_date": entry_date,
            "exit_date": exit_date,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "quantity": quantity,
            "capital": capital,
            "pnl": pnl,
            "holding_days": holding_days,
            "momentum_score": momentum_pct,
            "sentiment_score": sentiment_pct,
            "final_signal": final_signal,
            "article_title": article_title,
            "article_url": article_url,
            "reason": reason
        })

        i = exit_index + 1

    df = pd.DataFrame(trades)
    return df[df["final_signal"].isin(["BUY", "SELL"])]

