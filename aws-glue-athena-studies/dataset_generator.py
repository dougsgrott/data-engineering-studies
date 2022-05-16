import time
import pandas as pd
import yfinance as yf
import os
import sys

def on_premise_ingestion(ticker_list):
    """Ingest data on-premise from yfinance data sources
    :param ticker_list: list of strings
    """

    financial_data = []
    for ticker in ticker_list:
        df = pd.DataFrame()
        data = yf.Ticker(ticker).history(period="max", interval="1d")
        data["Ticker"] = ticker
        financial_data.append(data)
        df = pd.concat([df, data], axis=0)
        print(f"Ingested data from ticker '{ticker}'")
        time.sleep(3)
    stacked_data = pd.concat(financial_data, axis=0)
    directory = os.path.join(sys.path[0], "data")
    os.makedirs(directory, exist_ok=True)
    stacked_data.to_csv(f"{directory}/financial_data.csv")
    return stacked_data

if __name__ == "__main__":
    tickers = ["AMZN", "AAPL", "TSLA", "GOOG", "NFLX"]
    on_premise_ingestion(tickers)
