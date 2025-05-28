# Import modules
import os
import time
import csv
from typing import List
import pandas as pd
from dotenv import load_dotenv
from polygon import RESTClient, WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market

# Load environment variables
load_dotenv()

# Access API key from environment variables
polygon_api_key = os.getenv("POLYGON_API_KEY")

# Load ticker symbols from CSV
df = pd.read_csv("nasdaq_listed_symbols.csv")
tickers = df['Symbol'].dropna().tolist()

# Prefix each ticker with "AM." to subscribe to per-minute aggregates
tickers = [f"AM.{t}" for t in tickers]

# File to store output
output_file = "candlesticks.csv"
fieldnames = ["ticker", "timestamp", "open", "high", "low", "close", "volume", "vwap", "transactions"]

# Set up timer
start_time = time.time()
run_duration = 10 * 60  # 10 minutes

# Open CSV file
with open(output_file, mode="w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()

    # Create WebSocket client
    ws_client = WebSocketClient(
        api_key=polygon_api_key,
        feed=Feed.Delayed,
        market=Market.Stocks
    )

    # Subscribe to tickers
    ws_client.subscribe(*tickers)

    # Define message handler
    def handle_msg(msgs: List[WebSocketMessage]):
        for m in msgs:
			writer.writerow({
			"t": m.sym, # Ticker symbol
			"e": m.e, # End timestamp of aggregate window
			"open": m.o,
			"high": m.high,
			"low": m.low,
			"close": m.close,
			"volume": m.volume,
			"vwap": m.vwap,
			"transactions": m.transactions,
			})
			print(f"Saved: {m.ticker} @ {m.timestamp}")

        # Stop after 10 minutes
        if time.time() - start_time > run_duration:
            print("10 minutes reached. Closing WebSocket.")
            ws_client.close_connection()

    # Run WebSocket with handler
    ws_client.run(handle_msg)

