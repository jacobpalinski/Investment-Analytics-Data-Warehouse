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
df = pd.read_csv("nasdaq_listed_symbols_20250528.csv")
tickers = df['Symbol'].dropna().tolist()

# Prefix each ticker with "AM." to subscribe to per-minute aggregates
tickers = [f"AM.{t}" for t in tickers]

# File to store output
output_file = "stock_aggregates.csv"
fieldnames = ["ticker_symbol", "timestamp", "volume", "accumulated_volume", "volume_weighted_average_price", "closing_tick_price", "average_trade_size"]

# Set up timer
start_time = time.time()
run_duration = 30 * 60  # 30 minutes

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
			"ticker_symbol": m.sym, # Ticker symbol
			"timestamp": m.e, # End timestamp of aggregate window
			"volume": m.v, # Ticker volume
            "accumulated_volume": m.av, # Accumulated volume
            "volume_weighted_average_price": m.vw, # Volume weighted average price
            "closing_tick_price": m.c, # Closing tick price
            "average_trade_size": m.z # Average trade size
			})
			print(f"Saved: {m.ticker} @ {m.timestamp}")

        # Stop after 10 minutes
        if time.time() - start_time > run_duration:
            print("30 minutes reached. Closing WebSocket.")
            ws_client.close_connection()

    # Run WebSocket with handler
    ws_client.run(handle_msg)

