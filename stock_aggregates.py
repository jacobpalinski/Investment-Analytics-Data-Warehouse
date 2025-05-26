# Import modules
from typing import List
from polygon import RESTClient, WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market

# Create a stock client to retrieve ticker symbols
client = RESTClient("YOUR_API_KEY")

# Create a list of all stock tickers on Nasdaq
tickers = client.list_tickers(
	market=Market.Stocks,
	limit=1000  # Adjust the limit as needed
)

# Create a WebSocket client for Polygon.io to receive stock aggregates
client = WebSocketClient(
	api_key="YOUR_API_KEY",
	feed=Feed.Delayed,
	market=Market.Stocks
	)

# aggregates (per minute)
client.subscribe("AM.*") # single ticker
# client.subscribe("AM.*") # all tickers
# client.subscribe("AM.AAPL") # single ticker
# client.subscribe("AM.AAPL", "AM.MSFT") # multiple tickers

def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        print(m)

# print messages
client.run(handle_msg)
