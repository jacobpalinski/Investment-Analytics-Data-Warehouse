# Import necessary libraries
import os
import time
import json
import pandas as pd
from typing import List
from dotenv import load_dotenv
from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from confluent_kafka import Producer

# Load environment variables
load_dotenv()
polygon_api_key = os.getenv("POLYGON_API_KEY")

# Load ticker symbols
df = pd.read_csv("nasdaq_listed_symbols_20250528.csv")
tickers = [f"AM.{t}" for t in df['Symbol'].dropna().tolist()]

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
KAFKA_TOPIC = "stock_aggregates"

# Kafka producer setup
producer = Producer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'acks': 'all',  # Ensure durability
    'linger.ms': 10  # Small buffer for batching
})

# Timer setup
start_time = time.time()
run_duration = 30 * 60  # 30 minutes

# WebSocket client setup
ws_client = WebSocketClient(
    api_key=polygon_api_key,
    feed=Feed.Delayed,
    market=Market.Stocks
)

# Subscribe to all tickers
ws_client.subscribe(*tickers)

# Handle messages
def handle_msg(msgs: List[WebSocketMessage]):
    for m in msgs:
        if not m.symbol:  # Skip malformed messages
            continue

        message = {
            "ticker_symbol": m.symbol,
            "timestamp": m.end_timestamp,
            "volume": m.volume,
            "accumulated_volume": m.accumulated_volume,
            "vwap": m.vwap,
            "close": m.close,
            "average_size": m.average_size
        }

        # Use ticker as Kafka message key
        key = m.symbol.encode("utf-8")
        value = json.dumps(message).encode("utf-8")

        try:
            producer.produce(topic=KAFKA_TOPIC, key=key, value=value)
            print(f"Produced: {m.symbol} @ {m.end_timestamp}")
        except Exception as e:
            print(f"Kafka produce error: {e}")

    # Flush periodically
    producer.poll(0)

# Run the WebSocket client
try:
    ws_client.run(handle_msg)
except KeyboardInterrupt:
    print("Manual interruption. Flushing Kafka producer and closing WebSocket...")
    producer.flush()
    ws_client.close()



