FROM python:3.10-slim

# Prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System dependencies for confluent-kafka / librdkafka
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install python-dotenv==1.1.0 polygon-api-client==1.14.5 confluent-kafka[schema-registry]==2.10.1 snowflake-connector-python==3.15.0 fastavro==1.12.0 avro==1.12.0

# Copy only the streaming directory
COPY streaming/ ./streaming/

# Run the producer
CMD ["python", "streaming/stock_aggregates_stream_producer.py"]