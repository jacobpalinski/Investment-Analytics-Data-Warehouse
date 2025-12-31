#!/bin/bash
set -euo pipefail

exec > >(tee /var/log/user-data.log | logger -t user-data) 2>&1

# Install requirments.txt packages
sudo pip install -r requirements.txt

# Create .env file with environment variables from AWS SSM Parameter Store
cat <<EOF > .env
AWS_ACCESS_KEY_ID=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/ACCESS_KEY_ID --with-decryption --query Parameter.Value --output text)
AWS_SECRET_ACCESS_KEY=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/SECRET --with-decryption --query Parameter.Value --output text)
AWS_S3_BUCKET=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/S3_BUCKET --with-decryption --query Parameter.Value --output text)
AWS_S3_TST_BUCKET=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/S3_TST_BUCKET --with-decryption --query Parameter.Value --output text)
AIRFLOW_UID=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/AIRFLOW_UID --with-decryption --query Parameter.Value --output text)
AIRFLOW_USERNAME=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/AIRFLOW_USERNAME --with-decryption --query Parameter.Value --output text)
AIRFLOW_PASSWORD=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/AIRFLOW_PASSWORD --with-decryption --query Parameter.Value --output text)
AIRFLOW_FERNET_KEY=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/AIRFLOW_FERNET_KEY --with-decryption --query Parameter.Value --output text)
AIRFLOW_EMAIL=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/AIRFLOW_EMAIL --with-decryption --query Parameter.Value --output text)
POLYGON_API_KEY=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/POLYGON_API_KEY --with-decryption --query Parameter.Value --output text)
FINNHUB_API_KEY=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/FINNHUB_API_KEY --with-decryption --query Parameter.Value --output text)
NEWS_API_KEY=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/NEWS_API_KEY --with-decryption --query Parameter.Value --output text)
SNOWFLAKE_USER=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/SNOWFLAKE_USER --with-decryption --query Parameter.Value --output text)
SNOWFLAKE_PASSWORD=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/SNOWFLAKE_PASSWORD --with-decryption --query Parameter.Value --output text)
SNOWFLAKE_ACCOUNT=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/SNOWFLAKE_ACCOUNT --with-decryption --query Parameter.Value --output text)
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/SNOWFLAKE_PRIVATE_KEY_PASSPHRASE --with-decryption --query Parameter.Value --output text)
SNOWFLAKE_PRIVATE_KEY_B64=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/SNOWFLAKE_PRIVATE_KEY_B64 --with-decryption --query Parameter.Value --output text)
KAFKA_BOOTSTRAP_SERVERS=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/KAFKA_BOOTSTRAP_SERVERS --with-decryption --query Parameter.Value --output text)
KAFKA_TOPIC=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/KAFKA_TOPIC --with-decryption --query Parameter.Value --output text)
SCHEMA_REGISTRY_URL=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/SCHEMA_REGISTRY_URL --with-decryption --query Parameter.Value --output text)
POSTGRES_USERNAME=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/POSTGRES_USERNAME --with-decryption --query Parameter.Value --output text)
POSTGRES_PASSWORD=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/POSTGRES_PASSWORD --with-decryption --query Parameter.Value --output text)
DOMAIN_NAME=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/DOMAIN_NAME --with-decryption --query Parameter.Value --output text)
CERTBOT_EMAIL=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/CERTBOT_EMAIL --with-decryption --query Parameter.Value --output text)
EOF

# Load environment variables from .env file
source .env

# Export base64 encoded environment variables
export METABASE_PRIVATE_KEY=$(aws ssm get-parameter --name /investment_analytics_data_warehouse/prd/METABASE_PRIVATE_KEY --with-decryption --query Parameter.Value --output text)

# Create Metabase private key file
echo "$METABASE_PRIVATE_KEY" > private_key_metabase.p8
chmod 600 private_key_metabase.p8

# Run Airflow docker containers
sudo docker compose run --rm airflow-init
sudo docker compose up -d postgres redis airflow-apiserver airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer

# Wait until containers are up and running
sleep 180

# Create Snowflake connection in Airflow
sudo docker exec investment-analytics-data-warehouse-airflow-scheduler-1 \
  airflow connections add snowflake_default \
  --conn-type snowflake \
  --conn-login "$SNOWFLAKE_USER" \
  --conn-password "$SNOWFLAKE_PRIVATE_KEY_PASSPHRASE" \
  --conn-extra "{\"database\":\"INVESTMENT_ANALYTICS\",\"warehouse\":\"INVESTMENT_ANALYTICS_DWH\", \"private_key_content\":\"$SNOWFLAKE_PRIVATE_KEY_B64\"}"

# Create AWS connection in Airflow
sudo docker exec investment-analytics-data-warehouse-airflow-scheduler-1 \
  airflow connections add aws_default \
  --conn-type aws \
  --conn-login "$AWS_ACCESS_KEY_ID" \
  --conn-password "$AWS_SECRET_ACCESS_KEY"

# Run Kafka docker containers
sudo docker compose up -d zookeeper-1 zookeeper-2 zookeeper-3 kafka-1 kafka-2 kafka-3 schema-registry kafka-connect

# Wait until containers are up and running
sleep 180

# Create Kafka topic
sudo docker exec investment-analytics-data-warehouse-kafka-1-1 kafka-topics --bootstrap-server kafka-1:9092 --create --topic stock_aggregates_raw --partitions 1 --replication-factor 3

# Create Kafka Snowflake connector
cd streaming
curl -X POST -H "Content-Type: application/json" --data @connector.json http://localhost:8083/connectors

# Run Kafka stock_aggregates_stream_producer.py script
nohup python3 stock_aggregates_stream_producer.py > stream_output.log 2>&1 & # Process can be stopped later using 'pkill -f stock_aggregates_stream_producer.py'

# Go back to main directory
cd ..

# Create metabase database in postgres container
sudo docker exec investment-analytics-data-warehouse-postgres-1 psql -U ${POSTGRES_USERNAME} -d postgres -c "CREATE DATABASE metabase;"

# Restore metabase database from local dump file
sudo docker exec -i investment-analytics-data-warehouse-postgres-1 psql -U ${POSTGRES_USERNAME} -d metabase < metabase_dump.sql

# Launch metabase container
sudo docker compose up -d metabase

# Print completion message
echo "Airflow, Kafka and Metabase services have been started successfully."


