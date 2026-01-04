# Import necessary libraries
import os
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dags.nasdaq_listed_tickers.extract_listed_tickers import extract_listed_tickers

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1, 4, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email': os.getenv("AIRFLOW_EMAIL"),
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': False,
}

# Define the DAG
with DAG(dag_id='nasdaq_listed_tickers_dag',
    default_args=default_args,
    description='DAG to extract nasdaq listed tickers and upload to S3',
    schedule='0 4 1 * *',
    max_active_runs=1,
    tags=['extract_tickers', 'script']
):
    
    extraction = PythonOperator(
        task_id='extraction',
        python_callable=extract_listed_tickers
    )