# Import necessary libraries
import os
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from dags.nasdaq_listed_tickers_dag.extract_listed_tickers import extract_listed_tickers
from dags.utils.s3_utils import SNSNotifier
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup SNS notifier class
sns_notifier = SNSNotifier(access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'), secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'), 
                           region=os.environ.get('AWS_REGION'), topic_arn=os.environ.get('SNS_TOPIC_ARN'))

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1, 4, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'on_failure_callback': sns_notifier
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