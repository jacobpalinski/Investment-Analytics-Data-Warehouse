# Import necessary libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
from airflow.dags.company_dimension_dag.company_info_extraction import extract_company_info
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
}

# Set paths to SQL files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MERGE_SQL_PATH = os.path.join(BASE_DIR, 'sql', 'merge_company_information.sql')
UPDATE_DIM_COMPANY = os.path.join(BASE_DIR, 'sql', 'update_current_dim_company.sql')
INSERT_DIM_COMPANY = os.path.join(BASE_DIR, 'sql', 'insert_dim_company.sql')

# Define the DAG
with DAG('company_dimension_dag',
    default_args=default_args,
    description='DAG to create dim_company table in Snowflake',
    schedule_interval='@monthly',
    max_active_runs=1,
    tags=['company', 'dimension', 'snowflake']
):
    
    extraction = PythonOperator(
        task_id='extraction',
        python_callable='airflow.dags.company_dimension_dag.company_info_extraction.extract_company_info
    )

    merge_raw_company_information = SnowflakeOperator(
        task_id="merge_raw_company_information",
        sql=MERGE_SQL_PATH,
        snowflake_conn_id='snowflake_raw_connection'
    )

    update_current_dim_company = SnowflakeOperator(
        task_id="update_current_dim_company",
        sql=UPDATE_DIM_COMPANY,
        snowflake_conn_id='snowflake_analytics_connection'
    )

    insert_dim_company = SnowflakeOperator(
        task_id="insert_dim_company",
        sql=INSERT_DIM_COMPANY,
        snowflake_conn_id='snowflake_analytics_connection'
    )

    





    







