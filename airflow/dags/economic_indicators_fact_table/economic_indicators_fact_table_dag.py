# Import necessary libraries
import os
from datetime import datetime, timedelta
from dags.utils.snowflake_utils import Snowflake
from dags.economic_indicators_fact_table.economic_indicator_extraction import extract_economic_indicators
from dags.data_quality_checks_outcomes import fail_if_data_quality_tests_failed
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# Instantiate Snowflake Client
snowflake_client = Snowflake(
    user=os.getenv("SNOWFLAKE_USER"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    private_key_encoded=os.getenv("SNOWFLAKE_PRIVATE_KEY_B64")
)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 2, 5, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email': os.getenv("AIRFLOW_EMAIL"),
    'email_on_failure': True,
    'email_on_retry': False,
    'email_on_success': False,
}

# Set paths to SQL files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INSERT_STAGING_ECONOMIC_INDICATORS_PATH = os.path.join(BASE_DIR, 'sql', 'insert_staging_economic_indicators.sql')
REMOVE_NULLS_PATH = os.path.join(BASE_DIR, 'sql', 'remove_nulls.sql')
INSERT_DIM_INDICATOR_DATE_PATH = os.path.join(BASE_DIR, 'sql', 'insert_dim_indicator_date.sql')
INSERT_FACT_ECONOMIC_INDICATORS_PATH = os.path.join(BASE_DIR, 'sql', 'insert_fact_economic_indicators.sql')
DATA_QUALITY_TESTS_STAGING_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_staging_economic_indicators.sql')
DATA_QUALITY_TESTS_DIMENSION_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_dim_indicator_date.sql')
DATA_QUALITY_TESTS_FACT_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_fact_economic_indicators.sql')
DATA_QUALITY_STAGING_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_staging_economic_indicators_fail.sql')
DATA_QUALITY_DIMENSION_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_dim_indicator_date_fail.sql')
DATA_QUALITY_FACT_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_fact_economic_indicators_fail.sql')

# Read SQL contents
INSERT_STAGING_ECONOMIC_INDICATORS = snowflake_client.read_sql_file(INSERT_STAGING_ECONOMIC_INDICATORS_PATH)
REMOVE_NULLS = snowflake_client.read_sql_file(REMOVE_NULLS_PATH)
INSERT_DIM_INDICATOR_DATE = snowflake_client.read_sql_file(INSERT_DIM_INDICATOR_DATE_PATH)
INSERT_FACT_ECONOMIC_INDICATORS = snowflake_client.read_sql_file(INSERT_FACT_ECONOMIC_INDICATORS_PATH)
DQ_STAGING_SQL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_STAGING_PATH)
DQ_DIM_SQL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_DIMENSION_PATH)
DQ_FACT_SQL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_FACT_PATH)
DQ_STAGING_FAIL = snowflake_client.read_sql_file(DATA_QUALITY_STAGING_FAIL_PATH)
DQ_DIM_FAIL = snowflake_client.read_sql_file(DATA_QUALITY_DIMENSION_FAIL_PATH)
DQ_FACT_FAIL = snowflake_client.read_sql_file(DATA_QUALITY_FACT_FAIL_PATH)

# Define the DAG
with DAG(dag_id='economic_indicators_fact_dag',
    default_args=default_args,
    description='DAG to create dim_economic_indicators table in Snowflake',
    schedule='0 5 * * *',
    max_active_runs=1,
    tags=['economic_indicators', 'dimension', 'snowflake']
):

    extraction = PythonOperator(
        task_id='extraction',
        python_callable=extract_economic_indicators
    )

    insert_staging_economic_indicators = SQLExecuteQueryOperator(
        task_id="insert_staging_economic_indicators",
        sql=INSERT_STAGING_ECONOMIC_INDICATORS,
        conn_id='snowflake_connection'
    )

    remove_nulls = SQLExecuteQueryOperator(
        task_id="remove_nulls",
        sql=REMOVE_NULLS,
        conn_id='snowflake_connection'
    )

    data_quality_tests_staging = SQLExecuteQueryOperator(
        task_id="data_quality_tests_staging",
        sql=DQ_STAGING_SQL,
        conn_id='snowflake_connection')
    
    data_quality_tests_staging_fail = PythonOperator(
        task_id="data_quality_tests_staging_fail",
        python_callable=fail_if_data_quality_tests_failed,
        op_kwargs={
            'sql_string': DQ_STAGING_FAIL,
            'schema': 'STAGING',
            'table_name': 'staging_economic_indicators'
        }
    )
    
    insert_dim_indicator_date = SQLExecuteQueryOperator(
        task_id="insert_dim_indicator_date",
        sql=INSERT_DIM_INDICATOR_DATE,
        conn_id='snowflake_connection'
    )

    insert_fact_economic_indicators = SQLExecuteQueryOperator(
        task_id="insert_fact_economic_indicators",
        sql=INSERT_FACT_ECONOMIC_INDICATORS,
        conn_id='snowflake_connection'
    )

    data_quality_tests_dimension = SQLExecuteQueryOperator(
        task_id="data_quality_tests_dimension",
        sql=DQ_DIM_SQL,
        conn_id='snowflake_connection'
    )

    data_quality_tests_dimension_fail = PythonOperator(
        task_id="data_quality_tests_dimension_fail",
        python_callable=fail_if_data_quality_tests_failed,
        op_kwargs={
            'sql_string': DQ_DIM_FAIL,
            'schema': 'ANALYTICS',
            'table_name': 'dim_economic_indicators'
        }
    )

    data_quality_tests_fact = SQLExecuteQueryOperator(
        task_id="data_quality_tests_fact",
        sql=DQ_FACT_SQL,
        conn_id='snowflake_connection'
    )

    data_quality_tests_fact_fail = PythonOperator(
        task_id="data_quality_tests_fact_fail",
        python_callable=fail_if_data_quality_tests_failed,
        op_kwargs={
            'sql_string': DQ_FACT_FAIL,
            'schema': 'ANALYTICS',
            'table_name': 'fact_economic_indicators'
        })

    # Define task dependencies
    extraction >> insert_staging_economic_indicators >> remove_nulls >> data_quality_tests_staging >> data_quality_tests_staging_fail \
    >> insert_dim_indicator_date >> insert_fact_economic_indicators >> data_quality_tests_dimension >> data_quality_tests_dimension_fail \
    >> data_quality_tests_fact >> data_quality_tests_fact_fail