# Import necessary libraries
import os
from datetime import datetime, timedelta
from utils.utils import read_sql_file
from economic_indicator_dimension.economic_indicator_extraction import extract_economic_indicators
from data_quality_checks_outcomes import fail_if_data_quality_tests_failed
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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
MERGE_SQL_PATH = os.path.join(BASE_DIR, 'sql', 'merge_economic_indicators.sql')
REMOVE_NULLS_PATH = os.path.join(BASE_DIR, 'sql', 'remove_nulls.sql')
UPDATE_DIM_ECONOMIC_INDICATORS = os.path.join(BASE_DIR, 'sql', 'update_dim_economic_indicators.sql')
INSERT_DIM_ECONOMIC_INDICATORS = os.path.join(BASE_DIR, 'sql', 'insert_dim_economic_indicators.sql')
DATA_QUALITY_TESTS_STAGING = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_staging_economic_indicators.sql')
DATA_QUALITY_TESTS_DIMENSION = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_dim_economic_indicators.sql')
DATA_QUALITY_STAGING_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_staging_economic_indicators_fail.sql')
DATA_QUALITY_DIMENSION_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_dim_economic_indicators_fail.sql')

# Read SQL contents
MERGE_SQL = read_sql_file(MERGE_SQL_PATH)
REMOVE_NULLS = read_sql_file(REMOVE_NULLS_PATH)
UPDATE_DIM = read_sql_file(UPDATE_DIM_ECONOMIC_INDICATORS)
INSERT_DIM = read_sql_file(INSERT_DIM_ECONOMIC_INDICATORS)
DQ_STAGING_SQL = read_sql_file(DATA_QUALITY_TESTS_STAGING)
DQ_DIM_SQL = read_sql_file(DATA_QUALITY_TESTS_DIMENSION)
DQ_STAGING_FAIL = read_sql_file(DATA_QUALITY_STAGING_FAIL_PATH)
DQ_DIM_FAIL = read_sql_file(DATA_QUALITY_DIMENSION_FAIL_PATH)

# Define the DAG
with DAG(dag_id='economic_indicators_dimension_dag',
    default_args=default_args,
    description='DAG to create dim_economic_indicators table in Snowflake',
    schedule='@monthly',
    max_active_runs=1,
    tags=['economic_indicators', 'dimension', 'snowflake']
):

    extraction = PythonOperator(
        task_id='extraction',
        python_callable=extract_economic_indicators
    )

    merge_economic_indicators = SQLExecuteQueryOperator(
        task_id="merge_economic_indicators_information",
        sql=MERGE_SQL,
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
    
    update_current_dim_economic_indicators = SQLExecuteQueryOperator(
        task_id="update_current_dim_economic_indicators",
        sql=UPDATE_DIM,
        conn_id='snowflake_connection'
    )

    insert_dim_economic_indicators = SQLExecuteQueryOperator(
        task_id="insert_dim_economic_indicators",
        sql=INSERT_DIM,
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

    # Define task dependencies
    extraction >> merge_economic_indicators >> remove_nulls >> data_quality_tests_staging >> data_quality_tests_staging_fail >> update_current_dim_economic_indicators \
    >> insert_dim_economic_indicators >> data_quality_tests_dimension >> data_quality_tests_dimension_fail