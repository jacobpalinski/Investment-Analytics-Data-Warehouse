# Import necessary libraries
import os
from datetime import datetime, timedelta
from utils.utils import read_sql_file
from financials_dimension.financials_extraction import extract_financials
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
INSERT_STAGING_FINANCIALS_PATH = os.path.join(BASE_DIR, 'sql', 'insert_staging_financials.sql')
MOST_RECENT_RECORDS_PATH = os.path.join(BASE_DIR, 'sql', 'keep_most_recent_record.sql')
DELETE_RECORDS_PATH = os.path.join(BASE_DIR, 'sql', 'delete_records.sql')
DERIVE_RATIOS_PATH = os.path.join(BASE_DIR, 'sql', 'derive_ratios.sql')
RENAME_ITEMS_PATH = os.path.join(BASE_DIR, 'sql', 'rename_items.sql')
POPULATE_QUARTER_PATH = os.path.join(BASE_DIR, 'sql', 'populate_quarter.sql')
INSERT_DIM_FINANCIALS = os.path.join(BASE_DIR, 'sql', 'insert_dim_financials.sql')
DATA_QUALITY_TESTS_STAGING = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_staging_financials.sql')
DATA_QUALITY_TESTS_DIMENSION = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_dim_financials.sql')
DATA_QUALITY_STAGING_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_staging_financials_fail.sql')
DATA_QUALITY_DIMENSION_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_dim_financials_fail.sql')

# Read SQL contents
INSERT_STAGING_FINANCIALS = read_sql_file(INSERT_STAGING_FINANCIALS_PATH)
MOST_RECENT_RECORDS = read_sql_file(MOST_RECENT_RECORDS_PATH)
DELETE_RECORDS = read_sql_file(DELETE_RECORDS_PATH)
RENAME_ITEMS = read_sql_file(RENAME_ITEMS_PATH)
DERIVE_RATIOS = read_sql_file(DERIVE_RATIOS_PATH)
POPULATE_QUARTER = read_sql_file(POPULATE_QUARTER_PATH)
INSERT_DIM = read_sql_file(INSERT_DIM_FINANCIALS)
DQ_STAGING_SQL = read_sql_file(DATA_QUALITY_TESTS_STAGING)
DQ_DIM_SQL = read_sql_file(DATA_QUALITY_TESTS_DIMENSION)
DQ_STAGING_FAIL = read_sql_file(DATA_QUALITY_STAGING_FAIL_PATH)
DQ_DIM_FAIL = read_sql_file(DATA_QUALITY_DIMENSION_FAIL_PATH)

# Define the DAG
with DAG(dag_id='financials_dimension_dag',
    default_args=default_args,
    description='DAG to create dim_company table in Snowflake',
    schedule='@monthly',
    max_active_runs=1,
    tags=['financials', 'dimension', 'snowflake']
):
    
    extraction = PythonOperator(
        task_id='extraction',
        python_callable=extract_financials
    )

    insert_staging_financials = SQLExecuteQueryOperator(
        task_id="insert_staging_financials",
        sql=INSERT_STAGING_FINANCIALS,
        conn_id='snowflake_connection'
    )

    keep_most_recent_record = SQLExecuteQueryOperator(
        task_id="keep_most_recent_record",
        sql=MOST_RECENT_RECORDS,
        conn_id='snowflake_connection'
    )

    rename_items = SQLExecuteQueryOperator(
        task_id="rename_items",
        sql=RENAME_ITEMS,
        conn_id='snowflake_connection'
    )

    delete_records = SQLExecuteQueryOperator(
        task_id="delete_records",
        sql=DELETE_RECORDS,
        conn_id='snowflake_connection'
    )

    derive_ratios = SQLExecuteQueryOperator(
        task_id="derive_ratios",
        sql=DERIVE_RATIOS,
        conn_id='snowflake_connection'
    )

    populate_quarter = SQLExecuteQueryOperator(
        task_id="populate_quarter",
        sql=POPULATE_QUARTER,
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
            'table_name': 'staging_financials'
        }
    )

    insert_dim_company = SQLExecuteQueryOperator(
        task_id="insert_dim_company",
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
            'table_name': 'dim_financials'
        }
    )

    # Define task dependencies
    extraction >> insert_staging_financials >> keep_most_recent_record >> rename_items >> delete_records >> derive_ratios >> populate_quarter >> data_quality_tests_staging \
    >> data_quality_tests_staging_fail >> insert_dim_company >> data_quality_tests_dimension >> data_quality_tests_dimension_fail

    





    







