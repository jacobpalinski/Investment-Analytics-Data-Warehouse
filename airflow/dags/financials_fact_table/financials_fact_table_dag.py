# Import necessary libraries
import os
from datetime import datetime, timedelta
from dags.utils.snowflake_utils import Snowflake
from dags.financials_fact_table.financials_extraction import extract_financials
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
    'start_date': datetime(2026, 1, 2, 6, 0),
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
INSERT_STAGING_FINANCIALS_PATH = os.path.join(BASE_DIR, 'sql', 'insert_staging_financials.sql')
MOST_RECENT_RECORDS_PATH = os.path.join(BASE_DIR, 'sql', 'keep_most_recent_record.sql')
DELETE_RECORDS_PATH = os.path.join(BASE_DIR, 'sql', 'delete_records.sql')
DERIVE_RATIOS_PATH = os.path.join(BASE_DIR, 'sql', 'derive_ratios.sql')
DELETE_NULL_RATIOS_PATH = os.path.join(BASE_DIR, 'sql', 'delete_null_ratios.sql')
POPULATE_QUARTER_PATH = os.path.join(BASE_DIR, 'sql', 'populate_quarter.sql')
INSERT_DIM_PERIOD_PATH = os.path.join(BASE_DIR, 'sql', 'insert_dim_period.sql')
INSERT_FACT_FINANCIALS_PATH = os.path.join(BASE_DIR, 'sql', 'insert_fact_financials.sql')
DATA_QUALITY_TESTS_STAGING_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_staging_financials.sql')
DATA_QUALITY_TESTS_FACT_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_fact_financials.sql')
DATA_QUALITY_TESTS_DIMENSION_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_dim_period.sql')
DATA_QUALITY_STAGING_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_staging_financials_fail.sql')
DATA_QUALITY_TESTS_DIMENSION_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_dim_period_fail.sql')
DATA_QUALITY_FACT_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_fact_financials_fail.sql')

# Read SQL contents
INSERT_STAGING_FINANCIALS = snowflake_client.read_sql_file(INSERT_STAGING_FINANCIALS_PATH)
MOST_RECENT_RECORDS = snowflake_client.read_sql_file(MOST_RECENT_RECORDS_PATH)
DELETE_RECORDS = snowflake_client.read_sql_file(DELETE_RECORDS_PATH)
DERIVE_RATIOS = snowflake_client.read_sql_file(DERIVE_RATIOS_PATH)
DELETE_NULL_RATIOS = snowflake_client.read_sql_file(DELETE_NULL_RATIOS_PATH)
POPULATE_QUARTER = snowflake_client.read_sql_file(POPULATE_QUARTER_PATH)
INSERT_DIM_PERIOD = snowflake_client.read_sql_file(INSERT_DIM_PERIOD_PATH)
INSERT_FACT_FINANCIALS = snowflake_client.read_sql_file(INSERT_FACT_FINANCIALS_PATH)
DQ_STAGING_SQL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_STAGING_PATH)
DQ_FACT_SQL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_FACT_PATH)
DQ_DIM_SQL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_DIMENSION_PATH)
DQ_STAGING_FAIL = snowflake_client.read_sql_file(DATA_QUALITY_STAGING_FAIL_PATH)
DQ_FACT_FAIL = snowflake_client.read_sql_file(DATA_QUALITY_STAGING_FAIL_PATH)
DQ_DIM_FAIL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_DIMENSION_FAIL_PATH)

# Define the DAG
with DAG(dag_id='financials_fact_dag',
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

    delete_null_ratios = SQLExecuteQueryOperator(
        task_id="delete_null_ratios",
        sql=DELETE_NULL_RATIOS,
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

    insert_dim_period = SQLExecuteQueryOperator(
        task_id="insert_dim_period",
        sql=INSERT_DIM_PERIOD,
        conn_id='snowflake_connection'
    )

    insert_fact_financials = SQLExecuteQueryOperator(
        task_id="insert_fact_financials",
        sql=INSERT_FACT_FINANCIALS,
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
            'schema': 'FINANCIALS',
            'table_name': 'dim_period'
        }
    )

    data_quality_tests_fact = SQLExecuteQueryOperator(
        task_id="data_quality_tests_fact_financials",
        sql=DQ_FACT_SQL,
        conn_id='snowflake_connection'
    )

    data_quality_tests_fact_fail = SQLExecuteQueryOperator(
        task_id="data_quality_tests_fact_fail",
        sql=DQ_FACT_FAIL,
        conn_id='snowflake_connection'
    )

    # Define task dependencies
    extraction >> insert_staging_financials >> keep_most_recent_record >> delete_records >> derive_ratios \
    >> delete_null_ratios >> populate_quarter >> data_quality_tests_staging >> data_quality_tests_staging_fail >> insert_dim_period >> insert_fact_financials \
    >> data_quality_tests_dimension >> data_quality_tests_dimension_fail >> data_quality_tests_fact >> data_quality_tests_fact_fail

    





    







