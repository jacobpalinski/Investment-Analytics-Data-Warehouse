# Import necessary libraries
import os
from datetime import datetime, timedelta
from dags.utils.snowflake_utils import Snowflake
from dags.company_dimension_dag.company_info_extraction import extract_company_info
from dags.data_quality_checks_outcomes import fail_if_data_quality_tests_failed
from dags.utils.s3_utils import SNSNotifier
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup SNS notifier class
sns_notifier = SNSNotifier(access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'), secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'), 
                           region=os.environ.get('AWS_REGION'), topic_arn=os.environ.get('SNS_TOPIC_ARN'))

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
    'on_failure_callback': sns_notifier
}

# Set paths to SQL files
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MERGE_SQL_PATH = os.path.join(BASE_DIR, 'sql', 'merge_company_information.sql')
UPDATE_DIM_COMPANY = os.path.join(BASE_DIR, 'sql', 'update_current_dim_company.sql')
INSERT_DIM_COMPANY = os.path.join(BASE_DIR, 'sql', 'insert_dim_company.sql')
CREATE_DEFAULT_KEYS = os.path.join(BASE_DIR, 'sql', 'create_default_keys.sql')
DATA_QUALITY_TESTS_STAGING = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_staging_company_information.sql')
DATA_QUALITY_TESTS_DIMENSION = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_dim_company.sql')
DATA_QUALITY_STAGING_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_staging_company_information_fail.sql')
DATA_QUALITY_DIMENSION_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_dim_company_fail.sql')

# Read SQL contents
MERGE_SQL = snowflake_client.read_sql_file(MERGE_SQL_PATH)
UPDATE_DIM = snowflake_client.read_sql_file(UPDATE_DIM_COMPANY)
INSERT_DIM = snowflake_client.read_sql_file(INSERT_DIM_COMPANY)
CREATE_DEFAULT_KEYS_SQL = snowflake_client.read_sql_file(CREATE_DEFAULT_KEYS)
DQ_STAGING_SQL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_STAGING)
DQ_DIM_SQL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_DIMENSION)
DQ_STAGING_FAIL = snowflake_client.read_sql_file(DATA_QUALITY_STAGING_FAIL_PATH)
DQ_DIM_FAIL = snowflake_client.read_sql_file(DATA_QUALITY_DIMENSION_FAIL_PATH)

# Define the DAG
with DAG(dag_id='company_dimension_dag',
    default_args=default_args,
    description='DAG to create dim_company table in Snowflake',
    schedule='0 5 1 * *',
    max_active_runs=1,
    tags=['company', 'dimension', 'snowflake']
):
    
    extraction = PythonOperator(
        task_id='extraction',
        python_callable=extract_company_info
    )

    merge_raw_company_information = SQLExecuteQueryOperator(
        task_id="merge_raw_company_information",
        sql=MERGE_SQL,
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
            'table_name': 'staging_company_information'
        }
    )
    
    update_current_dim_company = SQLExecuteQueryOperator(
        task_id="update_current_dim_company",
        sql=UPDATE_DIM,
        conn_id='snowflake_connection'
    )

    insert_dim_company = SQLExecuteQueryOperator(
        task_id="insert_dim_company",
        sql=INSERT_DIM,
        conn_id='snowflake_connection'
    )

    create_default_keys = SQLExecuteQueryOperator(
        task_id="create_default_keys",
        sql=CREATE_DEFAULT_KEYS_SQL,
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
            'schema': 'CORE',
            'table_name': 'dim_company'
        }
    )

    # Define task dependencies
    extraction >> merge_raw_company_information >> data_quality_tests_staging >> data_quality_tests_staging_fail >> update_current_dim_company >> insert_dim_company \
    >> create_default_keys >> data_quality_tests_dimension >> data_quality_tests_dimension_fail

    





    







