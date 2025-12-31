# Import necessary libraries
import os
from dotenv import load_dotenv
from dags.utils.snowflake_utils import Snowflake
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def fail_if_data_quality_tests_failed(sql_string:str, schema:str, table_name:str) -> ValueError:
    """    Checks if any data quality checks failed for a given set of data quality checks
    Raises an error if any checks failed.
    """
    # Load environment variables
    load_dotenv()

    # Instantiate Snowflake Client
    snowflake_client = Snowflake(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        private_key_encoded=os.getenv("SNOWFLAKE_PRIVATE_KEY_B64")
    )

    # Create snowflake connection
    snowflake_conn = snowflake_client.create_connection(
    warehouse='INVESTMENT_ANALYTICS_DWH',
    database='INVESTMENT_ANALYTICS',
    schema=schema)
    
    # Execute SQL to check data quality results
    hook = SnowflakeHook(snowflake_conn_id='snowflake_connection')
    sql = sql_string
    result = hook.get_first(sql)
    if result and result[0] > 0:
        raise ValueError(f"Data quality checks failed for {schema}.{table_name}")