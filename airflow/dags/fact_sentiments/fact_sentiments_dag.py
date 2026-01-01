# Import necessary libraries
import os
from datetime import datetime, timedelta
from dags.utils.snowflake_utils import Snowflake
from dags.fact_sentiments.extraction_functions import extract_company_news, extract_non_company_news, extract_reddit_submissions
from dags.fact_sentiments.calculate_sentiment_scores import calculate_sentiment_scores_snowflake
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
    'start_date': datetime(2026, 1, 2, 7, 0),
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
INSERT_STAGING_COMPANY_NEWS_PATH = os.path.join(BASE_DIR, 'sql', 'insert_staging_company_news.sql')
INSERT_STAGING_NON_COMPANY_NEWS_PATH = os.path.join(BASE_DIR, 'sql', 'insert_staging_non_company_news.sql')
INSERT_STAGING_REDDIT_SUBMISSIONS_PATH = os.path.join(BASE_DIR, 'sql', 'insert_staging_reddit_submissions.sql')
INSERT_STAGING_COMBINED_NO_SENTIMENT_PATH = os.path.join(BASE_DIR, 'sql', 'insert_staging_combined_no_sentiment.sql')
REMOVE_NULLS_STAGING_COMBINED_NO_SENTIMENT_PATH = os.path.join(BASE_DIR, 'sql', 'remove_nulls_staging_combined_no_sentiment.sql')
INSERT_DIM_SOURCE_PATH = os.path.join(BASE_DIR, 'sql', 'insert_dim_source.sql')
INSERT_DIM_SENTIMENT_DATE_PATH = os.path.join(BASE_DIR, 'sql', 'insert_dim_sentiment_date.sql')
INSERT_FACT_SENTIMENTS_PATH = os.path.join(BASE_DIR, 'sql', 'insert_fact_sentiments.sql')
DATA_QUALITY_TESTS_STAGING_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_staging_combined_with_sentiment.sql')
DATA_QUALITY_TESTS_FACT_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_fact_sentiments.sql')
DATA_QUALITY_TESTS_DIM_SOURCE_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_dim_source.sql')
DATA_QUALITY_TESTS_DIM_SENTIMENT_DATE_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_tests_dim_sentiment_date.sql')
DATA_QUALITY_STAGING_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_staging_combined_with_sentiment_fail.sql')
DATA_QUALITY_FACT_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_fact_sentiments_fail.sql')
DATA_QUALITY_DIM_SOURCE_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_dim_source_fail.sql')
DATA_QUALITY_DIM_SENTIMENT_DATE_FAIL_PATH = os.path.join(BASE_DIR, 'sql', 'data_quality_dim_sentiment_date_fail.sql')

# Read SQL contents
INSERT_STAGING_COMPANY_NEWS = snowflake_client.read_sql_file(INSERT_STAGING_COMPANY_NEWS_PATH)
INSERT_STAGING_NON_COMPANY_NEWS = snowflake_client.read_sql_file(INSERT_STAGING_NON_COMPANY_NEWS_PATH)
INSERT_STAGING_REDDIT_SUBMISSIONS = snowflake_client.read_sql_file(INSERT_STAGING_REDDIT_SUBMISSIONS_PATH)
INSERT_STAGING_COMBINED_NO_SENTIMENT = snowflake_client.read_sql_file(INSERT_STAGING_COMBINED_NO_SENTIMENT_PATH)
REMOVE_NULLS_STAGING_COMBINED_NO_SENTIMENT = snowflake_client.read_sql_file(REMOVE_NULLS_STAGING_COMBINED_NO_SENTIMENT_PATH)
INSERT_DIM_SOURCE = snowflake_client.read_sql_file(INSERT_DIM_SOURCE_PATH)
INSERT_DIM_SENTIMENT_DATE = snowflake_client.read_sql_file(INSERT_DIM_SENTIMENT_DATE_PATH)
INSERT_FACT_SENTIMENTS = snowflake_client.read_sql_file(INSERT_FACT_SENTIMENTS_PATH)
DQ_STAGING_SQL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_STAGING_PATH)
DQ_FACT_SQL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_FACT_PATH)
DQ_DIM_SOURCE_SQL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_DIM_SOURCE_PATH)
DQ_DIM_SENTIMENT_DATE_SQL = snowflake_client.read_sql_file(DATA_QUALITY_TESTS_DIM_SENTIMENT_DATE_PATH)
DQ_STAGING_FAIL = snowflake_client.read_sql_file(DATA_QUALITY_STAGING_FAIL_PATH)
DQ_FACT_FAIL = snowflake_client.read_sql_file(DATA_QUALITY_FACT_FAIL_PATH)
DQ_DIM_SOURCE_FAIL = snowflake_client.read_sql_file(DATA_QUALITY_DIM_SOURCE_FAIL_PATH)
DQ_DIM_SENTIMENT_DATE_FAIL = snowflake_client.read_sql_file(DATA_QUALITY_DIM_SENTIMENT_DATE_FAIL_PATH)

# Define the DAG
with DAG(dag_id='fact_sentiments_dag',
    default_args=default_args,
    description='DAG to create fact_sentiments table in Snowflake',
    schedule='0 7 * * *',
    max_active_runs=1,
    tags=['sentiments', 'fact', 'snowflake']
):
    
    extract_non_company_news = PythonOperator(
        task_id='extract_non_company_news',
        python_callable=extract_non_company_news,
        op_kwargs={
            'parameters': [
                {"category": "business", "country": "us", "qInMeta": "economy AND interest rate"},
                {"category": "business", "country": "us", "qInMeta": "economy AND inflation"},
                {"category": "business", "country": "us", "qInMeta": "economy AND Federal Reserve"},
                {"category": "business", "country": "us", "qInMeta": "economy AND consumer confidence"},
                {"category": "business", "country": "us", "qInMeta": "economy and unemployment"},
                {"category": "business", "country": "us", "qInMeta": "economy AND GDP"},
                {"category": "business", "country": "us", "qInMeta": "economy AND tariffs"},
                {"category": "business", "country": "us", "qInMeta": "economy AND treasury yields"},
                {"category": "business", "country": "us", "qInMeta": "economy AND trade balance"},
                {"category": "business", "country": "us", "qInMeta": "economy AND retail sales"},
                {"category": "business", "country": "us", "qInMeta": "economy AND CPI"},
                {"category": "business", "country": "us", "qInMeta": "economy AND bond spreads"},
                {"category": "politics", "country": "us", "qInMeta": "Trump AND Canada"},
                {"category": "politics", "country": "us", "qInMeta": "Trump AND China"},
                {"category": "politics", "country": "us", "qInMeta": "Trump AND Mexico"},
                {"category": "politics", "country": "us", "qInMeta": "Trump AND EU"},
                {"category": "politics", "country": "us", "qInMeta": "Democrats"},
                {"category": "politics", "country": "us", "qInMeta": "Republicans"}
            ],
            'timeout': 15
        }
    )

    insert_staging_non_company_news = SQLExecuteQueryOperator(
        task_id="insert_staging_non_company_news",
        sql=INSERT_STAGING_NON_COMPANY_NEWS,
        conn_id='snowflake_connection'
    )
    
    extract_company_news = PythonOperator(
        task_id='extract_company_articles',
        python_callable=extract_company_news
    )

    insert_staging_company_news = SQLExecuteQueryOperator(
        task_id="insert_staging_company_news",
        sql=INSERT_STAGING_COMPANY_NEWS,
        conn_id='snowflake_connection'
    )

    extract_reddit_submissions = PythonOperator(
        task_id='extract_reddit_submissions',
        python_callable=extract_reddit_submissions,
        op_kwargs={
            'subreddit_name': 'stockmarket',
            'submissions_limit': 1000
        }
    )

    insert_staging_reddit_submissions = SQLExecuteQueryOperator(
        task_id="insert_staging_reddit_submissions",
        sql=INSERT_STAGING_REDDIT_SUBMISSIONS,
        conn_id='snowflake_connection'
    )

    insert_staging_combined_no_sentiment = SQLExecuteQueryOperator(
        task_id="insert_staging_combined_no_sentiment",
        sql=INSERT_STAGING_COMBINED_NO_SENTIMENT,
        conn_id='snowflake_connection'
    )

    remove_nulls_staging_combined_no_sentiment = SQLExecuteQueryOperator(
        task_id="remove_nulls_staging_combined_no_sentiment",
        sql=REMOVE_NULLS_STAGING_COMBINED_NO_SENTIMENT,
        conn_id='snowflake_connection'
    )

    calculate_sentiment_scores_snowflake = PythonOperator(
        task_id='calculate_sentiment_scores_snowflake',
        python_callable=calculate_sentiment_scores_snowflake
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
            'table_name': 'staging_combined_with_sentiment'
        }
    )

    insert_dim_source = SQLExecuteQueryOperator(
        task_id="insert_dim_source",
        sql=INSERT_DIM_SOURCE,
        conn_id='snowflake_connection'
    )

    insert_dim_sentiment_date = SQLExecuteQueryOperator(
        task_id="insert_dim_sentiment_date",
        sql=INSERT_DIM_SENTIMENT_DATE,
        conn_id='snowflake_connection'
    )

    insert_fact_sentiments = SQLExecuteQueryOperator(
        task_id="insert_fact_sentiments",
        sql=INSERT_FACT_SENTIMENTS,
        conn_id='snowflake_connection'
    )

    data_quality_tests_dim_source = SQLExecuteQueryOperator(
        task_id="data_quality_tests_dim_source",
        sql=DQ_DIM_SOURCE_SQL,
        conn_id='snowflake_connection'
    )

    data_quality_tests_dim_source_fail = PythonOperator(
        task_id="data_quality_tests_dim_source_fail",
        python_callable=fail_if_data_quality_tests_failed,
        op_kwargs={
            'sql_string': DQ_DIM_SOURCE_FAIL,
            'schema': 'SENTIMENT',
            'table_name': 'dim_source'
        }
    )

    data_quality_tests_dim_sentiment_date = SQLExecuteQueryOperator(
        task_id="data_quality_tests_dim_sentiment_date",
        sql=DQ_DIM_SENTIMENT_DATE_SQL,
        conn_id='snowflake_connection'
    )

    data_quality_tests_dim_sentiment_date_fail = PythonOperator(
        task_id="data_quality_tests_dim_sentiment_date_fail",
        python_callable=fail_if_data_quality_tests_failed,
        op_kwargs={
            'sql_string': DQ_DIM_SENTIMENT_DATE_FAIL,
            'schema': 'SENTIMENT',
            'table_name': 'dim_sentiment_date'
        }
    )

    data_quality_tests_fact_sentiments = SQLExecuteQueryOperator(
        task_id="data_quality_tests_fact_sentiments",
        sql=DQ_FACT_SQL,
        conn_id='snowflake_connection'
    )

    data_quality_tests_fact_sentiments_fail = SQLExecuteQueryOperator(
        task_id="data_quality_tests_fact_sentiments_fail",
        sql=DQ_FACT_FAIL,
        conn_id='snowflake_connection'
    )

    # Define task dependencies
    extract_non_company_news >> insert_staging_non_company_news >> extract_company_news >> insert_staging_company_news \
    >> extract_reddit_submissions >> insert_staging_reddit_submissions >> insert_staging_combined_no_sentiment \
    >> remove_nulls_staging_combined_no_sentiment >> calculate_sentiment_scores_snowflake >> data_quality_tests_staging \
    >> data_quality_tests_staging_fail >> insert_dim_source >> insert_dim_sentiment_date \
    >> insert_fact_sentiments >> data_quality_tests_dim_source >> data_quality_tests_dim_source_fail >> data_quality_tests_dim_sentiment_date \
    >> data_quality_tests_dim_sentiment_date_fail >> data_quality_tests_fact_sentiments >> data_quality_tests_fact_sentiments_fail 

    





    







