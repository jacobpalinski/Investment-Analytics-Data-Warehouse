# Import modules
import os
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from utils.utils import create_snowflake_connection
from sentiments_fact_table.sentiment_score_functions import initialise_model, initialise_tokenizer, calculate_sentiment_scores, process_text_with_chunking, \
process_in_batches_short_description, process_in_batches_long_description

def calculate_sentiment_scores_snowflake():
    '''
    Function to calculate sentiment scores for titles and descriptions for each row within 
    staging_combined_no_sentiment snowflake table
    '''
    # Load environment variables
    load_dotenv()

    # Create Snowflake connection
    snowflake_conn = create_snowflake_connection(
        user=os.getenv("SNOWFLAKE_USER"),
        private_key_encoded=os.getenv("SNOWFLAKE_PRIVATE_KEY_B64"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse='INVESTMENT_ANALYTICS_DWH',
        database='INVESTMENT_ANALYTICS',
        schema='STAGING'
    )

    # Retrieve all records from staging_combined_no_sentiment table in Snowflake
    with snowflake_conn.cursor() as cursor:
        cursor.execute("""
                        select 
                        date,
                        sentiment_category, 
                        ticker_symbol, 
                        title,
                        description, 
                        source
                        from investment_analytics.staging.staging_combined_no_sentiment
                    """)
        staging_combined_no_sentiment_df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    
    # Reshape into long format
    staging_combined_no_sentiment_df_long = pd.melt(
        staging_combined_no_sentiment_df,
        id_vars=["DATE", "TICKER_SYMBOL", "SOURCE", "SENTIMENT_CATEGORY"],
        value_vars=["TITLE", "DESCRIPTION"],
        var_name="ARTICLE_SENTIMENT_SOURCE",
        value_name="TEXT"
    )
    
    # Initialise Finbert model and tokenizer
    finbert = initialise_model()
    tokenizer = initialise_tokenizer()

    # Create seperate dataframes for records which have sentiment_category = 'Non Company' and sentiment_category != 'Non Company'
    non_company_df = staging_combined_no_sentiment_df_long[staging_combined_no_sentiment_df_long['SENTIMENT_CATEGORY'] == 'Non Company']
    remaining_df = staging_combined_no_sentiment_df_long[staging_combined_no_sentiment_df_long['SENTIMENT_CATEGORY'] != 'Non Company']

    # Calculate sentiment scores for non_company_df and remaining_df
    non_company_df['SENTIMENT_SCORE'] = process_in_batches_long_description(model=finbert, tokenizer=tokenizer, df=non_company_df, text_column='TEXT')
    remaining_df['SENTIMENT_SCORE'] = process_in_batches_short_description(model=finbert, df=remaining_df, text_column='TEXT')

    # Concatenate each dataframe
    final_df = pd.concat([non_company_df, remaining_df])

    # Drop text column
    final_df = final_df.drop('TEXT', axis=1)

    # Convert column names to uppercase
    final_df.columns = map(str.upper, final_df.columns)

    # Load into a Snowflake temp table
    success, nchunks, nrows, _ = write_pandas(snowflake_conn, final_df, table_name="STAGING_COMBINED_WITH_SENTIMENT_TEMP")
    print(f"Staged {nrows} rows into STAGING_COMBINED_WITH_SENTIMENT_TEMP. Success: {success}")

    with snowflake_conn.cursor() as cursor:
        cursor.execute("""
                        merge into investment_analytics.staging.staging_combined_with_sentiment as target
                        using (
                        select
                        distinct
                        *
                        from investment_analytics.staging.staging_combined_with_sentiment_temp
                        ) as source_data
                        on target.date = source_data.date
                        and target.ticker_symbol = source_data.ticker_symbol
                        and target.source = source_data.source
                        and target.sentiment_category = source_data.sentiment_category
                        and target.article_sentiment_source = source_data.article_sentiment_source
                        
                        when not matched then
                        insert (
                        date,
                        ticker_symbol,
                        source,
                        sentiment_category,
                        article_sentiment_source,
                        sentiment_score
                        )
                        values (
                        source_data.date,
                        source_data.ticker_symbol,
                        source_data.source,
                        source_data.sentiment_category,
                        source_data.article_sentiment_source,
                        source_data.sentiment_score
                        )
                    """)
        print("Merged data from STAGING_COMBINED_WITH_SENTIMENT_TEMP into STAGING_COMBINED_WITH_SENTIMENT")
        
        cursor.execute("""
                        delete from investment_analytics.staging.staging_combined_with_sentiment_temp
                """)
        print("Deleted contents of STAGING_COMBINED_WITH_SENTIMENT table")






    




