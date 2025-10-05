# Import modules
import base64
import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# Create setup for logging
logger = logging.getLogger(__name__)

class Snowflake:
    """
    Class for connecting, interacting and loading to Snowflake
    
    Attributes:
        user (str): User for Snowflake connection
        account (str): Account for Snowflake connection
        private_key (str): Private key used for Snowflake connection

    Methods:
        create_connection(warehouse, database, schema):
            Creates a Snowflake connection using the provided credentials
        query_current_ciks(connection):
            Retrieves current CIKs from the dim_company dimension table in Snowflake
        load_to_snowflake(connection, data, target_table):
            Loads data into a specified Snowflake table.
    """
    def __init__(self, user: str, account: str, private_key_encoded: str):
        """ Initialises user, account and private_key for Snowflake connection """
        self.user = user
        self.account = account
        self.private_key = private_key_encoded
    
    def create_connection(self, warehouse: str, database: str, schema: str) -> snowflake.connector.SnowflakeConnection:
        """
        Creates a Snowflake connection using the provided credentials
        
        Args:
            warehouse (str): Snowflake warehouse name.
            database (str): Snowflake database name.
            schema (str): Snowflake schema name.

        Returns:
            snowflake.connector.SnowflakeConnection: A connection object to interact with Snowflake.
        """
        private_key_decoded = base64.b64decode(self.private_key)

        return snowflake.connector.connect(
            user=self.user,
            private_key=private_key_decoded,
            account=self.account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
    
    def query_current_ciks(self, connection: snowflake.connector.SnowflakeConnection) -> list:
        """
        Retrieves current CIKs from the dim_company dimension table in Snowflake

        Args:
            connection (snowflake.connector.SnowflakeConnection): Snowflake connection object

        Returns:
            list: List of current CIKs
        """
        with connection.cursor() as cursor:
            cursor.execute("""
                        select 
                        distinct 
                        cik 
                        from investment_analytics.core.dim_company
                        where is_current = TRUE """)
            ciks = [row[0] for row in cursor.fetchall()]
        
        return ciks
    
    def load_to_snowflake(self, connection: snowflake.connector.SnowflakeConnection, data: list, target_table: str) -> None:
        """
        Loads data into a specified Snowflake table.

        Args:
            connection (snowflake.connector.SnowflakeConnection): The Snowflake connection object.
            data (list): The data to load into Snowflake.
            target_table (str): The target table in Snowflake where data will be loaded.
        """
        df = pd.DataFrame(data)
        df.columns = map(str.upper, df.columns)  # Convert column names to uppercase
        success, nchunks, nrows, _ = write_pandas(connection, df, target_table)
        
        if not success:
            raise Exception(f"Failed to load data into {target_table}.")
        
        logger.info(f"Loaded {nrows} rows into {target_table} in {nchunks} chunks.")