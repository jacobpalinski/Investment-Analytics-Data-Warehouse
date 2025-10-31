# Import modules
import base64
import pytest
import pandas as pd
from dags.utils.snowflake_utils import Snowflake
from dags.tests.fixtures import mock_snowflake_client

class TestSnowflake:
    def test_create_connection(self, mocker):
        # Create variables for function
        user = "test_user"
        account = "test_account"
        warehouse = "test_wh"
        database = "test_db"
        schema = "test_schema"
        private_key = b"fake_private_key"
        private_key_encoded = base64.b64encode(private_key)

        # Create a mock snowflake connection
        mock_connection = mocker.MagicMock()
        mock_connect = mocker.patch(
            "dags.utils.snowflake_utils.snowflake.connector.connect",
            return_value=mock_connection
        )

        # Instantiate the Snowflake class
        snowflake_instance = Snowflake(user, account, private_key_encoded)

        # Invoke create_snowflake_connection
        conn = snowflake_instance.create_connection(
        warehouse=warehouse,
        database=database,
        schema=schema,
        )

        # Assert
        assert conn == mock_connection
        mock_connect.assert_called_once_with(
        user=user,
        private_key=private_key,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema,
        )
    
    def test_create_snowflake_connection_failure(self, mocker):
        # Create variables for function
        user = "test_user"
        account = "test_account"
        warehouse = "test_wh"
        database = "test_db"
        schema = "test_schema"
        private_key = b"fake_private_key"
        private_key_encoded = base64.b64encode(private_key)

        # Patch connect to raise an exception
        mocker.patch(
            "dags.utils.snowflake_utils.snowflake.connector.connect",
            side_effect=Exception("Connection failed")
        )

        # Instantiate the Snowflake class
        snowflake_instance = Snowflake(user, account, private_key_encoded)

        # Assert that exception is raised
        with pytest.raises(Exception, match="Connection failed"):
            snowflake_instance.create_connection(
                warehouse=warehouse,
                database=database,
                schema=schema,
            )
    
    def test_query_current_ciks(self, mocker, mock_snowflake_client):
        """ Test querying current CIKs from Snowflake """
        # Mock cursor and connection
        mock_cursor = mocker.MagicMock()
        mock_cursor.fetchall.return_value = [(12345,), (67890,)]

        mock_connection = mocker.MagicMock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor

        # Create test schema and table_name values
        schema = "tst"
        table_name = "dim_company"

        # Run test
        result = mock_snowflake_client.query_current_ciks(connection=mock_connection, schema=schema, table_name=table_name)

        # Assertions
        mock_cursor.execute.assert_called_once()
        assert result == [12345, 67890]
    
    def test_load_to_snowflake_success(self, mocker, mock_snowflake_client):
        """ Test successful load_to_snowflake with mocked write_pandas """
        # Mock write_pandas to simulate success
        mock_write_pandas = mocker.patch("dags.utils.snowflake_utils.write_pandas")
        mock_write_pandas.return_value = (True, 1, 10, None)
        mock_connection = mocker.MagicMock()

        # Input data
        data = [{"col1": "A", "col2": "B"}]

        # Run test
        mock_snowflake_client.load_to_snowflake(mock_connection, data, "TEST_TABLE")

        # Assertions
        mock_write_pandas.assert_called_once()
        args, _ = mock_write_pandas.call_args
        df_arg = args[1]
        assert isinstance(df_arg, pd.DataFrame)
        assert df_arg.columns.tolist() == ["COL1", "COL2"]
    
    def test_load_to_snowflake_failure(self, mocker, mock_snowflake_client):
        """ Test failure case for load_to_snowflake. """
        mock_write_pandas = mocker.patch("dags.utils.snowflake_utils.write_pandas")
        mock_write_pandas.return_value = (False, 0, 0, None)
        mock_connection = mocker.MagicMock()

        # Input data
        data = [{"col1": "A", "col2": "B"}]

        # Assert that exception is raised
        with pytest.raises(Exception, match="Failed to load data into TEST_TABLE"):
            mock_snowflake_client.load_to_snowflake(mock_connection, data, "TEST_TABLE")
    
    def test_read_sql_file(self, mocker, mock_snowflake_client):
        """ Test reading a SQL file """
        mock_sql = "SELECT * FROM TEST_TABLE"
        mock_open = mocker.mock_open(read_data=mock_sql)
        mocker.patch("builtins.open", mock_open)
        
        # Run test
        result = mock_snowflake_client.read_sql_file("mock_path.sql")
        
        # Assert sql files has been read correctly
        mock_open.assert_called_once_with("mock_path.sql", "r")
        assert result == mock_sql
    





