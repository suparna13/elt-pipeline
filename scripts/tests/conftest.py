"""
Pytest configuration and fixtures for PySpark ELT Pipeline tests
"""
import pytest
import tempfile
import shutil
import os
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime

@pytest.fixture(scope="session")
def spark_session():
    """
    Create a Spark session for testing
    """
    spark = SparkSession.builder \
        .appName("test_elt_pipeline") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()

@pytest.fixture
def temp_data_dir():
    """
    Create temporary data directory for tests
    """
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def sample_data():
    """
    Sample data for testing
    """
    return [
        (1, "customer_001", 100, datetime(2024, 1, 15, 10, 30, 0)),
        (2, "customer_002", 150, datetime(2024, 1, 15, 11, 45, 0)),
        (3, "customer_003", 200, datetime(2024, 1, 15, 14, 20, 0)),
        (4, "customer_004", 75, datetime(2024, 1, 15, 16, 10, 0)),
        (5, "customer_005", 300, datetime(2024, 1, 15, 18, 35, 0))
    ]

@pytest.fixture
def sample_schema():
    """
    Sample schema for testing
    """
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True),
        StructField("timestamp", TimestampType(), True)
    ])

@pytest.fixture
def sample_dataframe(spark_session, sample_data, sample_schema):
    """
    Create sample DataFrame for testing
    """
    return spark_session.createDataFrame(sample_data, sample_schema)

@pytest.fixture
def mock_db_config():
    """
    Mock database configuration
    """
    return {
        'jdbc_url': 'jdbc:postgresql://localhost:5432/test_db',
        'properties': {
            'user': 'test_user',
            'password': 'test_password',
            'driver': 'org.postgresql.Driver'
        }
    }

@pytest.fixture
def mock_env_vars():
    """
    Mock environment variables
    """
    env_vars = {
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'test_db',
        'POSTGRES_USER': 'test_user',
        'POSTGRES_PASSWORD': 'test_password'
    }
    
    with patch.dict(os.environ, env_vars):
        yield env_vars

@pytest.fixture
def mock_psycopg2():
    """
    Mock psycopg2 connection
    """
    with patch('psycopg2.connect') as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_conn, mock_cursor

@pytest.fixture(autouse=True)
def setup_test_paths():
    """
    Setup test paths and clean up after tests
    """
    # Create test directories
    test_dirs = ['/tmp/test_airflow/data/raw', '/tmp/test_airflow/data/processed']
    for test_dir in test_dirs:
        os.makedirs(test_dir, exist_ok=True)
    
    yield
    
    # Cleanup
    if os.path.exists('/tmp/test_airflow'):
        shutil.rmtree('/tmp/test_airflow')

# Custom assertions for PySpark DataFrames
class DataFrameAssertions:
    @staticmethod
    def assert_dataframes_equal(df1, df2):
        """Assert that two DataFrames are equal"""
        assert df1.count() == df2.count(), "DataFrames have different row counts"
        assert df1.columns == df2.columns, "DataFrames have different columns"
        
        # Convert to lists for comparison
        df1_rows = df1.collect()
        df2_rows = df2.collect()
        
        assert df1_rows == df2_rows, "DataFrames have different content"
    
    @staticmethod
    def assert_schema_equal(df1, df2):
        """Assert that two DataFrames have the same schema"""
        assert df1.schema == df2.schema, "DataFrames have different schemas"

@pytest.fixture
def df_assertions():
    """Provide DataFrame assertion utilities"""
    return DataFrameAssertions() 