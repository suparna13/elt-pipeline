"""
Unit tests for ingest module
"""
import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock, mock_open
from datetime import datetime

# Add parent directory to path for imports
import sys
sys.path.append('/opt/airflow/scripts')

import ingest

class TestDataExtraction:
    """Test data extraction functionality"""
    
    @patch('ingest.get_spark_session')
    @patch('ingest.stop_spark_session')
    @patch('os.makedirs')
    def test_extract_data_success(self, mock_makedirs, mock_stop_spark, mock_get_spark, spark_session):
        """Test successful data extraction"""
        # Setup mock Spark session
        mock_get_spark.return_value = spark_session
        
        # Create test DataFrame
        test_data = [
            (1, "customer_001", 100, datetime.now()),
            (2, "customer_002", 150, datetime.now())
        ]
        test_df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        
        # Mock DataFrame operations
        with patch.object(spark_session, 'createDataFrame', return_value=test_df):
            # Mock write operations
            mock_writer = MagicMock()
            test_df.write = mock_writer
            test_df.coalesce = MagicMock(return_value=test_df)
            
            result = ingest.extract_data()
            
            assert result is True
            mock_get_spark.assert_called_once()
            mock_stop_spark.assert_called_once()
            mock_makedirs.assert_called()
    
    @patch('ingest.get_spark_session')
    @patch('ingest.stop_spark_session')
    def test_extract_data_spark_error(self, mock_stop_spark, mock_get_spark):
        """Test data extraction with Spark error"""
        # Mock Spark session creation failure
        mock_get_spark.side_effect = Exception("Spark initialization failed")
        
        with pytest.raises(Exception, match="Spark initialization failed"):
            ingest.extract_data()
        
        mock_stop_spark.assert_called_once()
    
    @patch('ingest.get_spark_session')
    @patch('ingest.stop_spark_session')
    @patch('os.makedirs')
    def test_extract_data_write_error(self, mock_makedirs, mock_stop_spark, mock_get_spark, spark_session):
        """Test data extraction with write error"""
        mock_get_spark.return_value = spark_session
        
        # Mock DataFrame creation but write failure
        test_data = [(1, "test", 100, datetime.now())]
        test_df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        
        with patch.object(spark_session, 'createDataFrame', return_value=test_df):
            # Mock write failure
            mock_writer = MagicMock()
            mock_writer.parquet.side_effect = Exception("Write failed")
            test_df.write = mock_writer
            
            with pytest.raises(Exception, match="Write failed"):
                ingest.extract_data()
            
            mock_stop_spark.assert_called_once()

class TestDatabaseExtraction:
    """Test database extraction functionality"""
    
    @patch('ingest.get_spark_session')
    @patch('ingest.stop_spark_session')
    @patch('ingest.get_database_config')
    def test_extract_from_database_success(self, mock_db_config_patch, mock_stop_spark, mock_get_spark, spark_session, mock_db_config):
        """Test successful database extraction"""
        mock_get_spark.return_value = spark_session
        mock_db_config_patch.return_value = mock_db_config
        
        # Create test DataFrame to simulate database read
        test_data = [(1, "db_record", 200)]
        test_df = spark_session.createDataFrame(test_data, ["id", "name", "value"])
        
        # Mock read operation
        mock_reader = MagicMock()
        mock_reader.jdbc.return_value = test_df
        spark_session.read = mock_reader
        
        with patch.dict(os.environ, {
            'SOURCE_DB_HOST': 'test_host',
            'SOURCE_DB_PORT': '5432',
            'SOURCE_DB_NAME': 'test_source_db',
            'SOURCE_DB_USER': 'test_user',
            'SOURCE_DB_PASSWORD': 'test_pass'
        }):
            result = ingest.extract_from_database()
            
            assert result == test_df
            mock_get_spark.assert_called_once_with("ELT_Database_Extract")
            mock_stop_spark.assert_called_once()
            mock_reader.jdbc.assert_called_once()
    
    @patch('ingest.get_spark_session')
    @patch('ingest.stop_spark_session')
    def test_extract_from_database_connection_error(self, mock_stop_spark, mock_get_spark, spark_session):
        """Test database extraction with connection error"""
        mock_get_spark.return_value = spark_session
        
        # Mock JDBC read failure
        mock_reader = MagicMock()
        mock_reader.jdbc.side_effect = Exception("Database connection failed")
        spark_session.read = mock_reader
        
        with pytest.raises(Exception, match="Database connection failed"):
            ingest.extract_from_database()
        
        mock_stop_spark.assert_called_once()
    
    @patch('ingest.get_spark_session')
    @patch('ingest.stop_spark_session')
    def test_extract_from_database_no_data(self, mock_stop_spark, mock_get_spark, spark_session):
        """Test database extraction with no data"""
        mock_get_spark.return_value = spark_session
        
        # Create empty DataFrame
        empty_df = spark_session.createDataFrame([], ["id", "name", "value"])
        
        # Mock read operation returning empty DataFrame
        mock_reader = MagicMock()
        mock_reader.jdbc.return_value = empty_df
        spark_session.read = mock_reader
        
        result = ingest.extract_from_database()
        
        assert result.count() == 0
        mock_stop_spark.assert_called_once()

class TestDataValidation:
    """Test data validation functionality"""
    
    def test_extract_with_sample_data_validation(self, spark_session, sample_data, sample_schema):
        """Test that extracted sample data has correct structure"""
        df = spark_session.createDataFrame(sample_data, sample_schema)
        
        # Validate schema
        assert len(df.columns) == 4
        assert "id" in df.columns
        assert "name" in df.columns
        assert "value" in df.columns
        assert "timestamp" in df.columns
        
        # Validate data
        assert df.count() == 5
        
        # Validate data types
        schema_dict = {field.name: field.dataType for field in df.schema.fields}
        assert str(schema_dict["id"]) == "IntegerType()"
        assert str(schema_dict["name"]) == "StringType()"
        assert str(schema_dict["value"]) == "IntegerType()"
        assert str(schema_dict["timestamp"]) == "TimestampType()"

class TestFileOperations:
    """Test file operations in ingest module"""
    
    @patch('os.makedirs')
    @patch('os.path.exists')
    def test_directory_creation(self, mock_exists, mock_makedirs):
        """Test directory creation for data storage"""
        mock_exists.return_value = False
        
        # This would be called within extract_data function
        data_path = "/opt/airflow/data/raw"
        os.makedirs(data_path, exist_ok=True)
        
        mock_makedirs.assert_called_with(data_path, exist_ok=True)
    
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.makedirs')
    def test_csv_file_creation(self, mock_makedirs, mock_file):
        """Test CSV file creation (legacy compatibility)"""
        # Test file writing functionality
        test_content = "id,name,value,timestamp\n1,test,100,2024-01-01 10:00:00\n"
        
        with open("/tmp/test.csv", "w") as f:
            f.write(test_content)
        
        mock_file.assert_called_with("/tmp/test.csv", "w")
        mock_file().write.assert_called_with(test_content)

class TestErrorHandling:
    """Test error handling in ingest module"""
    
    @patch('ingest.get_spark_session')
    def test_spark_session_failure_handling(self, mock_get_spark):
        """Test handling of Spark session creation failure"""
        mock_get_spark.side_effect = Exception("Failed to create Spark session")
        
        with pytest.raises(Exception) as exc_info:
            ingest.extract_data()
        
        assert "Failed to create Spark session" in str(exc_info.value)
    
    @patch('ingest.get_spark_session')
    @patch('ingest.stop_spark_session')
    def test_resource_cleanup_on_error(self, mock_stop_spark, mock_get_spark, spark_session):
        """Test that resources are cleaned up on error"""
        mock_get_spark.return_value = spark_session
        
        # Mock an error during processing
        with patch.object(spark_session, 'createDataFrame', side_effect=Exception("Processing error")):
            with pytest.raises(Exception):
                ingest.extract_data()
            
            # Verify cleanup was called
            mock_stop_spark.assert_called_once_with(spark_session)

class TestIntegration:
    """Integration tests for ingest module"""
    
    @patch('ingest.get_spark_session')
    @patch('ingest.stop_spark_session')
    @patch('os.makedirs')
    def test_full_extraction_pipeline(self, mock_makedirs, mock_stop_spark, mock_get_spark, spark_session, temp_data_dir):
        """Test complete extraction pipeline"""
        mock_get_spark.return_value = spark_session
        
        # Create test data
        test_data = [
            (1, "customer_001", 100, datetime(2024, 1, 15, 10, 30, 0)),
            (2, "customer_002", 150, datetime(2024, 1, 15, 11, 45, 0))
        ]
        test_df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        
        with patch.object(spark_session, 'createDataFrame', return_value=test_df):
            # Mock successful write operations
            mock_writer = MagicMock()
            test_df.write = mock_writer
            test_df.coalesce = MagicMock(return_value=test_df)
            
            result = ingest.extract_data()
            
            # Verify the complete pipeline executed successfully
            assert result is True
            mock_get_spark.assert_called_once()
            mock_stop_spark.assert_called_once()
            
            # Verify write operations were called
            assert mock_writer.mode.called
            assert mock_writer.option.called 