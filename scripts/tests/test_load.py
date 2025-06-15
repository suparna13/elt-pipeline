"""
Unit tests for load module
"""
import pytest
import os
from unittest.mock import patch, MagicMock, call
from datetime import datetime

# Add parent directory to path for imports
import sys
sys.path.append('/opt/airflow/scripts')

import load

class TestLoadToDatabase:
    """Test database loading functionality"""
    
    @patch('load.get_spark_session')
    @patch('load.stop_spark_session')
    @patch('load.get_database_config')
    @patch('os.listdir')
    @patch('os.path.isdir')
    def test_load_to_database_parquet_success(self, mock_isdir, mock_listdir, mock_db_config_patch, mock_stop_spark, mock_get_spark, spark_session, sample_dataframe, mock_db_config):
        """Test successful loading of Parquet files to database"""
        mock_get_spark.return_value = spark_session
        mock_db_config_patch.return_value = mock_db_config
        mock_listdir.return_value = ['processed_data_20240115_103000']
        mock_isdir.return_value = True
        
        # Mock read operations
        mock_reader = MagicMock()
        mock_reader.parquet.return_value = sample_dataframe
        spark_session.read = mock_reader
        
        # Mock write operations
        mock_writer = MagicMock()
        sample_dataframe.write = mock_writer
        
        result = load.load_to_database()
        
        assert result is True
        mock_get_spark.assert_called_once_with("ELT_Database_Load")
        mock_stop_spark.assert_called_once()
        mock_reader.parquet.assert_called()
        mock_writer.jdbc.assert_called()
    
    @patch('load.get_spark_session')
    @patch('load.stop_spark_session')
    @patch('load.get_database_config')
    @patch('os.listdir')
    @patch('os.path.isdir')
    def test_load_to_database_csv_success(self, mock_isdir, mock_listdir, mock_db_config_patch, mock_stop_spark, mock_get_spark, spark_session, sample_dataframe, mock_db_config):
        """Test successful loading of CSV files to database"""
        mock_get_spark.return_value = spark_session
        mock_db_config_patch.return_value = mock_db_config
        mock_listdir.side_effect = [[], ['processed_data.csv']]  # No parquet dirs, CSV files
        mock_isdir.return_value = False
        
        # Mock read operations
        mock_reader = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_reader.csv.return_value = sample_dataframe
        spark_session.read = mock_reader
        
        # Mock write operations
        mock_writer = MagicMock()
        sample_dataframe.write = mock_writer
        
        result = load.load_to_database()
        
        assert result is True
        mock_reader.csv.assert_called()
        mock_writer.jdbc.assert_called()
    
    @patch('load.get_spark_session')
    @patch('load.stop_spark_session')
    def test_load_to_database_spark_error(self, mock_stop_spark, mock_get_spark):
        """Test database loading with Spark session error"""
        mock_get_spark.side_effect = Exception("Spark session failed")
        
        with pytest.raises(Exception, match="Spark session failed"):
            load.load_to_database()
        
        mock_stop_spark.assert_called_once()
    
    @patch('load.get_spark_session')
    @patch('load.stop_spark_session')
    @patch('load.get_database_config')
    @patch('os.listdir')
    @patch('os.path.isdir')
    def test_load_to_database_write_error(self, mock_isdir, mock_listdir, mock_db_config_patch, mock_stop_spark, mock_get_spark, spark_session, sample_dataframe, mock_db_config):
        """Test database loading with write error"""
        mock_get_spark.return_value = spark_session
        mock_db_config_patch.return_value = mock_db_config
        mock_listdir.return_value = ['processed_data_20240115_103000']
        mock_isdir.return_value = True
        
        # Mock read operations
        mock_reader = MagicMock()
        mock_reader.parquet.return_value = sample_dataframe
        spark_session.read = mock_reader
        
        # Mock write failure
        mock_writer = MagicMock()
        mock_writer.jdbc.side_effect = Exception("Database write failed")
        sample_dataframe.write = mock_writer
        
        with pytest.raises(Exception, match="Database write failed"):
            load.load_to_database()
        
        mock_stop_spark.assert_called_once()

class TestLoadWithOptimizations:
    """Test loading with performance optimizations"""
    
    @patch('load.get_spark_session')
    @patch('load.stop_spark_session')
    @patch('load.get_database_config')
    @patch('os.listdir')
    @patch('os.path.isdir')
    def test_load_with_batch_optimization(self, mock_isdir, mock_listdir, mock_db_config_patch, mock_stop_spark, mock_get_spark, spark_session, sample_dataframe, mock_db_config):
        """Test loading with batch size optimization"""
        mock_get_spark.return_value = spark_session
        mock_db_config_patch.return_value = mock_db_config
        mock_listdir.return_value = ['processed_data_20240115_103000']
        mock_isdir.return_value = True
        
        # Mock optimized Spark session
        optimized_spark = MagicMock()
        with patch('load.SparkConfig.optimize_for_write', return_value=optimized_spark):
            # Mock read operations
            mock_reader = MagicMock()
            mock_reader.parquet.return_value = sample_dataframe
            optimized_spark.read = mock_reader
            
            # Mock write operations
            mock_writer = MagicMock()
            sample_dataframe.write = mock_writer
            
            result = load.load_to_database()
            
            assert result is True
            mock_writer.mode.assert_called_with("append")
            mock_writer.option.assert_called()  # Batch size options
    
    @patch('load.get_spark_session')
    @patch('load.stop_spark_session')
    @patch('load.get_database_config')
    @patch('os.listdir') 
    def test_load_with_partitioning(self, mock_listdir, mock_db_config_patch, mock_stop_spark, mock_get_spark, spark_session, mock_db_config):
        """Test loading with partitioned data"""
        mock_get_spark.return_value = spark_session
        mock_db_config_patch.return_value = mock_db_config
        mock_listdir.return_value = ['processed_data_20240115_103000']
        
        # Create partitioned test data
        test_data = [
            (1, "customer_001", 100, datetime(2024, 1, 15), 2024, 1),
            (2, "customer_002", 150, datetime(2024, 2, 15), 2024, 2)
        ]
        partitioned_df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp", "year", "month"])
        
        with patch('os.path.isdir', return_value=True):
            # Mock read operations
            mock_reader = MagicMock()
            mock_reader.parquet.return_value = partitioned_df
            spark_session.read = mock_reader
            
            # Mock write operations
            mock_writer = MagicMock()
            partitioned_df.write = mock_writer
            
            result = load.load_to_database()
            
            assert result is True
            # Verify partitioned write was attempted
            mock_writer.jdbc.assert_called()

class TestCreateTableIfNotExists:
    """Test table creation functionality"""
    
    @patch('load.get_database_config')
    def test_create_table_if_not_exists_success(self, mock_db_config_patch, mock_psycopg2, mock_env_vars):
        """Test successful table creation"""
        mock_db_config_patch.return_value = {
            'jdbc_url': 'jdbc:postgresql://localhost:5432/test_db',
            'properties': {'user': 'test_user', 'password': 'test_password'}
        }
        
        mock_conn, mock_cursor = mock_psycopg2
        mock_cursor.fetchone.return_value = None  # Table doesn't exist
        
        result = load.create_table_if_not_exists("test_table")
        
        assert result is True
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called()
    
    @patch('load.get_database_config')
    def test_create_table_if_not_exists_already_exists(self, mock_db_config_patch, mock_psycopg2, mock_env_vars):
        """Test table creation when table already exists"""
        mock_db_config_patch.return_value = {
            'jdbc_url': 'jdbc:postgresql://localhost:5432/test_db',
            'properties': {'user': 'test_user', 'password': 'test_password'}
        }
        
        mock_conn, mock_cursor = mock_psycopg2
        mock_cursor.fetchone.return_value = ("test_table",)  # Table exists
        
        result = load.create_table_if_not_exists("test_table")
        
        assert result is True
        # Should not attempt to create table
        create_calls = [call for call in mock_cursor.execute.call_args_list 
                       if 'CREATE TABLE' in str(call)]
        assert len(create_calls) == 0
    
    @patch('load.get_database_config')
    def test_create_table_if_not_exists_error(self, mock_db_config_patch, mock_psycopg2, mock_env_vars):
        """Test table creation with database error"""
        mock_db_config_patch.return_value = {
            'jdbc_url': 'jdbc:postgresql://localhost:5432/test_db',
            'properties': {'user': 'test_user', 'password': 'test_password'}
        }
        
        mock_conn, mock_cursor = mock_psycopg2
        mock_cursor.execute.side_effect = Exception("Database error")
        
        with pytest.raises(Exception, match="Database error"):
            load.create_table_if_not_exists("test_table")

class TestValidateData:
    """Test data validation before loading"""
    
    def test_validate_data_success(self, sample_dataframe):
        """Test successful data validation"""
        result = load.validate_data(sample_dataframe)
        
        assert result is True
    
    def test_validate_data_empty_dataframe(self, spark_session):
        """Test validation with empty DataFrame"""
        empty_df = spark_session.createDataFrame([], ["id", "name", "value", "timestamp"])
        
        result = load.validate_data(empty_df)
        
        assert result is False
    
    def test_validate_data_missing_columns(self, spark_session):
        """Test validation with missing required columns"""
        incomplete_data = [(1, "customer_001")]
        incomplete_df = spark_session.createDataFrame(incomplete_data, ["id", "name"])
        
        result = load.validate_data(incomplete_df)
        
        assert result is False
    
    def test_validate_data_null_values(self, spark_session):
        """Test validation with null values in critical columns"""
        test_data = [
            (None, "customer_001", 100, datetime.now()),  # Null ID
            (2, "customer_002", 150, datetime.now())
        ]
        df_with_nulls = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        
        result = load.validate_data(df_with_nulls)
        
        # Should pass validation (nulls are handled in cleaning)
        assert result is True

class TestLoadPerformance:
    """Test loading performance optimizations"""
    
    @patch('load.get_spark_session')
    @patch('load.stop_spark_session')
    @patch('load.get_database_config')
    @patch('os.listdir')
    @patch('os.path.isdir')
    def test_load_with_repartitioning(self, mock_isdir, mock_listdir, mock_db_config_patch, mock_stop_spark, mock_get_spark, spark_session, mock_db_config):
        """Test loading with DataFrame repartitioning"""
        mock_get_spark.return_value = spark_session
        mock_db_config_patch.return_value = mock_db_config
        mock_listdir.return_value = ['processed_data_20240115_103000']
        mock_isdir.return_value = True
        
        # Create large test dataset
        test_data = [(i, f"customer_{i}", i * 10, datetime.now()) for i in range(1, 101)]
        large_df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        
        # Mock read operations
        mock_reader = MagicMock()
        mock_reader.parquet.return_value = large_df
        spark_session.read = mock_reader
        
        # Mock repartition
        repartitioned_df = MagicMock()
        large_df.repartition = MagicMock(return_value=repartitioned_df)
        
        # Mock write operations
        mock_writer = MagicMock()
        repartitioned_df.write = mock_writer
        
        result = load.load_to_database()
        
        assert result is True
        # Verify repartitioning was called for optimization
        large_df.repartition.assert_called()
    
    def test_batch_size_calculation(self):
        """Test batch size calculation based on DataFrame size"""
        # This would test internal batch size logic
        # Since batch size is configurable, we test the configuration
        from spark_config import SparkConfig
        
        properties = SparkConfig.create_jdbc_properties()
        batch_size = int(properties.get('batchsize', '1000'))
        
        assert batch_size > 0
        assert batch_size <= 50000  # Reasonable upper limit

class TestIntegrationLoad:
    """Integration tests for load functionality"""
    
    @patch('load.get_spark_session')
    @patch('load.stop_spark_session')
    @patch('load.get_database_config')
    @patch('load.create_table_if_not_exists')
    @patch('os.listdir')
    @patch('os.path.isdir')
    def test_full_load_pipeline(self, mock_isdir, mock_listdir, mock_create_table, mock_db_config_patch, mock_stop_spark, mock_get_spark, spark_session, sample_dataframe, mock_db_config):
        """Test complete load pipeline"""
        mock_get_spark.return_value = spark_session
        mock_db_config_patch.return_value = mock_db_config
        mock_create_table.return_value = True
        mock_listdir.return_value = ['processed_data_20240115_103000']
        mock_isdir.return_value = True
        
        # Mock read operations
        mock_reader = MagicMock()
        mock_reader.parquet.return_value = sample_dataframe
        spark_session.read = mock_reader
        
        # Mock write operations
        mock_writer = MagicMock()
        sample_dataframe.write = mock_writer
        
        result = load.load_to_database()
        
        # Verify complete pipeline executed
        assert result is True
        mock_get_spark.assert_called_once()
        mock_create_table.assert_called()
        mock_reader.parquet.assert_called()
        mock_writer.jdbc.assert_called()
        mock_stop_spark.assert_called_once()
    
    @patch('load.get_spark_session')
    @patch('load.stop_spark_session')
    @patch('load.get_database_config')
    @patch('os.listdir')
    def test_load_multiple_files(self, mock_listdir, mock_db_config_patch, mock_stop_spark, mock_get_spark, spark_session, sample_dataframe, mock_db_config):
        """Test loading multiple processed files"""
        mock_get_spark.return_value = spark_session
        mock_db_config_patch.return_value = mock_db_config
        mock_listdir.return_value = [
            'processed_data_20240115_103000',
            'processed_data_20240115_110000',
            'processed_data_20240115_113000'
        ]
        
        with patch('os.path.isdir', return_value=True):
            # Mock read operations
            mock_reader = MagicMock()
            mock_reader.parquet.return_value = sample_dataframe
            spark_session.read = mock_reader
            
            # Mock write operations
            mock_writer = MagicMock()
            sample_dataframe.write = mock_writer
            
            result = load.load_to_database()
            
            assert result is True
            # Verify multiple files were processed
            assert mock_reader.parquet.call_count == 3
            assert mock_writer.jdbc.call_count == 3

class TestErrorRecovery:
    """Test error recovery and resilience"""
    
    @patch('load.get_spark_session')
    @patch('load.stop_spark_session')
    @patch('load.get_database_config')
    @patch('os.listdir')
    @patch('os.path.isdir')
    def test_partial_failure_recovery(self, mock_isdir, mock_listdir, mock_db_config_patch, mock_stop_spark, mock_get_spark, spark_session, sample_dataframe, mock_db_config):
        """Test recovery from partial load failures"""
        mock_get_spark.return_value = spark_session
        mock_db_config_patch.return_value = mock_db_config
        mock_listdir.return_value = [
            'processed_data_20240115_103000',
            'processed_data_20240115_110000'
        ]
        mock_isdir.return_value = True
        
        # Mock read operations
        mock_reader = MagicMock()
        mock_reader.parquet.return_value = sample_dataframe
        spark_session.read = mock_reader
        
        # Mock write operations with first failure, second success
        mock_writer = MagicMock()
        mock_writer.jdbc.side_effect = [Exception("First load failed"), None]
        sample_dataframe.write = mock_writer
        
        # Should handle the error and continue with remaining files
        with pytest.raises(Exception):
            load.load_to_database()
        
        mock_stop_spark.assert_called_once()
    
    @patch('load.get_spark_session')
    @patch('load.stop_spark_session')
    def test_cleanup_on_error(self, mock_stop_spark, mock_get_spark, spark_session):
        """Test resource cleanup on errors"""
        mock_get_spark.return_value = spark_session
        
        # Mock error during processing
        with patch('os.listdir', side_effect=Exception("File system error")):
            with pytest.raises(Exception, match="File system error"):
                load.load_to_database()
            
            # Verify cleanup was called
            mock_stop_spark.assert_called_once_with(spark_session) 