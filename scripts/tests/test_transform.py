"""
Unit tests for transform module
"""
import pytest
import os
import tempfile
from unittest.mock import patch, MagicMock
from pyspark.sql.functions import col, when
from datetime import datetime

# Add parent directory to path for imports
import sys
sys.path.append('/opt/airflow/scripts')

import transform

class TestDataQualityCheck:
    """Test data quality check functionality"""
    
    def test_data_quality_check_basic(self, sample_dataframe):
        """Test basic data quality checks"""
        stats = transform.data_quality_check(sample_dataframe, "test_table")
        
        assert stats['total_rows'] == 5
        assert stats['distinct_rows'] == 5
        assert stats['duplicates'] == 0
        assert isinstance(stats['null_counts'], dict)
    
    def test_data_quality_check_with_nulls(self, spark_session):
        """Test data quality checks with null values"""
        # Create data with nulls
        test_data = [
            (1, "customer_001", 100, datetime.now()),
            (2, None, 150, datetime.now()),
            (3, "customer_003", None, datetime.now())
        ]
        
        df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        stats = transform.data_quality_check(df)
        
        assert stats['total_rows'] == 3
        assert stats['null_counts']['name'] == 1
        assert stats['null_counts']['value'] == 1
    
    def test_data_quality_check_with_duplicates(self, spark_session):
        """Test data quality checks with duplicate rows"""
        # Create data with duplicates
        test_data = [
            (1, "customer_001", 100, datetime(2024, 1, 1)),
            (1, "customer_001", 100, datetime(2024, 1, 1)),  # Duplicate
            (2, "customer_002", 150, datetime(2024, 1, 2))
        ]
        
        df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        stats = transform.data_quality_check(df)
        
        assert stats['total_rows'] == 3
        assert stats['distinct_rows'] == 2
        assert stats['duplicates'] == 1

class TestDataCleaning:
    """Test data cleaning functionality"""
    
    def test_clean_and_validate_data_remove_nulls(self, spark_session):
        """Test removal of completely null rows"""
        test_data = [
            (1, "customer_001", 100, datetime.now()),
            (None, None, None, None),  # Completely null row
            (2, "customer_002", 150, datetime.now())
        ]
        
        df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        cleaned_df = transform.clean_and_validate_data(df)
        
        assert cleaned_df.count() == 2
    
    def test_clean_and_validate_data_string_cleaning(self, spark_session):
        """Test string column cleaning"""
        test_data = [
            (1, "  Customer_001  ", 100, datetime.now()),
            (2, "CUSTOMER_002", 150, datetime.now())
        ]
        
        df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        cleaned_df = transform.clean_and_validate_data(df)
        
        # Check that strings are trimmed and lowercased
        names = [row.name for row in cleaned_df.collect()]
        assert "customer_001" in names
        assert "customer_002" in names
    
    def test_clean_and_validate_data_negative_values(self, spark_session):
        """Test removal of negative values"""
        test_data = [
            (1, "customer_001", 100, datetime.now()),
            (2, "customer_002", -50, datetime.now()),  # Negative value
            (3, "customer_003", 150, datetime.now())
        ]
        
        df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        cleaned_df = transform.clean_and_validate_data(df)
        
        # Should only have 2 rows (negative value removed)
        assert cleaned_df.count() == 2
        
        # Check that remaining values are positive
        values = [row.value for row in cleaned_df.collect()]
        assert all(v > 0 for v in values)
    
    def test_clean_and_validate_data_duplicates(self, spark_session):
        """Test removal of duplicate rows"""
        test_data = [
            (1, "customer_001", 100, datetime(2024, 1, 1)),
            (1, "customer_001", 100, datetime(2024, 1, 1)),  # Duplicate
            (2, "customer_002", 150, datetime(2024, 1, 2))
        ]
        
        df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        cleaned_df = transform.clean_and_validate_data(df)
        
        assert cleaned_df.count() == 2  # Duplicates removed

class TestDataEnrichment:
    """Test data enrichment functionality"""
    
    def test_enrich_data_metadata(self, sample_dataframe):
        """Test addition of processing metadata"""
        enriched_df = transform.enrich_data(sample_dataframe)
        
        # Check new columns exist
        assert "processed_timestamp" in enriched_df.columns
        assert "processing_version" in enriched_df.columns
        
        # Check metadata values
        row = enriched_df.first()
        assert row.processing_version == "1.0"
        assert row.processed_timestamp is not None
    
    def test_enrich_data_date_components(self, sample_dataframe):
        """Test extraction of date components"""
        enriched_df = transform.enrich_data(sample_dataframe)
        
        # Check date component columns exist
        assert "year" in enriched_df.columns
        assert "month" in enriched_df.columns
        assert "day" in enriched_df.columns
        
        # Verify date extraction
        row = enriched_df.first()
        assert row.year == 2024
        assert row.month == 1
        assert row.day == 15
    
    def test_enrich_data_value_categorization(self, sample_dataframe):
        """Test value categorization"""
        enriched_df = transform.enrich_data(sample_dataframe)
        
        # Check value_category column exists
        assert "value_category" in enriched_df.columns
        
        # Check categorization logic
        categories = [row.value_category for row in enriched_df.collect()]
        expected_categories = ["medium", "medium", "high", "low", "high"]  # Based on sample data
        
        assert categories == expected_categories
    
    def test_enrich_data_name_hashing(self, sample_dataframe):
        """Test name hashing for anonymization"""
        enriched_df = transform.enrich_data(sample_dataframe)
        
        # Check name_hash column exists
        assert "name_hash" in enriched_df.columns
        
        # Verify hashes are generated
        hashes = [row.name_hash for row in enriched_df.collect()]
        assert all(h is not None for h in hashes)
        assert len(set(hashes)) == 5  # All hashes should be unique

class TestTransformData:
    """Test main transform_data function"""
    
    @patch('transform.get_spark_session')
    @patch('transform.stop_spark_session')
    @patch('os.listdir')
    @patch('os.path.isdir')
    @patch('os.makedirs')
    def test_transform_data_parquet_processing(self, mock_makedirs, mock_isdir, mock_listdir, mock_stop_spark, mock_get_spark, spark_session, sample_dataframe):
        """Test transformation of Parquet files"""
        mock_get_spark.return_value = spark_session
        mock_listdir.return_value = ['extracted_data_20240115_103000']
        mock_isdir.return_value = True
        
        # Mock read operations
        mock_reader = MagicMock()
        mock_reader.parquet.return_value = sample_dataframe
        spark_session.read = mock_reader
        
        # Mock write operations
        mock_writer = MagicMock()
        sample_dataframe.write = mock_writer
        
        result = transform.transform_data()
        
        assert result is True
        mock_get_spark.assert_called_once()
        mock_stop_spark.assert_called_once()
        mock_reader.parquet.assert_called()
    
    @patch('transform.get_spark_session')
    @patch('transform.stop_spark_session')
    @patch('os.listdir')
    @patch('os.path.isdir')
    @patch('os.makedirs')
    def test_transform_data_csv_processing(self, mock_makedirs, mock_isdir, mock_listdir, mock_stop_spark, mock_get_spark, spark_session, sample_dataframe):
        """Test transformation of CSV files"""
        mock_get_spark.return_value = spark_session
        mock_listdir.side_effect = [[], ['test_data.csv']]  # No parquet dirs, one CSV file
        mock_isdir.return_value = False
        
        # Mock read operations
        mock_reader = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_reader.csv.return_value = sample_dataframe
        spark_session.read = mock_reader
        
        # Mock write operations
        mock_writer = MagicMock()
        sample_dataframe.write = mock_writer
        sample_dataframe.coalesce = MagicMock(return_value=sample_dataframe)
        
        result = transform.transform_data()
        
        assert result is True
        mock_reader.csv.assert_called()
    
    @patch('transform.get_spark_session')
    @patch('transform.stop_spark_session')
    def test_transform_data_error_handling(self, mock_stop_spark, mock_get_spark):
        """Test error handling in transform_data"""
        mock_get_spark.side_effect = Exception("Spark session failed")
        
        with pytest.raises(Exception, match="Spark session failed"):
            transform.transform_data()
        
        mock_stop_spark.assert_called_once()

class TestTransformWithSQL:
    """Test SQL-based transformation functionality"""
    
    @patch('transform.get_spark_session')
    @patch('transform.stop_spark_session')
    def test_transform_with_sql_success(self, mock_stop_spark, mock_get_spark, spark_session, sample_dataframe):
        """Test SQL transformation success"""
        mock_get_spark.return_value = spark_session
        
        # Mock read operation
        mock_reader = MagicMock()
        mock_reader.parquet.return_value = sample_dataframe
        spark_session.read = mock_reader
        
        # Mock SQL operation
        spark_session.sql = MagicMock(return_value=sample_dataframe)
        
        result = transform.transform_with_sql()
        
        assert result == sample_dataframe
        mock_stop_spark.assert_called_once()
        spark_session.sql.assert_called_once()
    
    @patch('transform.get_spark_session')
    @patch('transform.stop_spark_session')
    def test_transform_with_sql_error(self, mock_stop_spark, mock_get_spark, spark_session):
        """Test SQL transformation error handling"""
        mock_get_spark.return_value = spark_session
        
        # Mock read operation failure
        mock_reader = MagicMock()
        mock_reader.parquet.side_effect = Exception("Read failed")
        spark_session.read = mock_reader
        
        with pytest.raises(Exception, match="Read failed"):
            transform.transform_with_sql()
        
        mock_stop_spark.assert_called_once()

class TestComplexTransformations:
    """Test complex transformation scenarios"""
    
    def test_multiple_transformations_pipeline(self, spark_session):
        """Test multiple transformations in sequence"""
        # Create test data with various data quality issues
        test_data = [
            (1, "  Customer_001  ", 100, datetime(2024, 1, 15)),
            (2, "CUSTOMER_002", -50, datetime(2024, 1, 16)),  # Negative value
            (3, None, 150, datetime(2024, 1, 17)),  # Null name
            (1, "  Customer_001  ", 100, datetime(2024, 1, 15)),  # Duplicate
            (4, "customer_004", 200, datetime(2024, 1, 18))
        ]
        
        df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        
        # Apply transformations
        cleaned_df = transform.clean_and_validate_data(df)
        enriched_df = transform.enrich_data(cleaned_df)
        
        # Verify results
        assert enriched_df.count() == 2  # Only valid, unique records
        
        # Check data quality
        records = enriched_df.collect()
        for record in records:
            assert record.value > 0  # No negative values
            assert record.name is not None  # No null names
            assert record.name == record.name.lower().strip()  # Cleaned strings
            assert hasattr(record, 'value_category')  # Enrichment applied
    
    def test_large_dataset_simulation(self, spark_session):
        """Test transformation with larger dataset"""
        # Create larger test dataset
        test_data = [(i, f"customer_{i:03d}", i * 10, datetime(2024, 1, 15)) 
                     for i in range(1, 101)]
        
        df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        
        # Apply transformations
        cleaned_df = transform.clean_and_validate_data(df)
        enriched_df = transform.enrich_data(cleaned_df)
        
        # Verify results
        assert enriched_df.count() == 100
        
        # Check categorization distribution
        categories = [row.value_category for row in enriched_df.collect()]
        assert "low" in categories
        assert "medium" in categories
        assert "high" in categories

class TestPerformanceAndOptimization:
    """Test performance aspects of transformations"""
    
    def test_transformation_with_partitioning(self, spark_session):
        """Test transformation maintains proper partitioning"""
        # Create test data
        test_data = [(i, f"customer_{i}", i * 10, datetime(2024, 1, 15)) 
                     for i in range(1, 21)]
        
        df = spark_session.createDataFrame(test_data, ["id", "name", "value", "timestamp"])
        
        # Apply transformations
        enriched_df = transform.enrich_data(df)
        
        # Check that we have the expected structure for partitioning
        assert "year" in enriched_df.columns
        assert "month" in enriched_df.columns
        
        # Verify partitioning values
        year_values = [row.year for row in enriched_df.collect()]
        month_values = [row.month for row in enriched_df.collect()]
        
        assert all(year == 2024 for year in year_values)
        assert all(month == 1 for month in month_values) 