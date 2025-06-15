"""
Unit tests for spark_config module
"""
import pytest
import os
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Add parent directory to path for imports
import sys
sys.path.append('/opt/airflow/scripts')

from spark_config import (
    SparkConfig, 
    get_spark_session, 
    stop_spark_session, 
    get_database_config
)

class TestSparkConfig:
    """Test SparkConfig class methods"""
    
    def test_get_spark_config(self):
        """Test Spark configuration creation"""
        conf = SparkConfig.get_spark_config()
        
        assert isinstance(conf, SparkConf)
        assert conf.get("spark.app.name") == "ELT_Pipeline"
        assert conf.get("spark.sql.adaptive.enabled") == "true"
        assert conf.get("spark.serializer") == "org.apache.spark.serializer.KryoSerializer"
        assert "postgresql" in conf.get("spark.jars.packages")
    
    def test_create_spark_session_default(self):
        """Test default Spark session creation"""
        session = SparkConfig.create_spark_session()
        
        assert isinstance(session, SparkSession)
        assert session.conf.get("spark.app.name") == "ELT_Pipeline"
        
        session.stop()
    
    def test_create_spark_session_custom_name(self):
        """Test Spark session creation with custom name"""
        session = SparkConfig.create_spark_session("TestApp")
        
        assert isinstance(session, SparkSession)
        assert session.conf.get("spark.app.name") == "TestApp"
        
        session.stop()
    
    def test_create_jdbc_properties(self, mock_env_vars):
        """Test JDBC properties creation"""
        properties = SparkConfig.create_jdbc_properties()
        
        assert properties["user"] == "test_user"
        assert properties["password"] == "test_password"
        assert properties["driver"] == "org.postgresql.Driver"
        assert properties["stringtype"] == "unspecified"
        assert properties["batchsize"] == "10000"
    
    def test_create_jdbc_url(self, mock_env_vars):
        """Test JDBC URL creation"""
        url = SparkConfig.create_jdbc_url()
        expected_url = "jdbc:postgresql://localhost:5432/test_db"
        
        assert url == expected_url
    
    def test_optimize_for_write(self, spark_session):
        """Test write optimization configuration"""
        optimized_session = SparkConfig.optimize_for_write(spark_session)
        
        assert optimized_session.conf.get("spark.sql.adaptive.coalescePartitions.enabled") == "true"
        assert optimized_session.conf.get("spark.sql.adaptive.coalescePartitions.minPartitionNum") == "1"
    
    def test_optimize_for_read(self, spark_session):
        """Test read optimization configuration"""
        optimized_session = SparkConfig.optimize_for_read(spark_session)
        
        assert optimized_session.conf.get("spark.sql.adaptive.enabled") == "true"
        assert optimized_session.conf.get("spark.sql.adaptive.coalescePartitions.enabled") == "true"


class TestUtilityFunctions:
    """Test utility functions"""
    
    def test_get_spark_session(self):
        """Test get_spark_session utility function"""
        session = get_spark_session("TestUtility")
        
        assert isinstance(session, SparkSession)
        assert session.conf.get("spark.app.name") == "TestUtility"
        
        session.stop()
    
    def test_stop_spark_session_valid(self):
        """Test stopping a valid Spark session"""
        session = get_spark_session("TestStop")
        
        # Should not raise any exception
        stop_spark_session(session)
    
    def test_stop_spark_session_none(self):
        """Test stopping None session"""
        # Should not raise any exception
        stop_spark_session(None)
    
    def test_get_database_config(self, mock_env_vars):
        """Test database configuration retrieval"""
        config = get_database_config()
        
        assert "jdbc_url" in config
        assert "properties" in config
        assert config["jdbc_url"] == "jdbc:postgresql://localhost:5432/test_db"
        assert config["properties"]["user"] == "test_user"


class TestEnvironmentVariables:
    """Test environment variable handling"""
    
    def test_default_values_no_env(self):
        """Test default values when no environment variables are set"""
        with patch.dict(os.environ, {}, clear=True):
            properties = SparkConfig.create_jdbc_properties()
            url = SparkConfig.create_jdbc_url()
            
            assert properties["user"] == "elt_user"
            assert properties["password"] == "elt_password"
            assert "localhost" in url
            assert "5433" in url
            assert "elt_db" in url
    
    @patch.dict(os.environ, {
        'POSTGRES_HOST': 'custom-host',
        'POSTGRES_PORT': '9999',
        'POSTGRES_DB': 'custom_db',
        'POSTGRES_USER': 'custom_user',
        'POSTGRES_PASSWORD': 'custom_pass'
    })
    def test_custom_environment_variables(self):
        """Test custom environment variables"""
        properties = SparkConfig.create_jdbc_properties()
        url = SparkConfig.create_jdbc_url()
        
        assert properties["user"] == "custom_user"
        assert properties["password"] == "custom_pass"
        assert "custom-host" in url
        assert "9999" in url
        assert "custom_db" in url


class TestSparkConfigIntegration:
    """Integration tests for SparkConfig"""
    
    def test_full_spark_session_lifecycle(self):
        """Test complete Spark session lifecycle"""
        # Create session
        session = SparkConfig.create_spark_session("LifecycleTest")
        
        # Verify it's working
        assert session.sparkContext.appName == "LifecycleTest"
        
        # Create a simple DataFrame to test functionality
        data = [("test", 1), ("data", 2)]
        df = session.createDataFrame(data, ["col1", "col2"])
        assert df.count() == 2
        
        # Stop session
        session.stop()
    
    def test_spark_config_with_custom_settings(self):
        """Test Spark configuration with additional custom settings"""
        conf = SparkConfig.get_spark_config()
        conf.set("spark.test.custom", "value")
        
        session = SparkSession.builder.config(conf=conf).getOrCreate()
        
        assert session.conf.get("spark.test.custom") == "value"
        assert session.conf.get("spark.app.name") == "ELT_Pipeline"
        
        session.stop() 