"""
PySpark Configuration Module
Centralized Spark session management and configuration
"""
import os
import findspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Initialize findspark
findspark.init()

class SparkConfig:
    """
    Centralized Spark configuration class
    """
    
    @staticmethod
    def get_spark_config():
        """
        Get optimized Spark configuration
        """
        conf = SparkConf()
        
        # Basic configuration
        conf.set("spark.app.name", "ELT_Pipeline")
        conf.set("spark.sql.adaptive.enabled", "true")
        conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        
        # Memory configuration
        conf.set("spark.executor.memory", "2g")
        conf.set("spark.driver.memory", "1g")
        conf.set("spark.driver.maxResultSize", "1g")
        
        # Serialization
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        
        # Network configuration
        conf.set("spark.network.timeout", "600s")
        conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        
        # Database JDBC configuration
        conf.set("spark.jars.packages", "org.postgresql:postgresql:42.7.1")
        
        # Logging configuration
        conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        
        return conf
    
    @staticmethod
    def create_spark_session(app_name="ELT_Pipeline", enable_hive=False):
        """
        Create and configure Spark session
        """
        builder = SparkSession.builder
        
        # Set application name
        builder = builder.appName(app_name)
        
        # Apply configuration
        conf = SparkConfig.get_spark_config()
        builder = builder.config(conf=conf)
        
        # Enable Hive support if requested
        if enable_hive:
            builder = builder.enableHiveSupport()
        
        # Create session
        spark = builder.getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    @staticmethod
    def create_jdbc_properties():
        """
        Create JDBC connection properties for PostgreSQL
        """
        return {
            "user": os.getenv('POSTGRES_USER', 'elt_user'),
            "password": os.getenv('POSTGRES_PASSWORD', 'elt_password'),
            "driver": "org.postgresql.Driver",
            "stringtype": "unspecified",
            "batchsize": "10000",
            "isolationLevel": "READ_COMMITTED"
        }
    
    @staticmethod
    def create_jdbc_url():
        """
        Create JDBC URL for PostgreSQL connection
        """
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5433')
        database = os.getenv('POSTGRES_DB', 'elt_db')
        
        return f"jdbc:postgresql://{host}:{port}/{database}"
    
    @staticmethod
    def optimize_for_write(spark_session):
        """
        Optimize Spark session for write operations
        """
        spark_session.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark_session.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
        spark_session.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "4")
        
        return spark_session
    
    @staticmethod
    def optimize_for_read(spark_session):
        """
        Optimize Spark session for read operations
        """
        spark_session.conf.set("spark.sql.adaptive.enabled", "true")
        spark_session.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark_session.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        
        return spark_session

# Utility functions for common Spark operations
def get_spark_session(app_name="ELT_Pipeline"):
    """
    Get or create Spark session
    """
    return SparkConfig.create_spark_session(app_name)

def stop_spark_session(spark):
    """
    Safely stop Spark session
    """
    if spark:
        spark.stop()

def get_database_config():
    """
    Get database configuration
    """
    return {
        'jdbc_url': SparkConfig.create_jdbc_url(),
        'properties': SparkConfig.create_jdbc_properties()
    } 