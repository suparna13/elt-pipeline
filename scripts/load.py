"""
Data Loading Script using PySpark
Handles loading transformed data into target systems using JDBC
"""
import logging
import os
from datetime import datetime
from spark_config import get_spark_session, stop_spark_session, get_database_config
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Removed - using centralized spark configuration

def create_tables_with_spark(spark, jdbc_url, properties):
    """
    Create necessary tables in the database using Spark
    """
    logger.info("Creating database tables using Spark")
    
    try:
        # Create processed_data table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS processed_data (
            id INTEGER,
            name VARCHAR(255),
            value NUMERIC,
            timestamp TIMESTAMP,
            extraction_timestamp TIMESTAMP,
            source_system VARCHAR(255),
            processed_timestamp TIMESTAMP,
            processing_version VARCHAR(50),
            year INTEGER,
            month INTEGER,
            day INTEGER,
            value_category VARCHAR(50),
            name_hash BIGINT,
            load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        # Execute using raw JDBC connection (since Spark doesn't directly support DDL via JDBC writer)
        import psycopg2
        
        connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5433'),
            database=os.getenv('POSTGRES_DB', 'elt_db'),
            user=os.getenv('POSTGRES_USER', 'elt_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'elt_password')
        )
        
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info("Database tables created successfully")
        
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        raise

def load_parquet_to_database(spark, parquet_path, table_name, jdbc_url, properties):
    """
    Load Parquet data to database using PySpark JDBC
    """
    logger.info(f"Loading Parquet data from {parquet_path} to table {table_name}")
    
    try:
        # Read Parquet data
        df = spark.read.parquet(parquet_path)
        
        # Add load metadata
        df = df.withColumn("load_timestamp", current_timestamp())
        
        # Show schema and sample data
        logger.info(f"DataFrame schema: {df.schema}")
        logger.info("Sample data:")
        df.show(5, truncate=False)
        
        # Get record count before loading
        record_count = df.count()
        logger.info(f"Loading {record_count} records to {table_name}")
        
        # Write to database using JDBC
        df.write \
          .mode("append") \
          .option("batchsize", "10000") \
          .option("isolationLevel", "READ_COMMITTED") \
          .jdbc(url=jdbc_url, table=table_name, properties=properties)
        
        logger.info(f"Successfully loaded {record_count} records to {table_name}")
        return record_count
        
    except Exception as e:
        logger.error(f"Error loading Parquet data: {str(e)}")
        raise

def load_csv_to_database(spark, csv_path, table_name, jdbc_url, properties):
    """
    Load CSV data to database using PySpark JDBC
    """
    logger.info(f"Loading CSV data from {csv_path} to table {table_name}")
    
    try:
        # Read CSV data with proper options
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
            .csv(csv_path)
        
        # Add load metadata
        df = df.withColumn("load_timestamp", current_timestamp())
        
        # Get record count before loading
        record_count = df.count()
        logger.info(f"Loading {record_count} records to {table_name}")
        
        # Write to database using JDBC
        df.write \
          .mode("append") \
          .option("batchsize", "10000") \
          .option("isolationLevel", "READ_COMMITTED") \
          .jdbc(url=jdbc_url, table=table_name, properties=properties)
        
        logger.info(f"Successfully loaded {record_count} records to {table_name}")
        return record_count
        
    except Exception as e:
        logger.error(f"Error loading CSV data: {str(e)}")
        raise

def load_data():
    """
    Load transformed data into target database using PySpark
    """
    logger.info("Starting PySpark data loading process")
    
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session("ELT_Data_Loading")
        logger.info("Spark session created successfully")
        
        # Get database configuration
        db_config = get_database_config()
        jdbc_url = db_config['jdbc_url']
        properties = db_config['properties']
        
        # Create tables
        create_tables_with_spark(spark, jdbc_url, properties)
        
        # Define paths
        processed_data_path = "/opt/airflow/data/processed"
        
        # Check if processed data directory exists
        if not os.path.exists(processed_data_path):
            logger.warning(f"Processed data directory {processed_data_path} does not exist")
            return True
        
        total_loaded = 0
        
        # Load Parquet files (preferred)
        parquet_dirs = [d for d in os.listdir(processed_data_path) 
                       if os.path.isdir(os.path.join(processed_data_path, d)) and 'transformed_' in d]
        
        for parquet_dir in parquet_dirs:
            parquet_path = os.path.join(processed_data_path, parquet_dir)
            record_count = load_parquet_to_database(
                spark, parquet_path, "processed_data", jdbc_url, properties
            )
            total_loaded += record_count
        
        # Load CSV files as fallback
        csv_files = [f for f in os.listdir(processed_data_path) if f.endswith('.csv')]
        
        for csv_file in csv_files:
            csv_path = os.path.join(processed_data_path, csv_file)
            record_count = load_csv_to_database(
                spark, csv_path, "processed_data", jdbc_url, properties
            )
            total_loaded += record_count
        
        logger.info(f"PySpark data loading completed successfully. Total records loaded: {total_loaded}")
        return True
        
    except Exception as e:
        logger.error(f"Error during PySpark data loading: {str(e)}")
        raise
    finally:
        stop_spark_session(spark)
        logger.info("Spark session stopped")

def load_with_partitioning():
    """
    Load data with partitioning for better performance
    """
    spark = None
    try:
        spark = get_spark_session("ELT_Partitioned_Load")
        db_config = get_database_config()
        jdbc_url = db_config['jdbc_url']
        properties = db_config['properties']
        
        # Read data
        df = spark.read.parquet("/opt/airflow/data/processed/*")
        
        # Partition by year and month for better performance
        df.write \
          .mode("append") \
          .option("batchsize", "10000") \
          .option("numPartitions", "4") \
          .partitionBy("year", "month") \
          .jdbc(url=jdbc_url, table="processed_data_partitioned", properties=properties)
          
        logger.info("Data loaded with partitioning")
        
    finally:
        stop_spark_session(spark)

def create_summary_tables():
    """
    Create summary/aggregation tables using Spark SQL
    """
    logger.info("Creating summary tables")
    
    spark = None
    try:
        spark = get_spark_session("ELT_Summary_Tables")
        db_config = get_database_config()
        jdbc_url = db_config['jdbc_url']
        properties = db_config['properties']
        
        # Read from main table
        df = spark.read \
            .jdbc(url=jdbc_url, table="processed_data", properties=properties)
        
        # Create temporary view
        df.createOrReplaceTempView("processed_data")
        
        # Create daily summary
        daily_summary = spark.sql("""
            SELECT 
                year,
                month,
                day,
                value_category,
                COUNT(*) as record_count,
                AVG(value) as avg_value,
                MIN(value) as min_value,
                MAX(value) as max_value,
                CURRENT_TIMESTAMP() as summary_timestamp
            FROM processed_data
            GROUP BY year, month, day, value_category
        """)
        
        # Save summary table
        daily_summary.write \
            .mode("overwrite") \
            .jdbc(url=jdbc_url, table="daily_summary", properties=properties)
        
        logger.info("Summary tables created successfully")
        
    except Exception as e:
        logger.error(f"Error creating summary tables: {str(e)}")
        raise
    finally:
        stop_spark_session(spark)

if __name__ == "__main__":
    load_data() 