"""
Data Ingestion Script using PySpark
Handles data extraction from various sources
"""
import logging
import os
from datetime import datetime
from spark_config import get_spark_session, stop_spark_session, get_database_config
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Removed - using centralized spark configuration

def extract_data():
    """
    Extract data from source systems using PySpark
    """
    logger.info("Starting PySpark data extraction process")
    
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session("ELT_Data_Ingestion")
        logger.info("Spark session created successfully")
        
        # Read customer churn data from input folder
        input_path = "/opt/airflow/data/input/customer_churn_data.csv"
        
        # Check if input file exists
        if not os.path.exists(input_path):
            logger.error(f"Input file not found: {input_path}")
            raise FileNotFoundError(f"Input file not found: {input_path}")
        
        logger.info(f"Reading customer churn data from: {input_path}")
        
        # Read CSV file with PySpark
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(input_path)
        
        # Add extraction metadata
        df = df.withColumn("extraction_timestamp", current_timestamp()) \
               .withColumn("source_system", lit("customer_churn_csv"))
        
        # Log data information
        record_count = df.count()
        column_count = len(df.columns)
        logger.info(f"Successfully loaded DataFrame with {record_count} records and {column_count} columns")
        
        # Print schema information
        logger.info("Data schema:")
        df.printSchema()
        
        # Show sample data
        logger.info("Sample of loaded data:")
        df.show(5, truncate=False)
        
        # Save to data lake (Parquet format for better performance)
        data_path = "/opt/airflow/data/raw"
        os.makedirs(data_path, exist_ok=True)
        
        output_path = f"{data_path}/customer_churn_extracted_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Write as Parquet (better for PySpark processing)
        df.write \
          .mode("overwrite") \
          .option("compression", "snappy") \
          .parquet(output_path)
        
        logger.info(f"Customer churn data written to: {output_path}")
        
        # Also save as CSV for compatibility
        csv_path = f"{data_path}/customer_churn_extracted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.coalesce(1) \
          .write \
          .mode("overwrite") \
          .option("header", "true") \
          .csv(csv_path)
        
        logger.info(f"CSV data written to: {csv_path}")
        
        # Data validation
        validate_customer_churn_data(df)
        
        logger.info("Customer churn data extraction completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error during customer churn data extraction: {str(e)}")
        raise
    finally:
        stop_spark_session(spark)
        logger.info("Spark session stopped")

def validate_customer_churn_data(df):
    """
    Validate the customer churn data structure and quality
    """
    logger.info("Validating customer churn data...")
    
    # Expected columns based on the actual dataset
    expected_columns = ['CustomerID', 'Age', 'Gender', 'Tenure', 'MonthlyCharges', 
                       'ContractType', 'InternetService', 'TotalCharges', 'TechSupport', 'Churn']
    
    actual_columns = df.columns
    logger.info(f"Columns found: {actual_columns}")
    
    # Validate required columns exist
    missing_columns = set(expected_columns) - set(actual_columns)
    if missing_columns:
        logger.warning(f"Missing expected columns: {missing_columns}")
    else:
        logger.info("All expected columns are present")
    
    # Check data quality
    null_counts = df.select([col for col in df.columns]).agg(
        *[sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
    ).collect()[0].asDict()
    
    logger.info("Null counts per column:")
    for col_name, null_count in null_counts.items():
        if null_count > 0:
            logger.warning(f"  {col_name}: {null_count} null values")
        else:
            logger.info(f"  {col_name}: {null_count} null values")
    
    # Show churn distribution
    if 'Churn' in actual_columns:
        churn_counts = df.groupBy('Churn').count().collect()
        logger.info("Churn distribution:")
        for row in churn_counts:
            logger.info(f"  {row['Churn']}: {row['count']} customers")
    
    # Show basic statistics for numeric columns
    numeric_columns = ['Age', 'Tenure', 'MonthlyCharges', 'TotalCharges']
    existing_numeric = [col for col in numeric_columns if col in actual_columns]
    
    if existing_numeric:
        logger.info("Basic statistics for numeric columns:")
        df.select(existing_numeric).describe().show()
    
    logger.info("Data validation completed")
    return True

def extract_from_database():
    """
    Extract data from database using PySpark JDBC
    """
    spark = None
    try:
        spark = get_spark_session("ELT_Database_Extract")
        
        # Get database configuration
        db_config = get_database_config()
        
        # Read from database (example with source table)
        source_url = f"jdbc:postgresql://{os.getenv('SOURCE_DB_HOST', 'localhost')}:{os.getenv('SOURCE_DB_PORT', '5432')}/{os.getenv('SOURCE_DB_NAME', 'source_db')}"
        source_properties = {
            "user": os.getenv('SOURCE_DB_USER', 'postgres'),
            "password": os.getenv('SOURCE_DB_PASSWORD', 'password'),
            "driver": "org.postgresql.Driver"
        }
        
        # Read from database
        df = spark.read \
            .jdbc(url=source_url, table="source_table", properties=source_properties)
        
        logger.info(f"Extracted {df.count()} records from database")
        return df
        
    except Exception as e:
        logger.error(f"Error extracting from database: {str(e)}")
        raise
    finally:
        stop_spark_session(spark)

if __name__ == "__main__":
    extract_data() 