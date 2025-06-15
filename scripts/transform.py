"""
Data Transformation Script using PySpark
Handles data cleaning, validation, and transformation
"""
import logging
import os
from datetime import datetime
from spark_config import get_spark_session, stop_spark_session
from pyspark.sql.functions import (
    col, when, isnan, isnull, year, month, dayofmonth, 
    current_timestamp, lit, regexp_replace, lower, trim,
    sum as spark_sum, count as spark_count, avg, stddev,
    hash, concat_ws
)
from pyspark.sql.types import IntegerType, DoubleType, StringType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Removed - using centralized spark configuration

def data_quality_check(df, table_name="DataFrame"):
    """
    Perform data quality checks on the DataFrame
    """
    logger.info(f"Performing data quality checks on {table_name}")
    
    total_rows = df.count()
    logger.info(f"Total rows in {table_name}: {total_rows}")
    
    # Check for null values in each column
    null_counts = {}
    for column in df.columns:
        null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
        null_counts[column] = null_count
        if null_count > 0:
            logger.warning(f"Column '{column}' has {null_count} null values ({null_count/total_rows*100:.2f}%)")
    
    # Check for duplicate rows
    distinct_rows = df.distinct().count()
    duplicates = total_rows - distinct_rows
    if duplicates > 0:
        logger.warning(f"Found {duplicates} duplicate rows ({duplicates/total_rows*100:.2f}%)")
    
    return {
        'total_rows': total_rows,
        'distinct_rows': distinct_rows,
        'duplicates': duplicates,
        'null_counts': null_counts
    }

def clean_and_validate_data(df):
    """
    Clean and validate the data using PySpark transformations
    """
    logger.info("Starting data cleaning and validation")
    
    # 1. Remove completely null rows
    df_cleaned = df.dropna(how='all')
    logger.info(f"Removed {df.count() - df_cleaned.count()} completely null rows")
    
    # 2. Clean string columns - trim whitespace and convert to lowercase
    string_columns = [field.name for field in df_cleaned.schema.fields if field.dataType == StringType()]
    for col_name in string_columns:
        if col_name != 'timestamp':  # Don't clean timestamp strings
            df_cleaned = df_cleaned.withColumn(col_name, trim(lower(col(col_name))))
    
    # 3. Handle specific business rules
    # Remove negative values (assuming value should be positive)
    if 'value' in df_cleaned.columns:
        df_cleaned = df_cleaned.filter(col('value') > 0)
        logger.info("Filtered out negative values")
    
    # 4. Remove duplicate records
    df_cleaned = df_cleaned.distinct()
    
    # 5. Fill null values with appropriate defaults
    numeric_columns = [field.name for field in df_cleaned.schema.fields 
                      if field.dataType in [IntegerType(), DoubleType()]]
    
    for col_name in numeric_columns:
        if col_name in ['value']:  # Fill with median or 0
            df_cleaned = df_cleaned.fillna({col_name: 0})
    
    logger.info("Data cleaning and validation completed")
    return df_cleaned

def enrich_data(df):
    """
    Enrich data with additional computed columns
    """
    logger.info("Starting data enrichment")
    
    # Add processing metadata
    df_enriched = df.withColumn("processed_timestamp", current_timestamp()) \
                   .withColumn("processing_version", lit("1.0"))
    
    # Extract date components if timestamp column exists
    if 'timestamp' in df.columns:
        df_enriched = df_enriched \
            .withColumn("year", year(col("timestamp"))) \
            .withColumn("month", month(col("timestamp"))) \
            .withColumn("day", dayofmonth(col("timestamp")))
    
    # Create computed columns
    if 'value' in df.columns:
        # Categorize values
        df_enriched = df_enriched.withColumn(
            "value_category",
            when(col("value") < 100, "low")
            .when(col("value") < 200, "medium")
            .otherwise("high")
        )
        
        # Create hash for anonymization if needed
        if 'name' in df.columns:
            df_enriched = df_enriched.withColumn(
                "name_hash",
                hash(col("name"))
            )
    
    logger.info("Data enrichment completed")
    return df_enriched

def transform_data():
    """
    Transform extracted data using PySpark
    """
    logger.info("Starting PySpark data transformation process")
    
    spark = None
    try:
        # Create Spark session
        spark = get_spark_session("ELT_Data_Transformation")
        logger.info("Spark session created successfully")
        
        # Define paths
        raw_data_path = "/opt/airflow/data/raw"
        processed_data_path = "/opt/airflow/data/processed"
        os.makedirs(processed_data_path, exist_ok=True)
        
        # Process Parquet files first (preferred for PySpark)
        parquet_dirs = [d for d in os.listdir(raw_data_path) 
                       if os.path.isdir(os.path.join(raw_data_path, d)) and 'extracted_data' in d]
        
        # Process CSV files as fallback
        csv_files = [f for f in os.listdir(raw_data_path) if f.endswith('.csv')]
        
        total_processed = 0
        
        # Process Parquet files
        for parquet_dir in parquet_dirs:
            parquet_path = os.path.join(raw_data_path, parquet_dir)
            logger.info(f"Processing Parquet data from: {parquet_path}")
            
            # Read Parquet data
            df = spark.read.parquet(parquet_path)
            
            # Perform data quality checks
            quality_stats = data_quality_check(df, parquet_dir)
            
            # Clean and validate data
            df_cleaned = clean_and_validate_data(df)
            
            # Enrich data
            df_transformed = enrich_data(df_cleaned)
            
            # Save transformed data
            output_path = os.path.join(processed_data_path, f"transformed_{parquet_dir}")
            
            df_transformed.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(output_path)
            
            logger.info(f"Processed {parquet_dir} -> {output_path}")
            total_processed += df_transformed.count()
        
        # Process CSV files
        for csv_file in csv_files:
            csv_path = os.path.join(raw_data_path, csv_file)
            logger.info(f"Processing CSV data from: {csv_path}")
            
            # Read CSV data
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(csv_path)
            
            # Perform data quality checks
            quality_stats = data_quality_check(df, csv_file)
            
            # Clean and validate data
            df_cleaned = clean_and_validate_data(df)
            
            # Enrich data
            df_transformed = enrich_data(df_cleaned)
            
            # Save transformed data (both Parquet and CSV)
            base_name = csv_file.replace('.csv', '')
            
            # Save as Parquet for better performance
            parquet_output = os.path.join(processed_data_path, f"transformed_{base_name}")
            df_transformed.write \
                .mode("overwrite") \
                .option("compression", "snappy") \
                .parquet(parquet_output)
            
            # Also save as CSV for compatibility
            csv_output = os.path.join(processed_data_path, f"transformed_{csv_file}")
            df_transformed.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv(csv_output)
            
            logger.info(f"Processed {csv_file} -> {parquet_output} & {csv_output}")
            total_processed += df_transformed.count()
        
        logger.info(f"PySpark data transformation completed successfully. Total records processed: {total_processed}")
        return True
        
    except Exception as e:
        logger.error(f"Error during PySpark data transformation: {str(e)}")
        raise
    finally:
        stop_spark_session(spark)
        logger.info("Spark session stopped")

def transform_with_sql():
    """
    Alternative transformation using Spark SQL
    """
    spark = None
    try:
        spark = get_spark_session("ELT_SQL_Transform")
        
        # Read data
        df = spark.read.parquet("/opt/airflow/data/raw/*")
        
        # Register as temporary view
        df.createOrReplaceTempView("raw_data")
        
        # Perform transformations using SQL
        transformed_df = spark.sql("""
            SELECT 
                id,
                LOWER(TRIM(name)) as name,
                value,
                timestamp,
                CURRENT_TIMESTAMP() as processed_timestamp,
                YEAR(timestamp) as year,
                MONTH(timestamp) as month,
                CASE 
                    WHEN value < 100 THEN 'low'
                    WHEN value < 200 THEN 'medium'
                    ELSE 'high'
                END as value_category,
                HASH(name) as name_hash
            FROM raw_data
            WHERE value > 0 AND name IS NOT NULL
        """)
        
        return transformed_df
        
    finally:
        stop_spark_session(spark)

if __name__ == "__main__":
    transform_data() 