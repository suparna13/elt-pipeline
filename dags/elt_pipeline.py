"""
PySpark ELT Pipeline DAG for Airflow
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
import os

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'pyspark_elt_pipeline',
    default_args=default_args,
    description='PySpark ELT Pipeline for data processing',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['elt', 'pyspark', 'data-pipeline'],
    max_active_runs=1,
)

# Environment setup task
setup_env_task = BashOperator(
    task_id='setup_environment',
    bash_command='''
    echo "Setting up environment for PySpark..."
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
    export SPARK_HOME=/opt/spark
    echo "JAVA_HOME: $JAVA_HOME"
    echo "SPARK_HOME: $SPARK_HOME"
    java -version
    echo "Environment setup complete"
    ''',
    dag=dag,
)

# Data extraction task using PySpark
extract_task = BashOperator(
    task_id='extract_data_pyspark',
    bash_command='cd /opt/airflow && python scripts/ingest.py',
    env={
        'JAVA_HOME': '/usr/lib/jvm/java-11-openjdk-amd64',
        'SPARK_HOME': '/opt/spark',
        'PYTHONPATH': '/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip',
        'POSTGRES_HOST': 'elt-postgres',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'elt_db',
        'POSTGRES_USER': 'elt_user',
        'POSTGRES_PASSWORD': 'elt_password'
    },
    dag=dag,
)

# Data validation task
validate_data_task = PythonOperator(
    task_id='validate_extracted_data',
    python_callable=lambda: validate_data_extraction(),
    dag=dag,
)

def validate_data_extraction():
    """Validate that data extraction was successful"""
    import os
    raw_data_path = "/opt/airflow/data/raw"
    
    if not os.path.exists(raw_data_path):
        raise ValueError(f"Raw data directory {raw_data_path} does not exist")
    
    files = os.listdir(raw_data_path)
    if not files:
        raise ValueError("No files found in raw data directory")
    
    print(f"Found {len(files)} files/directories in raw data: {files}")
    return True

# Data transformation task using PySpark
transform_task = BashOperator(
    task_id='transform_data_pyspark',
    bash_command='cd /opt/airflow && python scripts/transform.py',
    env={
        'JAVA_HOME': '/usr/lib/jvm/java-11-openjdk-amd64',
        'SPARK_HOME': '/opt/spark',
        'PYTHONPATH': '/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip',
    },
    dag=dag,
)

# Data quality check task
quality_check_task = PythonOperator(
    task_id='data_quality_check',
    python_callable=lambda: check_data_quality(),
    dag=dag,
)

def check_data_quality():
    """Check data quality after transformation"""
    import os
    processed_data_path = "/opt/airflow/data/processed"
    
    if not os.path.exists(processed_data_path):
        raise ValueError(f"Processed data directory {processed_data_path} does not exist")
    
    files = os.listdir(processed_data_path)
    if not files:
        raise ValueError("No processed files found")
    
    print(f"Found {len(files)} processed files/directories: {files}")
    return True

# Database connection test
db_connection_test = BashOperator(
    task_id='test_database_connection',
    bash_command='''
    echo "Testing database connection..."
    python -c "
import psycopg2
import os
try:
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'elt-postgres'),
        port=os.getenv('POSTGRES_PORT', '5432'),
        database=os.getenv('POSTGRES_DB', 'elt_db'),
        user=os.getenv('POSTGRES_USER', 'elt_user'),
        password=os.getenv('POSTGRES_PASSWORD', 'elt_password')
    )
    print('Database connection successful')
    conn.close()
except Exception as e:
    print(f'Database connection failed: {e}')
    raise
"
    ''',
    env={
        'POSTGRES_HOST': 'elt-postgres',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'elt_db',
        'POSTGRES_USER': 'elt_user',
        'POSTGRES_PASSWORD': 'elt_password'
    },
    dag=dag,
)

# Data loading task using PySpark
load_task = BashOperator(
    task_id='load_data_pyspark',
    bash_command='cd /opt/airflow && python scripts/load.py',
    env={
        'JAVA_HOME': '/usr/lib/jvm/java-11-openjdk-amd64',
        'SPARK_HOME': '/opt/spark',
        'PYTHONPATH': '/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip',
        'POSTGRES_HOST': 'elt-postgres',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DB': 'elt_db',
        'POSTGRES_USER': 'elt_user',
        'POSTGRES_PASSWORD': 'elt_password'
    },
    dag=dag,
)

# Data verification task
verify_load_task = PythonOperator(
    task_id='verify_data_load',
    python_callable=lambda: verify_data_in_database(),
    dag=dag,
)

def verify_data_in_database():
    """Verify that data was loaded into the database"""
    import psycopg2
    import os
    
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'elt-postgres'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database=os.getenv('POSTGRES_DB', 'elt_db'),
            user=os.getenv('POSTGRES_USER', 'elt_user'),
            password=os.getenv('POSTGRES_PASSWORD', 'elt_password')
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM processed_data")
        count = cursor.fetchone()[0]
        
        print(f"Total records in processed_data table: {count}")
        
        if count == 0:
            raise ValueError("No data found in processed_data table")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"Error verifying data load: {e}")
        raise

# Pipeline completion task
pipeline_complete_task = DummyOperator(
    task_id='pipeline_complete',
    dag=dag,
)

# Set task dependencies
setup_env_task >> extract_task >> validate_data_task >> transform_task
transform_task >> quality_check_task >> db_connection_test >> load_task
load_task >> verify_load_task >> pipeline_complete_task 