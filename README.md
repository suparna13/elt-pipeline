# PySpark ELT Pipeline

A comprehensive ELT (Extract, Load, Transform) pipeline using PySpark for distributed data processing, Apache Airflow for orchestration, and PostgreSQL as the data warehouse.

## 🏗️ Architecture Overview

```
Data Sources → PySpark Ingestion → Data Lake → PySpark Transform → PostgreSQL → Analytics
     ↓               ↓                 ↓            ↓              ↓           ↓
   APIs/DBs → Apache Spark → Parquet Files → Apache Spark → PostgreSQL → Dashboard
                ↑                                     ↑
           Airflow Orchestration              Airflow Orchestration
```

## 🚀 Tech Stack

- **Orchestration**: Apache Airflow 2.8.0
- **Processing**: Apache Spark 3.5.0 + PySpark
- **Database**: PostgreSQL 15 (Data Warehouse)
- **Storage**: Parquet files for data lake
- **Containerization**: Docker & Docker Compose
- **Language**: Python 3.11
- **Build Tool**: Java 11 (for Spark)

## 📋 Features

### PySpark Data Pipeline
- ✅ **Distributed Extract**: PySpark-based data extraction from multiple sources
- ✅ **Data Lake Storage**: Parquet format for optimal performance and compression
- ✅ **Distributed Transform**: Scalable data cleaning, validation, and enrichment
- ✅ **JDBC Load**: High-performance database loading with batch optimization

### Advanced Data Processing
- ✅ **Schema Management**: Automatic schema inference and validation
- ✅ **Data Quality Checks**: Built-in data profiling and quality metrics
- ✅ **Memory Optimization**: Adaptive query execution and partition management
- ✅ **SQL Support**: Both DataFrame API and Spark SQL for transformations

### Performance & Scalability
- ✅ **Distributed Computing**: Horizontal scaling with Apache Spark
- ✅ **Optimized Storage**: Columnar Parquet format with Snappy compression
- ✅ **Batch Processing**: Configurable batch sizes for optimal throughput
- ✅ **Resource Management**: Dynamic allocation and memory tuning

### Monitoring & Observability  
- ✅ **Spark UI Integration**: Built-in Spark monitoring and debugging
- ✅ **Pipeline Metrics**: Execution time, data quality, record counts
- ✅ **Comprehensive Logging**: Structured logging with different levels
- ✅ **Health Checks**: Database and Spark connectivity verification

### Operational Excellence
- ✅ **Containerized Deployment**: Docker-based setup with custom Spark environment
- ✅ **Error Handling**: Retries with exponential backoff and circuit breakers
- ✅ **Configuration Management**: Centralized Spark and database configuration
- ✅ **Easy Setup**: Automated setup scripts and Makefile for common operations

## 🏁 Quick Start

### Prerequisites

- Docker & Docker Compose
- At least 6GB RAM and 4 CPU cores (for Spark)
- 15GB+ available disk space
- Linux/macOS (Windows with WSL2)

### 1. Automated Setup (Recommended)

```bash
git clone <your-repo-url>
cd elt-pipeline

# Run automated setup
./setup.sh

# Or use individual commands
make help        # Show all available commands
make build       # Build Docker images
make up          # Start all services
```

### 2. Manual Setup

```bash
# Copy environment template
cp env.example .env

# Edit configuration (optional)
nano .env

# Create directories
mkdir -p logs plugins data/raw data/processed

# Build and start services
docker-compose build
docker-compose up airflow-init
docker-compose up -d
```

### 3. Access the Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow/airflow |
| ELT Database | localhost:5433 | elt_user/elt_password |
| Airflow Database | localhost:5432 | airflow/airflow |

### 4. Run the Pipeline

1. Open Airflow UI at http://localhost:8080
2. Find the DAG: `pyspark_elt_pipeline`
3. Toggle it ON to enable scheduling
4. Click "Trigger DAG" for manual execution

### 5. Monitor Execution

```bash
# View logs
make logs

# Monitor pipeline
make monitor

# Check service status
make status
```

## 📊 Database Schema

### Staging Database
- **Schema**: `staging`
- **Main Table**: `customer_churn_raw` (raw CSV data)
- **Metadata**: `load_metadata` (load tracking)

### Reporting Database  
- **Schema**: `reporting`
- **Fact Table**: `customer_churn_fact`
- **Dimensions**: `customer_dimension`, `service_dimension`, `contract_dimension`
- **Aggregates**: `churn_summary`, `pipeline_metrics`

## 🔧 Configuration

### Pipeline Schedule
Change the pipeline schedule via Airflow Variables:

```bash
# Set custom schedule (e.g., daily at 2 AM)
docker compose exec airflow-webserver airflow variables set elt_pipeline_schedule "0 2 * * *"
```

### Environment Configuration
Modify `config/pipeline_config.yaml` for:
- Data source URLs
- Quality thresholds  
- Transformation rules
- Database connections
- Monitoring settings

## 📈 Monitoring & Observability

### Pipeline Metrics
The pipeline tracks comprehensive metrics in `reporting.pipeline_metrics`:
- Records processed at each stage
- Data quality scores
- Processing times
- Error rates

### Logs
View detailed logs via:
```bash
# Airflow logs
docker compose logs airflow-scheduler
docker compose logs airflow-worker

# Database logs  
docker compose logs postgres-staging
docker compose logs postgres-reporting
```

### Health Checks
Built-in health checks verify:
- Database connectivity
- Data source availability
- Service dependencies

## 🔍 Data Flow Details

### 1. Extract Phase
- Downloads telecom churn dataset from configurable URLs
- Validates file structure and data quality
- Generates file hash for change detection
- Creates backup of source data

### 2. Load to Staging
- Stores raw data with metadata (source file, load timestamp)
- Maintains audit trail in `load_metadata` table
- Preserves original data types and values
- No transformations applied

### 3. Transform Phase
- **Column Standardization**: Lowercase names with underscores
- **PII Anonymization**: Hash customer IDs with SHA-256
- **Missing Value Handling**: Configurable imputation strategies
- **Data Type Cleaning**: Convert to appropriate types
- **Feature Engineering**: Derived columns (tenure categories, service counts)
- **Quality Validation**: Score and flag data quality issues

### 4. Load to Reporting
- **Dimensional Modeling**: Separate fact and dimension tables
- **Customer Dimension**: Demographics with anonymized IDs
- **Service Dimension**: Product and service usage
- **Contract Dimension**: Billing and contract details
- **Fact Table**: Metrics and measurements for analysis

## 📊 Analytics & Reporting

### Superset Dashboards
Access pre-built dashboards at http://localhost:8088:

1. **Churn Overview**: High-level metrics and trends
2. **Customer Segmentation**: Analysis by demographics
3. **Pipeline Monitoring**: ELT performance metrics

### Custom Analysis
Connect to reporting database directly:
```bash
# Connect to reporting database
docker compose exec postgres-reporting psql -U reporting_user -d reporting_db

# Sample queries
SELECT dimension, category, churn_rate 
FROM reporting.v_churn_summary_dashboard 
ORDER BY churn_rate DESC;
```

## 🛠️ Development

### Project Structure
```
elt-pipeline/
├── docker-compose.yml      # Infrastructure orchestration
├── dags/                   # Airflow DAGs
│   └── elt_pipeline_dag.py
├── src/                    # Python modules
│   ├── extract/           # Data extraction
│   ├── transform/         # Data transformation  
│   └── load/              # Data loading
├── sql/                   # Database schemas
├── config/                # Configuration files
├── data/                  # Data storage
└── logs/                  # Application logs
```

### Running Tests
```bash
# Test individual components
python src/extract/csv_extractor.py
python src/transform/data_transformer.py  
python src/load/database_loader.py

# Test full pipeline
docker compose exec airflow-scheduler airflow dags test telecom_churn_elt_pipeline
```

### Adding New Data Sources
1. Update `src/extract/csv_extractor.py`
2. Modify `config/pipeline_config.yaml`
3. Adjust schema in `sql/staging_schema.sql`
4. Update DAG parameters

## 🚨 Troubleshooting

### Common Issues

**Services not starting:**
```bash
# Check logs
docker compose logs
# Restart services
docker compose down && docker compose up -d
```

**Database connection errors:**
```bash
# Verify database status
docker compose ps
# Check database logs
docker compose logs postgres-staging
```

**Pipeline execution failures:**
- Check Airflow UI for task logs
- Verify data source availability
- Review configuration settings
- Check database permissions

**Memory/Resource issues:**
- Ensure minimum 4GB RAM available
- Monitor disk space usage
- Check Docker resource limits

### Performance Tuning
- Adjust `chunk_size` in configuration for large datasets
- Increase database connection pool sizes
- Configure Airflow parallelism settings
- Monitor and optimize SQL queries

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Telecom Customer Churn Dataset from IBM
- Apache Airflow Community
- PostgreSQL Team
- Apache Superset Project

---

**Built with ❤️ for data engineering excellence**