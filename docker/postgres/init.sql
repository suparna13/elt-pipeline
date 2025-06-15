-- Initialize the ELT database for PySpark pipeline
-- This script runs when the PostgreSQL container starts for the first time

-- Create schema for raw data staging
CREATE SCHEMA IF NOT EXISTS raw_data;

-- Create schema for processed data
CREATE SCHEMA IF NOT EXISTS processed_data;

-- Create main processed data table (will be created by PySpark scripts)
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
);

-- Create daily summary table
CREATE TABLE IF NOT EXISTS daily_summary (
    year INTEGER,
    month INTEGER,
    day INTEGER,
    value_category VARCHAR(50),
    record_count BIGINT,
    avg_value NUMERIC,
    min_value NUMERIC,
    max_value NUMERIC,
    summary_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create partitioned table for better performance (optional)
CREATE TABLE IF NOT EXISTS processed_data_partitioned (
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
) PARTITION BY RANGE (year);

-- Create partitions for current and next year (adjust as needed)
CREATE TABLE IF NOT EXISTS processed_data_2024 PARTITION OF processed_data_partitioned
FOR VALUES FROM (2024) TO (2025);

CREATE TABLE IF NOT EXISTS processed_data_2025 PARTITION OF processed_data_partitioned
FOR VALUES FROM (2025) TO (2026);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_processed_data_timestamp ON processed_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_processed_data_category ON processed_data(value_category);
CREATE INDEX IF NOT EXISTS idx_processed_data_year_month ON processed_data(year, month);

-- Create sample raw data table for testing
CREATE TABLE IF NOT EXISTS raw_data.sample_source (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255),
    transaction_value NUMERIC,
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(100) DEFAULT 'sample_api'
);

-- Insert sample data for testing
INSERT INTO raw_data.sample_source (customer_name, transaction_value, transaction_date) VALUES
('customer_001', 150.50, '2024-01-15 10:30:00'),
('customer_002', 75.25, '2024-01-15 11:45:00'),
('customer_003', 300.00, '2024-01-15 14:20:00'),
('customer_004', 45.75, '2024-01-15 16:10:00'),
('customer_005', 220.80, '2024-01-15 18:35:00')
ON CONFLICT DO NOTHING;

-- Grant permissions for ELT operations
-- Note: Adjust these permissions based on your security requirements
-- GRANT ALL PRIVILEGES ON SCHEMA raw_data TO elt_user;
-- GRANT ALL PRIVILEGES ON SCHEMA processed_data TO elt_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data TO elt_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA processed_data TO elt_user; 