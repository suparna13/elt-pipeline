# PySpark ELT Pipeline Makefile

.PHONY: help build up down clean logs test test-unit test-integration test-coverage test-specific test-module test-report test-clean test-lint test-format test-stats test-docker init-env install-deps

# Default target
help:
	@echo "Available commands:"
	@echo "  build            - Build the Docker containers"
	@echo "  up               - Start all services"
	@echo "  down             - Stop all services"
	@echo "  clean            - Remove all containers and volumes"
	@echo "  logs             - View logs from all services"
	@echo "  test             - Run all tests locally"
	@echo "  test-unit        - Run unit tests only"
	@echo "  test-integration - Run integration tests only"
	@echo "  test-coverage    - Run tests with coverage report"
	@echo "  test-report      - Generate HTML test report"
	@echo "  test-docker      - Run tests in Docker container"
	@echo "  test-clean       - Clean test artifacts"
	@echo "  init-env         - Initialize environment file"
	@echo "  install-deps     - Install Python dependencies locally"
	@echo "  spark-submit     - Submit a Spark application"
	@echo "  db-connect       - Connect to the ELT database"

# Initialize environment file
init-env:
	@if [ ! -f .env ]; then \
		cp env.example .env; \
		echo "Environment file created from template"; \
		echo "Please edit .env file with your configurations"; \
	else \
		echo "Environment file already exists"; \
	fi

# Build Docker containers
build:
	docker-compose build --no-cache

# Start all services
up: init-env
	docker-compose up -d
	@echo "Services started. Access:"
	@echo "  - Airflow UI: http://localhost:8080"
	@echo "  - Username: airflow, Password: airflow"

# Stop all services
down:
	docker-compose down

# Clean up everything
clean:
	docker-compose down -v --remove-orphans
	docker system prune -f
	sudo rm -rf logs/ data/raw/ data/processed/

# View logs
logs:
	docker-compose logs -f

# View specific service logs
logs-airflow:
	docker-compose logs -f airflow-webserver airflow-scheduler

logs-db:
	docker-compose logs -f elt-postgres

# Run tests
test:
	@echo "Running all tests..."
	./run_tests.sh all

test-unit:
	@echo "Running unit tests..."
	./run_tests.sh unit

test-integration:
	@echo "Running integration tests..."
	./run_tests.sh integration

test-coverage:
	@echo "Running tests with coverage..."
	./run_tests.sh coverage

test-specific:
	@echo "Running specific test file..."
	@read -p "Enter test file path: " test_file; \
	./run_tests.sh specific $$test_file

test-module:
	@echo "Running tests for specific module..."
	@read -p "Enter module name: " module; \
	./run_tests.sh module $$module

test-report:
	@echo "Generating test report..."
	./run_tests.sh report

test-clean:
	@echo "Cleaning test artifacts..."
	./run_tests.sh clean

test-lint:
	@echo "Linting test files..."
	./run_tests.sh lint

test-format:
	@echo "Formatting test files..."
	./run_tests.sh format

test-stats:
	@echo "Showing test statistics..."
	./run_tests.sh stats

test-docker:
	@echo "Running tests in Docker container..."
	docker-compose exec airflow-webserver python -m pytest scripts/tests/ -v

# Install Python dependencies locally
install-deps:
	pip install -r requirements.txt

# Submit a Spark application
spark-submit:
	@echo "Usage: make spark-submit APP=scripts/your_script.py"
	@if [ -z "$(APP)" ]; then \
		echo "Please specify APP=path/to/script.py"; \
	else \
		docker-compose exec airflow-webserver spark-submit $(APP); \
	fi

# Connect to ELT database
db-connect:
	docker-compose exec elt-postgres psql -U elt_user -d elt_db

# Connect to Airflow metadata database
db-connect-airflow:
	docker-compose exec postgres psql -U airflow -d airflow

# Check service status
status:
	docker-compose ps

# Restart services
restart: down up

# Initialize Airflow
airflow-init:
	docker-compose up airflow-init

# Create directories
create-dirs:
	mkdir -p logs plugins data/raw data/processed

# Backup database
backup-db:
	docker-compose exec elt-postgres pg_dump -U elt_user elt_db > backup_$(shell date +%Y%m%d_%H%M%S).sql

# Restore database
restore-db:
	@echo "Usage: make restore-db BACKUP=backup_file.sql"
	@if [ -z "$(BACKUP)" ]; then \
		echo "Please specify BACKUP=backup_file.sql"; \
	else \
		docker-compose exec -T elt-postgres psql -U elt_user -d elt_db < $(BACKUP); \
	fi

# Run PySpark scripts individually
run-extract:
	docker-compose exec airflow-webserver python scripts/ingest.py

run-transform:
	docker-compose exec airflow-webserver python scripts/transform.py

run-load:
	docker-compose exec airflow-webserver python scripts/load.py

# Monitor pipeline
monitor:
	@echo "Monitoring pipeline..."
	@echo "Check Airflow UI: http://localhost:8080"
	@echo "Check database records:"
	@docker-compose exec elt-postgres psql -U elt_user -d elt_db -c "SELECT COUNT(*) FROM processed_data;"

# Performance check
perf-check:
	@echo "Checking performance..."
	@docker-compose exec airflow-webserver python -c "
import psutil
print(f'CPU Usage: {psutil.cpu_percent()}%')
print(f'Memory Usage: {psutil.virtual_memory().percent}%')
print(f'Disk Usage: {psutil.disk_usage(\"/\").percent}%')
" 