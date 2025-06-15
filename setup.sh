#!/bin/bash

# PySpark ELT Pipeline Setup Script
echo "🚀 Setting up PySpark ELT Pipeline..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}$1${NC}"
}

# Check if Docker is installed
check_docker() {
    print_header "Checking Docker installation..."
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    print_status "Docker and Docker Compose are installed."
}

# Create required directories
create_directories() {
    print_header "Creating required directories..."
    mkdir -p logs plugins data/raw data/processed
    print_status "Directories created."
}

# Set up environment file
setup_env() {
    print_header "Setting up environment configuration..."
    if [ ! -f .env ]; then
        cp env.example .env
        print_status "Environment file created from template."
        print_warning "Please edit .env file with your specific configurations."
    else
        print_warning "Environment file already exists."
    fi
}

# Set proper permissions
set_permissions() {
    print_header "Setting up permissions..."
    # Get current user ID
    export AIRFLOW_UID=$(id -u)
    echo "AIRFLOW_UID=$AIRFLOW_UID" >> .env
    
    # Set permissions for Airflow directories
    sudo chown -R $USER:$USER logs/ plugins/ data/
    chmod -R 755 logs/ plugins/ data/
    
    print_status "Permissions set correctly."
}

# Build Docker images
build_images() {
    print_header "Building Docker images..."
    docker-compose build --no-cache
    if [ $? -eq 0 ]; then
        print_status "Docker images built successfully."
    else
        print_error "Failed to build Docker images."
        exit 1
    fi
}

# Initialize Airflow
init_airflow() {
    print_header "Initializing Airflow..."
    docker-compose up airflow-init
    if [ $? -eq 0 ]; then
        print_status "Airflow initialized successfully."
    else
        print_error "Failed to initialize Airflow."
        exit 1
    fi
}

# Start services
start_services() {
    print_header "Starting all services..."
    docker-compose up -d
    if [ $? -eq 0 ]; then
        print_status "All services started successfully."
    else
        print_error "Failed to start services."
        exit 1
    fi
}

# Wait for services to be ready
wait_for_services() {
    print_header "Waiting for services to be ready..."
    
    # Wait for Airflow webserver
    echo "Waiting for Airflow webserver..."
    timeout=300
    while [ $timeout -gt 0 ]; do
        if curl -f http://localhost:8080/health &> /dev/null; then
            break
        fi
        sleep 5
        timeout=$((timeout - 5))
    done
    
    if [ $timeout -le 0 ]; then
        print_warning "Airflow webserver might not be ready yet."
    else
        print_status "Airflow webserver is ready."
    fi
    
    # Wait for database
    echo "Waiting for database..."
    timeout=60
    while [ $timeout -gt 0 ]; do
        if docker-compose exec -T elt-postgres pg_isready -U elt_user -d elt_db &> /dev/null; then
            break
        fi
        sleep 2
        timeout=$((timeout - 2))
    done
    
    if [ $timeout -le 0 ]; then
        print_warning "Database might not be ready yet."
    else
        print_status "Database is ready."
    fi
}

# Display access information
show_access_info() {
    print_header "🎉 Setup Complete!"
    echo ""
    echo "Access Information:"
    echo "  📊 Airflow UI: http://localhost:8080"
    echo "     Username: airflow"
    echo "     Password: airflow"
    echo ""
    echo "  🗄️  ELT Database: localhost:5433"
    echo "     Username: elt_user"
    echo "     Password: elt_password"
    echo "     Database: elt_db"
    echo ""
    echo "  🗄️  Airflow Database: localhost:5432"
    echo "     Username: airflow"
    echo "     Password: airflow"
    echo "     Database: airflow"
    echo ""
    echo "Useful Commands:"
    echo "  make help          - Show all available commands"
    echo "  make logs          - View logs from all services"
    echo "  make status        - Check service status"
    echo "  make monitor       - Monitor pipeline execution"
    echo "  make down          - Stop all services"
    echo "  make clean         - Clean up everything"
    echo ""
    echo "Next Steps:"
    echo "  1. Open http://localhost:8080 in your browser"
    echo "  2. Enable the 'pyspark_elt_pipeline' DAG"
    echo "  3. Trigger the DAG to run your ELT pipeline"
}

# Run tests
run_tests() {
    print_header "Running basic connectivity tests..."
    
    # Test database connection
    if docker-compose exec -T elt-postgres psql -U elt_user -d elt_db -c "SELECT 1;" &> /dev/null; then
        print_status "Database connection test passed."
    else
        print_warning "Database connection test failed."
    fi
    
    # Test Airflow API
    if curl -f http://localhost:8080/api/v1/health &> /dev/null; then
        print_status "Airflow API test passed."
    else
        print_warning "Airflow API test failed."
    fi
}

# Main execution
main() {
    print_header "🔧 PySpark ELT Pipeline Setup"
    echo "This script will set up your complete ELT pipeline environment."
    echo ""
    
    # Ask for confirmation
    read -p "Do you want to proceed with the setup? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Setup cancelled."
        exit 0
    fi
    
    # Run setup steps
    check_docker
    create_directories
    setup_env
    set_permissions
    build_images
    init_airflow
    start_services
    wait_for_services
    run_tests
    show_access_info
}

# Handle script arguments
case "${1:-setup}" in
    "setup")
        main
        ;;
    "clean")
        print_header "Cleaning up environment..."
        docker-compose down -v --remove-orphans
        docker system prune -f
        sudo rm -rf logs/ data/raw/ data/processed/
        print_status "Environment cleaned."
        ;;
    "status")
        print_header "Service Status:"
        docker-compose ps
        ;;
    "logs")
        docker-compose logs -f
        ;;
    *)
        echo "Usage: $0 [setup|clean|status|logs]"
        echo "  setup  - Full environment setup (default)"
        echo "  clean  - Clean up all containers and data"
        echo "  status - Show service status"
        echo "  logs   - Show service logs"
        ;;
esac 