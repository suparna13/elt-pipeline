#!/bin/bash

# Test runner script for PySpark ELT Pipeline
# This script provides various ways to run the test suite

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if required tools are installed
check_dependencies() {
    print_status "Checking dependencies..."
    
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is required but not installed."
        exit 1
    fi
    
    if ! python3 -c "import pytest" &> /dev/null; then
        print_warning "pytest not found. Installing test dependencies..."
        pip install -r requirements.txt
    fi
    
    print_success "Dependencies check passed"
}

# Function to run unit tests
run_unit_tests() {
    print_status "Running unit tests..."
    python3 -m pytest scripts/tests/ -m "not integration" -v
}

# Function to run integration tests
run_integration_tests() {
    print_status "Running integration tests..."
    python3 -m pytest scripts/tests/ -m "integration" -v
}

# Function to run all tests
run_all_tests() {
    print_status "Running all tests..."
    python3 -m pytest scripts/tests/ -v
}

# Function to run tests with coverage
run_tests_with_coverage() {
    print_status "Running tests with coverage..."
    python3 -m pytest scripts/tests/ --cov=scripts --cov-report=html --cov-report=term-missing -v
}

# Function to run specific test file
run_specific_test() {
    local test_file=$1
    if [ -z "$test_file" ]; then
        print_error "Please specify a test file"
        echo "Usage: $0 specific <test_file>"
        echo "Example: $0 specific scripts/tests/test_spark_config.py"
        exit 1
    fi
    
    print_status "Running specific test file: $test_file"
    python3 -m pytest "$test_file" -v
}

# Function to run tests for a specific module
run_module_tests() {
    local module=$1
    if [ -z "$module" ]; then
        print_error "Please specify a module"
        echo "Usage: $0 module <module_name>"
        echo "Example: $0 module spark_config"
        exit 1
    fi
    
    print_status "Running tests for module: $module"
    python3 -m pytest "scripts/tests/test_${module}.py" -v
}

# Function to run tests in parallel
run_parallel_tests() {
    print_status "Running tests in parallel..."
    python3 -m pytest scripts/tests/ -n auto -v
}

# Function to run smoke tests (quick basic tests)
run_smoke_tests() {
    print_status "Running smoke tests..."
    python3 -m pytest scripts/tests/ -k "test_.*_success" -v --tb=short
}

# Function to generate test report
generate_test_report() {
    print_status "Generating test report..."
    python3 -m pytest scripts/tests/ --html=test_report.html --self-contained-html --cov=scripts --cov-report=html -v
    print_success "Test report generated: test_report.html"
}

# Function to run linting on test files
lint_tests() {
    print_status "Linting test files..."
    python3 -m flake8 scripts/tests/ --max-line-length=100
    python3 -m black --check scripts/tests/
    python3 -m isort --check-only scripts/tests/
}

# Function to format test files
format_tests() {
    print_status "Formatting test files..."
    python3 -m black scripts/tests/
    python3 -m isort scripts/tests/
    print_success "Test files formatted"
}

# Function to clean test artifacts
clean_test_artifacts() {
    print_status "Cleaning test artifacts..."
    rm -rf htmlcov/
    rm -rf .pytest_cache/
    rm -rf test_report.html
    rm -rf .coverage
    find . -name "*.pyc" -delete
    find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    print_success "Test artifacts cleaned"
}

# Function to show test statistics
show_test_stats() {
    print_status "Collecting test statistics..."
    
    total_tests=$(find scripts/tests/ -name "test_*.py" -exec grep -l "def test_" {} \; | wc -l)
    test_functions=$(find scripts/tests/ -name "test_*.py" -exec grep -h "def test_" {} \; | wc -l)
    
    echo "Test Statistics:"
    echo "  Test files: $total_tests"
    echo "  Test functions: $test_functions"
    echo "  Test directories: $(find scripts/tests/ -type d | wc -l)"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  unit                 Run unit tests only"
    echo "  integration          Run integration tests only"
    echo "  all                  Run all tests"
    echo "  coverage             Run tests with coverage report"
    echo "  specific <file>      Run specific test file"
    echo "  module <name>        Run tests for specific module"
    echo "  parallel             Run tests in parallel"
    echo "  smoke                Run quick smoke tests"
    echo "  report               Generate HTML test report"
    echo "  lint                 Lint test files"
    echo "  format               Format test files"
    echo "  clean                Clean test artifacts"
    echo "  stats                Show test statistics"
    echo "  help                 Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 all"
    echo "  $0 specific scripts/tests/test_spark_config.py"
    echo "  $0 module transform"
    echo "  $0 coverage"
}

# Main script logic
main() {
    local command=${1:-all}
    
    case $command in
        unit)
            check_dependencies
            run_unit_tests
            ;;
        integration)
            check_dependencies
            run_integration_tests
            ;;
        all)
            check_dependencies
            run_all_tests
            ;;
        coverage)
            check_dependencies
            run_tests_with_coverage
            ;;
        specific)
            check_dependencies
            run_specific_test "$2"
            ;;
        module)
            check_dependencies
            run_module_tests "$2"
            ;;
        parallel)
            check_dependencies
            run_parallel_tests
            ;;
        smoke)
            check_dependencies
            run_smoke_tests
            ;;
        report)
            check_dependencies
            generate_test_report
            ;;
        lint)
            lint_tests
            ;;
        format)
            format_tests
            ;;
        clean)
            clean_test_artifacts
            ;;
        stats)
            show_test_stats
            ;;
        help|--help|-h)
            show_usage
            ;;
        *)
            print_error "Unknown command: $command"
            show_usage
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@" 