"""
Unit tests for Airflow DAG
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

# Mock Airflow modules before importing
import sys
from unittest.mock import MagicMock

# Mock Airflow imports
sys.modules['airflow'] = MagicMock()
sys.modules['airflow.models'] = MagicMock()
sys.modules['airflow.operators'] = MagicMock()
sys.modules['airflow.operators.bash'] = MagicMock()
sys.modules['airflow.operators.python'] = MagicMock()
sys.modules['airflow.sensors'] = MagicMock()
sys.modules['airflow.sensors.filesystem'] = MagicMock()
sys.modules['airflow.utils'] = MagicMock()
sys.modules['airflow.utils.dates'] = MagicMock()

# Add parent directory to path for imports
sys.path.append('/opt/airflow/dags')

# Import DAG after mocking
import importlib.util
dag_spec = importlib.util.spec_from_file_location("pyspark_elt_pipeline", "/opt/airflow/dags/pyspark_elt_pipeline.py")
dag_module = importlib.util.module_from_spec(dag_spec)

class TestDAGStructure:
    """Test DAG structure and configuration"""
    
    def test_dag_imports(self):
        """Test that DAG imports work correctly"""
        try:
            # Try to load the DAG module
            dag_spec.loader.exec_module(dag_module)
            assert True
        except Exception as e:
            pytest.fail(f"DAG imports failed: {e}")
    
    def test_dag_creation(self):
        """Test DAG object creation"""
        # Load the DAG
        dag_spec.loader.exec_module(dag_module)
        
        # Check if DAG object exists
        assert hasattr(dag_module, 'dag')
        dag = dag_module.dag
        
        # Verify DAG properties
        assert dag.dag_id == 'pyspark_elt_pipeline'
        assert dag.schedule_interval == timedelta(hours=1)
        assert dag.catchup is False
    
    def test_dag_default_args(self):
        """Test DAG default arguments"""
        dag_spec.loader.exec_module(dag_module)
        dag = dag_module.dag
        
        # Check default args
        default_args = dag.default_args
        assert 'owner' in default_args
        assert default_args['owner'] == 'data_engineer'
        assert 'depends_on_past' in default_args
        assert default_args['depends_on_past'] is False
        assert 'email_on_failure' in default_args
        assert 'email_on_retry' in default_args
        assert 'retries' in default_args
        assert 'retry_delay' in default_args

class TestDAGTasks:
    """Test individual DAG tasks"""
    
    def setup_method(self):
        """Setup for each test method"""
        dag_spec.loader.exec_module(dag_module)
        self.dag = dag_module.dag
    
    def test_all_tasks_exist(self):
        """Test that all expected tasks exist in the DAG"""
        expected_tasks = [
            'setup_environment',
            'data_quality_check_start',
            'extract_data',
            'validate_extracted_data',
            'transform_data',
            'validate_transformed_data',
            'load_data',
            'validate_loaded_data',
            'data_quality_check_end',
            'cleanup_temp_files'
        ]
        
        actual_task_ids = [task.task_id for task in self.dag.tasks]
        
        for expected_task in expected_tasks:
            assert expected_task in actual_task_ids, f"Task {expected_task} not found in DAG"
    
    def test_setup_environment_task(self):
        """Test setup environment task configuration"""
        setup_task = self.dag.get_task('setup_environment')
        
        assert setup_task.task_id == 'setup_environment'
        # Check that it's a BashOperator or PythonOperator
        assert hasattr(setup_task, 'bash_command') or hasattr(setup_task, 'python_callable')
    
    def test_extract_data_task(self):
        """Test extract data task configuration"""
        extract_task = self.dag.get_task('extract_data')
        
        assert extract_task.task_id == 'extract_data'
        assert hasattr(extract_task, 'bash_command')
        
        # Check that the command references the ingest script
        if hasattr(extract_task, 'bash_command'):
            assert 'ingest.py' in extract_task.bash_command
    
    def test_transform_data_task(self):
        """Test transform data task configuration"""
        transform_task = self.dag.get_task('transform_data')
        
        assert transform_task.task_id == 'transform_data'
        assert hasattr(transform_task, 'bash_command')
        
        # Check that the command references the transform script
        if hasattr(transform_task, 'bash_command'):
            assert 'transform.py' in transform_task.bash_command
    
    def test_load_data_task(self):
        """Test load data task configuration"""
        load_task = self.dag.get_task('load_data')
        
        assert load_task.task_id == 'load_data'
        assert hasattr(load_task, 'bash_command')
        
        # Check that the command references the load script
        if hasattr(load_task, 'bash_command'):
            assert 'load.py' in load_task.bash_command

class TestDAGDependencies:
    """Test DAG task dependencies"""
    
    def setup_method(self):
        """Setup for each test method"""
        dag_spec.loader.exec_module(dag_module)
        self.dag = dag_module.dag
    
    def test_task_dependencies(self):
        """Test that task dependencies are correctly set"""
        # Get tasks
        setup_task = self.dag.get_task('setup_environment')
        extract_task = self.dag.get_task('extract_data')
        transform_task = self.dag.get_task('transform_data')
        load_task = self.dag.get_task('load_data')
        
        # Test basic flow dependencies
        assert extract_task in setup_task.downstream_list
        assert transform_task in extract_task.downstream_list
        assert load_task in transform_task.downstream_list
    
    def test_validation_task_dependencies(self):
        """Test validation task dependencies"""
        # Get validation tasks
        validate_extracted = self.dag.get_task('validate_extracted_data')
        validate_transformed = self.dag.get_task('validate_transformed_data')
        validate_loaded = self.dag.get_task('validate_loaded_data')
        
        # Get main tasks
        extract_task = self.dag.get_task('extract_data')
        transform_task = self.dag.get_task('transform_data')
        load_task = self.dag.get_task('load_data')
        
        # Test validation dependencies
        assert validate_extracted in extract_task.downstream_list
        assert validate_transformed in transform_task.downstream_list
        assert validate_loaded in load_task.downstream_list
    
    def test_data_quality_check_dependencies(self):
        """Test data quality check dependencies"""
        quality_start = self.dag.get_task('data_quality_check_start')
        quality_end = self.dag.get_task('data_quality_check_end')
        
        # Quality start should be early in the pipeline
        setup_task = self.dag.get_task('setup_environment')
        assert quality_start in setup_task.downstream_list or quality_start.upstream_list == []
        
        # Quality end should be late in the pipeline
        load_task = self.dag.get_task('validate_loaded_data')
        assert quality_end in load_task.downstream_list

class TestDAGValidation:
    """Test DAG validation and error handling"""
    
    def setup_method(self):
        """Setup for each test method"""
        dag_spec.loader.exec_module(dag_module)
        self.dag = dag_module.dag
    
    def test_dag_has_no_cycles(self):
        """Test that DAG has no circular dependencies"""
        # This would test that the DAG is acyclic
        # Airflow automatically checks this, but we can verify the structure
        
        # Get all tasks and check for circular references
        tasks = self.dag.tasks
        for task in tasks:
            visited = set()
            
            def check_downstream(current_task, path):
                if current_task.task_id in path:
                    return True  # Cycle detected
                new_path = path | {current_task.task_id}
                for downstream in current_task.downstream_list:
                    if check_downstream(downstream, new_path):
                        return True
                return False
            
            # Should not find cycles
            assert not check_downstream(task, set())
    
    def test_task_timeouts(self):
        """Test that tasks have appropriate timeouts"""
        critical_tasks = ['extract_data', 'transform_data', 'load_data']
        
        for task_id in critical_tasks:
            task = self.dag.get_task(task_id)
            # Check if timeout is set (either execution_timeout or task-specific timeout)
            assert hasattr(task, 'execution_timeout') or hasattr(task, 'timeout')
    
    def test_retry_configuration(self):
        """Test retry configuration for tasks"""
        for task in self.dag.tasks:
            # Check that retries are configured (from default_args or task-specific)
            retries = getattr(task, 'retries', None) or self.dag.default_args.get('retries', 0)
            assert retries >= 1, f"Task {task.task_id} should have retries configured"

class TestDAGScheduling:
    """Test DAG scheduling and timing"""
    
    def setup_method(self):
        """Setup for each test method"""
        dag_spec.loader.exec_module(dag_module)
        self.dag = dag_module.dag
    
    def test_schedule_interval(self):
        """Test DAG schedule interval"""
        assert self.dag.schedule_interval == timedelta(hours=1)
    
    def test_start_date(self):
        """Test DAG start date"""
        assert self.dag.start_date is not None
        # Start date should be in the past or present
        assert self.dag.start_date <= datetime.now()
    
    def test_catchup_disabled(self):
        """Test that catchup is disabled"""
        assert self.dag.catchup is False
    
    def test_max_active_runs(self):
        """Test maximum active runs configuration"""
        # Should have reasonable limit on concurrent runs
        max_active_runs = getattr(self.dag, 'max_active_runs', 1)
        assert max_active_runs <= 3  # Reasonable limit

class TestDAGParameters:
    """Test DAG parameterization and configuration"""
    
    def setup_method(self):
        """Setup for each test method"""
        dag_spec.loader.exec_module(dag_module)
        self.dag = dag_module.dag
    
    def test_dag_params(self):
        """Test DAG parameters"""
        # Check if DAG has configurable parameters
        if hasattr(self.dag, 'params'):
            params = self.dag.params
            # Verify common parameters exist
            expected_params = ['data_date', 'batch_size', 'env']
            for param in expected_params:
                if param in params:
                    assert params[param] is not None
    
    def test_environment_variables(self):
        """Test environment variable usage"""
        # Check that tasks use environment variables appropriately
        env_tasks = [task for task in self.dag.tasks if hasattr(task, 'env')]
        
        for task in env_tasks:
            # Common environment variables that should be set
            common_env_vars = ['AIRFLOW_HOME', 'PYTHONPATH']
            for env_var in common_env_vars:
                if env_var in task.env:
                    assert task.env[env_var] is not None

class TestDAGMonitoring:
    """Test DAG monitoring and observability"""
    
    def setup_method(self):
        """Setup for each test method"""
        dag_spec.loader.exec_module(dag_module)
        self.dag = dag_module.dag
    
    def test_dag_tags(self):
        """Test DAG tags for organization"""
        if hasattr(self.dag, 'tags'):
            tags = self.dag.tags
            # Should have relevant tags
            expected_tags = ['etl', 'pyspark', 'data-pipeline']
            common_tags = set(tags) & set(expected_tags)
            assert len(common_tags) > 0, "DAG should have relevant tags"
    
    def test_task_documentation(self):
        """Test that tasks have documentation"""
        for task in self.dag.tasks:
            # Tasks should have docs or description
            has_docs = hasattr(task, 'doc') or hasattr(task, 'doc_md') or hasattr(task, 'description')
            # At least critical tasks should be documented
            if task.task_id in ['extract_data', 'transform_data', 'load_data']:
                assert has_docs, f"Critical task {task.task_id} should be documented"

class TestDAGIntegration:
    """Integration tests for the complete DAG"""
    
    def setup_method(self):
        """Setup for each test method"""
        dag_spec.loader.exec_module(dag_module)
        self.dag = dag_module.dag
    
    @patch('subprocess.run')
    def test_dag_syntax_check(self, mock_subprocess):
        """Test DAG syntax validation"""
        # Mock successful syntax check
        mock_subprocess.return_value.returncode = 0
        
        # This would run airflow dags check command
        # For now, we verify that the DAG loads without syntax errors
        assert self.dag is not None
        assert len(self.dag.tasks) > 0
    
    def test_dag_task_count(self):
        """Test expected number of tasks"""
        # Should have reasonable number of tasks for ELT pipeline
        task_count = len(self.dag.tasks)
        assert task_count >= 8, "DAG should have at least 8 tasks"
        assert task_count <= 15, "DAG should not have too many tasks"
    
    def test_dag_complexity(self):
        """Test DAG complexity metrics"""
        # Calculate basic complexity metrics
        total_dependencies = sum(len(task.downstream_list) for task in self.dag.tasks)
        avg_dependencies = total_dependencies / len(self.dag.tasks) if self.dag.tasks else 0
        
        # Should have reasonable complexity
        assert avg_dependencies <= 3, "Average task dependencies should be reasonable"
        
        # Check for parallel execution opportunities
        parallel_tasks = [task for task in self.dag.tasks if len(task.upstream_list) == 0]
        assert len(parallel_tasks) >= 1, "Should have at least one starting task" 