# Tests for CFA Dagster Custom Run Launcher and Executor

This directory contains comprehensive tests for the custom Dagster run launcher and executor implementations in the `cfa_dagster` package.

## Test Structure

### 1. Run Launcher Tests (`test_run_launcher.py`)
Tests for the `DynamicRunLauncher` class that handles run launching based on configuration:

- **Configuration Resolution**: Tests that the launcher properly resolves configuration from multiple sources in the correct precedence order:
  - Run config (highest priority)
  - Run tags
  - Repository metadata
  - Legacy launcher tags
  - Legacy repository metadata (lowest priority)

- **Launcher Creation**: Tests for creating different types of launchers (DefaultRunLauncher, AzureContainerAppJobRunLauncher, etc.) based on configuration

- **Environment Handling**: Tests for dev vs production environment detection and appropriate launcher selection

- **Legacy Support**: Tests for backward compatibility with legacy launcher tags

### 2. Executor Tests (`test_executor.py`)
Tests for the `DynamicExecutor` class that handles execution based on configuration:

- **Executor Creation**: Tests for creating different types of executors (in_process_executor, multiprocess_executor, docker_executor, azure_container_app_job_executor, etc.)

- **Runtime Configuration**: Tests for selecting executors based on run tags and metadata

- **Production Restrictions**: Tests that certain executors (like Docker) are restricted in production environments

- **Integration**: Tests for the dynamic executor's ability to switch executors at runtime

### 3. Utility Tests (`test_utils.py`)
Tests for the utility classes that support configuration management:

- **SelectorConfig**: Tests for the configuration selector class that handles class names and configurations

- **ExecutionConfig**: Tests for the execution configuration class that manages launcher and executor configurations

- **Serialization**: Tests for converting configurations to/from run tags and run config

## Key Features Tested

1. **Cascading Configuration**: Ensures configuration is resolved in the correct order of precedence
2. **Environment Detection**: Verifies proper dev vs production environment handling
3. **Runtime Flexibility**: Tests that launchers and executors can be selected at runtime
4. **Safety Checks**: Validates that restricted configurations are prevented in production
5. **Backward Compatibility**: Ensures legacy configurations still work
6. **Error Handling**: Tests proper error responses for invalid configurations

## Testing Approach

The tests use mocking extensively to isolate the functionality being tested while avoiding complex external dependencies. Some tests are marked as skipped when they encounter complex dependency issues that would require extensive mocking to resolve properly.

## Running Tests

```bash
cd /path/to/cfa-dagster
PYTHONPATH=src:. python -m pytest tests/ -v
```

Or using uv:

```bash
cd /path/to/cfa-dagster
uv run pytest tests/ -v
```