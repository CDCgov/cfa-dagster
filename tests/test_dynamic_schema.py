import os
from unittest.mock import patch

import pytest

from cfa_dagster.execution.utils import get_dynamic_executor_config_schema


def test_dynamic_schema_in_dev_mode():
    """Test that the dynamic schema includes dev-only options when in dev mode"""
    # Simulate dev mode by setting DAGSTER_IS_DEV_CLI
    with patch.dict(os.environ, {"DAGSTER_IS_DEV_CLI": "1"}):
        schema = get_dynamic_executor_config_schema()
        
        # In dev mode, DockerRunLauncher should be available in launcher options
        launcher_selector_fields = schema["launcher"].config_type.fields
        assert "DockerRunLauncher" in launcher_selector_fields
        
        # In dev mode, docker_executor should be available in executor options
        executor_selector_fields = schema["executor"].config_type.fields
        assert "docker_executor" in executor_selector_fields
        
        # Default should be DefaultRunLauncher in dev mode
        assert schema["launcher"].default_value == {"DefaultRunLauncher": {}}


def test_dynamic_schema_in_production_mode():
    """Test that the dynamic schema excludes dev-only options when in production mode"""
    # Simulate production mode by not setting DAGSTER_IS_DEV_CLI
    with patch.dict(os.environ, {}, clear=True):
        schema = get_dynamic_executor_config_schema()
        
        # In production mode, DockerRunLauncher should NOT be available in launcher options
        launcher_selector_fields = schema["launcher"].config_type.fields
        assert "DockerRunLauncher" not in launcher_selector_fields
        
        # In production mode, docker_executor should NOT be available in executor options
        executor_selector_fields = schema["executor"].config_type.fields
        assert "docker_executor" not in executor_selector_fields
        
        # Default should be AzureContainerAppJobRunLauncher in production mode
        assert schema["launcher"].default_value == {"AzureContainerAppJobRunLauncher": {}}


def test_dynamic_schema_executor_options_dev_vs_prod():
    """Test that executor options differ between dev and prod modes"""
    # Test dev mode
    with patch.dict(os.environ, {"DAGSTER_IS_DEV_CLI": "1"}):
        dev_schema = get_dynamic_executor_config_schema()
        dev_executor_fields = dev_schema["executor"].config_type.fields
    
    # Test production mode
    with patch.dict(os.environ, {}, clear=True):
        prod_schema = get_dynamic_executor_config_schema()
        prod_executor_fields = prod_schema["executor"].config_type.fields
    
    # Dev should have docker_executor, prod should not
    assert "docker_executor" in dev_executor_fields
    assert "docker_executor" not in prod_executor_fields
    
    # Both should have common executors
    assert "in_process_executor" in dev_executor_fields
    assert "in_process_executor" in prod_executor_fields
    assert "multiprocess_executor" in dev_executor_fields
    assert "multiprocess_executor" in prod_executor_fields


def test_dynamic_schema_launcher_options_dev_vs_prod():
    """Test that launcher options differ between dev and prod modes"""
    # Test dev mode
    with patch.dict(os.environ, {"DAGSTER_IS_DEV_CLI": "1"}):
        dev_schema = get_dynamic_executor_config_schema()
        dev_launcher_fields = dev_schema["launcher"].config_type.fields
    
    # Test production mode
    with patch.dict(os.environ, {}, clear=True):
        prod_schema = get_dynamic_executor_config_schema()
        prod_launcher_fields = prod_schema["launcher"].config_type.fields
    
    # Dev should have DockerRunLauncher, prod should not
    assert "DockerRunLauncher" in dev_launcher_fields
    assert "DockerRunLauncher" not in prod_launcher_fields
    
    # Production should have AzureContainerAppJobRunLauncher
    assert "AzureContainerAppJobRunLauncher" in prod_launcher_fields
    assert "AzureContainerAppJobRunLauncher" in dev_launcher_fields  # Also available in dev for testing
    
    # Both should have DefaultRunLauncher
    assert "DefaultRunLauncher" in dev_launcher_fields
    assert "DefaultRunLauncher" in prod_launcher_fields