"""cfa_dagster"""

# ruff: noqa: F401
import logging
import os

from .azure_adls2.io_manager import ADLS2PickleIOManager
from .azure_batch.executor import azure_batch_executor
from .azure_container_app_job.executor import azure_container_app_job_executor
from .azure_container_app_job.launcher import AzureContainerAppJobRunLauncher
from .docker.executor import docker_executor
from .execution import (
    DynamicRunLauncher,
    dynamic_executor,
    ExecutionConfig,
)
from .utils import (
    collect_definitions,
    get_latest_metadata_for_partition,
    get_runs_url_for_tag,
    get_webserver_url,
    launch_asset_backfill,
    start_dev_env,
)

# Create a logger for the package
log = logging.getLogger(__name__)

# Set the log level from an environment variable, defaulting to INFO
log_level_name = os.environ.get("CFA_DAGSTER_LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
log.setLevel(log_level)

# Add a stream handler to output logs to the console if no handlers are configured
if not log.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    log.addHandler(handler)
