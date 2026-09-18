"""cfa_dagster"""

# ruff: noqa: F401
import logging
import os

from .azure_adls2.filesystem_io_manager import ADLS2FilesystemIOManager
from .azure_adls2.filesystem_path import ADLS2Path
from .azure_adls2.pickle_io_manager import ADLS2PickleIOManager
from .azure_batch.executor import azure_batch_executor
from .azure_container_app_job.executor import azure_container_app_job_executor
from .azure_container_app_job.launcher import AzureContainerAppJobRunLauncher
from .azure_keyvault import AzureKeyVaultResource
from .cli import start_dev_env
from .docker.executor import docker_executor
from .dynamic_graph import (
    GraphDimension,
    GraphDimensionExclusion,
    dynamic_graph_asset,
)
from .execution import (
    CFAQueuedRunCoordinator,
    DynamicRunLauncher,
    ExecutionConfig,
    SelectorConfig,
    dynamic_executor,
)
from .utils import (
    collect_definitions,
    get_latest_metadata_for_partition,
    get_run_timestamp,
    get_runs_url_for_tag,
    get_webserver_url,
    is_production,
    launch_asset_backfill,
    require_dagster_user,
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


# below is required to suppress the warnings emitted when calling `cfa-dg dev` and using `dg.load_from_defs_folder` due to the way cfa-dagster always calls `dg dev -f some_file.py`. We need the `-f` flag specifically so Dagster can find the local python file when running in docker/Azure Batch.
class IgnoreCodeLocationWarning(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()

        return not (
            record.name == "dagster.components.core.component_tree"
            and record.levelno == logging.WARNING
            and message.startswith("The code location name ")
            and " configured on this project does not match the name "
            in message
            and " this code location is registered under." in message
        )


logger = logging.getLogger("dagster.components.core.component_tree")
logger.addFilter(IgnoreCodeLocationWarning())
