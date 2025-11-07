from dagster import (
    AssetChecksDefinition,
    AssetsDefinition,
    JobDefinition,
    ScheduleDefinition,
    SensorDefinition,
)
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition
)
import os
import sys
from pathlib import Path
import subprocess


def bootstrap_dev():
    """
    Function to set up the local dev server by:
    1. setting `DAGSTER_HOME` environment variable
    2. setting `DAGSTER_USER` environment variable
    3. running `dagster dev -f <script_name>.py` in a subprocess
    4. Validating the DAGSTER_USER environment variable for non-dev scenarios
    """
    # Start the Dagster UI and set necessary env vars
    if "--dev" in sys.argv:
        # Set environment variables
        home_dir = Path.home()
        dagster_user = home_dir.name
        dagster_home = home_dir / ".dagster_home"

        os.environ["DAGSTER_USER"] = dagster_user
        os.environ["DAGSTER_HOME"] = str(dagster_home)
        script = sys.argv[0]

        # Run the Dagster webserver
        try:
            subprocess.run(["dagster", "dev", "-f", script])
        except KeyboardInterrupt:
            print("\nShutting down cleanly...")

    if os.getenv("DAGSTER_IS_DEV_CLI"):  # set by dagster cli
        print("Running in local dev environment")

    # get the user from the environment, throw an error if variable is not set
    if not os.getenv("DAGSTER_USER"):
        raise RuntimeError((
            "Env var 'DAGSTER_USER' is not set. "
            "If you are running locally, don't forget the '--dev' cli argument"
            " e.g. uv run dagster_defs.py --dev"))


def collect_definitions(namespace):
    """
    Function to collect Dagster definitions from a namespace.
    Usage:
    # collect definitions from globals() namespace in current file
    collected_defs = collect_definitions(globals())

    # Create Definitions object passing collected definitions
    defs = dg.Definitions(
        assets=collected_defs["assets"],
        asset_checks=collected_defs["asset_checks"],
        jobs=collected_defs["jobs"],
        sensors=collected_defs["sensors"],
        schedules=collected_defs["schedules"],
    )
    """
    assets = []
    asset_checks = []
    jobs = []
    schedules = []
    sensors = []

    for obj in list(namespace.values()):
        if isinstance(obj, AssetsDefinition):
            assets.append(obj)
        if isinstance(obj, AssetChecksDefinition):
            asset_checks.append(obj)
        elif (isinstance(obj, JobDefinition)
              or isinstance(obj, UnresolvedAssetJobDefinition)):
            jobs.append(obj)
        elif isinstance(obj, ScheduleDefinition):
            schedules.append(obj)
        elif isinstance(obj, SensorDefinition):
            sensors.append(obj)

    return {
        "assets": assets,
        "asset_checks": asset_checks,
        "jobs": jobs,
        "schedules": schedules,
        "sensors": sensors,
    }
