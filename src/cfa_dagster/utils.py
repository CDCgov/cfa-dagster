from dagster_graphql import DagsterGraphQLClient
import dagster as dg
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
            subprocess.run([
                "dagster",
                "dev",
                "-h",
                "127.0.0.1",
                "-p",
                "3000",
                "-f",
                script
            ])
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
        if isinstance(obj, dg.AssetsDefinition):
            assets.append(obj)
        if isinstance(obj, dg.AssetChecksDefinition):
            asset_checks.append(obj)
        elif (isinstance(obj, dg.JobDefinition)
              or isinstance(obj, UnresolvedAssetJobDefinition)):
            jobs.append(obj)
        elif isinstance(obj, dg.ScheduleDefinition):
            schedules.append(obj)
        elif isinstance(obj, dg.SensorDefinition):
            sensors.append(obj)

    return {
        "assets": assets,
        "asset_checks": asset_checks,
        "jobs": jobs,
        "schedules": schedules,
        "sensors": sensors,
    }


def launch_asset_backfill(
    asset_keys: list[str],
    partition_keys: list[str],
    tags: dict = {"programmed_backfill": "true"},
    run_config: dg.RunConfig = dg.RunConfig(),
):
    """
    Function to launch an asset backfill via the GraphQL client
    """

    if os.getenv("DAGSTER_IS_DEV_CLI"):  # set by dagster cli
        client = DagsterGraphQLClient(hostname="127.0.0.1", port_number=3000)
    else:
        client = DagsterGraphQLClient(hostname="dagster.apps.edav.ext.cdc.gov")

    query = """
    mutation LaunchPartitionBackfill(
        $backfillParams: LaunchBackfillParams!
    ) {
        launchPartitionBackfill(backfillParams: $backfillParams) {
            __typename
            ... on LaunchBackfillSuccess {
                backfillId
            }
            ... on PythonError {
                message
                stack
            }
        }
    }
    """
    variables = {
        "backfillParams": {
            "partitionNames": partition_keys,
            "tags": [{"key": k, "value": v} for k, v in (tags or {}).items()],
            "assetSelection": [{"path": key.split("/")} for key in asset_keys],
            "runConfigData": run_config.to_config_dict(),
        }
    }
    print(f"variables: '{variables}'")
    result = client._execute(query, variables=variables)
    print(f"result: '{result}'")
    payload = result.get("launchPartitionBackfill")
    if payload["__typename"] == "LaunchBackfillSuccess":
        return payload["backfillId"]
    else:
        raise RuntimeError(f"Backfill failed: {payload['message']}")


def get_latest_metadata_for_partition(
    instance: dg.DagsterInstance,
    asset_key_str: str,
    partition_key: str
) -> dict:
    """
    Returns the metadata from the latest materialization for a given asset and partition.

    Used to pass data between assets via metadata when typical outputs are not available like when using BackfillPolicy.single_run().
    """
    instance = dg.DagsterInstance.get()
    asset_key = dg.AssetKey(asset_key_str)

    # Filter for materialization events for this asset and partition
    event_records_filter = dg.EventRecordsFilter(
        asset_key=asset_key,
        event_type=dg.DagsterEventType.ASSET_MATERIALIZATION,
        asset_partitions=[partition_key],
    )

    # Fetch all matching events
    events = instance.get_event_records(event_records_filter)

    # Filter materializations with non-empty metadata
    materializations = [
        e.event_log_entry
        for e in events
        if e.event_log_entry.asset_materialization is not None
        and e.event_log_entry.asset_materialization.metadata
    ]

    # Sort by event timestamp descending
    materializations.sort(key=lambda e: e.timestamp, reverse=True)

    # Return metadata from the latest one
    if materializations:
        metadata = materializations[0].asset_materialization.metadata
        unwrapped_metadata = {k: v.value for k, v in metadata.items()}
        return unwrapped_metadata
    else:
        return {}
