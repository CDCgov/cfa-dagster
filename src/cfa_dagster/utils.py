import logging
import os
from datetime import datetime
from urllib.parse import quote

import dagster as dg
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from dagster_graphql import DagsterGraphQLClient

log = logging.getLogger(__name__)

LOCAL_HOSTNAME = "127.0.0.1"
LOCAL_PORT = 4000
PROD_HOSTNAME = os.getenv(
    "DAGSTER_WEBSERVER_URL", "dagster.apps.edav.ext.cdc.gov"
)


def require_dagster_user() -> str:
    value = os.getenv("DAGSTER_USER")
    if not value:
        raise RuntimeError("DAGSTER_USER env var is required but not set. ")
    return value


def is_production() -> bool:
    # If DAGSTER_IS_DEV_CLI is set, we're in dev mode regardless of CFA_DAGSTER_ENV
    if os.getenv("DAGSTER_IS_DEV_CLI"):
        return False
    # Otherwise, check if we're in production based on CFA_DAGSTER_ENV
    return os.getenv("CFA_DAGSTER_ENV") == "prod"


def get_webserver_url() -> str:
    if is_production():
        return f"https://{PROD_HOSTNAME}"
    else:
        return f"http://{LOCAL_HOSTNAME}:{LOCAL_PORT}"


def get_runs_url_for_tag(tag_key: str, tag_value: str) -> str:
    encoded_value = quote(f"tag:{tag_key}={tag_value}")
    return f"{get_webserver_url()}/runs?q[0]={encoded_value}"


def get_graphql_client() -> DagsterGraphQLClient:
    if is_production():
        return DagsterGraphQLClient(hostname=PROD_HOSTNAME)
    else:
        return DagsterGraphQLClient(
            hostname=LOCAL_HOSTNAME, port_number=LOCAL_PORT
        )


def get_run_timestamp(run: dg.DagsterRun) -> datetime:
    """
    Return the run start timestamp parsed from the ``cfa_dagster/run_ts`` tag.

    Parameters
    ----------
    run : dg.DagsterRun
        The Dagster run object containing run metadata and tags.

    Returns
    -------
    datetime
        A timezone-aware ``datetime`` object parsed from the ISO 8601 timestamp
        stored in the ``cfa_dagster/run_ts`` tag.

    Raises
    ------
    KeyError
        If the ``cfa_dagster/run_ts`` tag is not present on the run.
    ValueError
        If the tag value is not a valid ISO 8601 datetime string.
    """
    ts = run.tags["cfa_dagster/run_ts"]
    return datetime.fromisoformat(ts)


def start_dev_env(caller_name: str):
    from .cli import start_dev_env as _start_dev_env

    _start_dev_env(caller_name)


# TODO: look into dg.load_definitions_from_current_module()
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
        if isinstance(obj, dg.AssetsDefinition) or isinstance(
            obj, dg.AssetSpec
        ):
            assets.append(obj)
        if isinstance(obj, dg.AssetChecksDefinition):
            asset_checks.append(obj)
        elif isinstance(obj, dg.JobDefinition) or isinstance(
            obj, UnresolvedAssetJobDefinition
        ):
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
    tags: dict = {},
    run_config: dg.RunConfig = dg.RunConfig(),
):
    tags["programmed_backfill"] = "true"
    """
    Function to launch an asset backfill via the GraphQL client
    """
    client = get_graphql_client()

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
    instance: dg.DagsterInstance, asset_key_str: str, partition_key: str
) -> dict:
    """
    Returns the metadata from the latest materialization for a given asset and partition.

    Used to pass data between assets via metadata when typical outputs are not available like when using BackfillPolicy.single_run().
    """
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
