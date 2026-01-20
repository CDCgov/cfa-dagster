import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote

import dagster as dg
import psycopg2
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from dagster_graphql import DagsterGraphQLClient
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

LOCAL_HOSTNAME = "127.0.0.1"
LOCAL_PORT = 3000
PROD_HOSTNAME = os.getenv(
    "DAGSTER_WEBSERVER_URL",
    "dagster.apps.edav.ext.cdc.gov"
)


def get_webserver_url() -> str:
    if os.getenv("DAGSTER_IS_DEV_CLI"):  # set by dagster cli
        return f"http://{LOCAL_HOSTNAME}:{LOCAL_PORT}"
    else:
        return f"https://{PROD_HOSTNAME}"


def get_runs_url_for_tag(tag_key: str, tag_value: str) -> str:
    encoded_value = quote(f"tag:{tag_key}={tag_value}")
    return f"{get_webserver_url()}/runs?q[0]={encoded_value}"


def get_graphql_client() -> DagsterGraphQLClient:
    if os.getenv("DAGSTER_IS_DEV_CLI"):  # set by dagster cli
        return DagsterGraphQLClient(
            hostname=LOCAL_HOSTNAME,
            port_number=LOCAL_PORT
        )
    else:
        return DagsterGraphQLClient(hostname=PROD_HOSTNAME)


def create_dev_env():
    # Authenticate using DefaultAzureCredential
    credential = DefaultAzureCredential()

    # Connect to the CFA-Tools Key Vault
    key_vault_url = "https://CFA-Predict.vault.azure.net/"
    client = SecretClient(vault_url=key_vault_url, credential=credential)

    # Fetch secrets
    db_host = client.get_secret("cfa-pg-dagster-dev-host").value
    db_username = client.get_secret("cfa-pg-dagster-dev-admin-username").value
    db_password = client.get_secret("cfa-pg-dagster-dev-admin-password").value
    existing_db_name = "postgres"

    # Create a new database for the user based on home directory
    # using the $USER env var includes the domain extension which is not
    # valid for a postgres db name
    user_db_name = Path.home().name

    conn = None
    try:
        conn = psycopg2.connect(
            dbname=existing_db_name,
            user=db_username,
            password=db_password,
            host=db_host,
            port="5432",
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"CREATE DATABASE {user_db_name} TEMPLATE template0"
            )
            print(f"Database '{user_db_name}' created successfully.")
        except psycopg2.errors.DuplicateDatabase:
            print(f"Database '{user_db_name}' already exists.")
        finally:
            cursor.close()
    except psycopg2.Error as e:
        print(f"Error connecting to or creating database: {e}")
    finally:
        if conn:
            conn.close()

    # create a dagster.yaml file with the new database
    dagster_yaml_raw = f"""
    storage:
      postgres:
        postgres_db:
          hostname: {db_host}
          username: {db_username}
          password: {db_password}
          db_name: {user_db_name}
          port: 5432

    compute_logs:
      module: dagster_azure.blob.compute_log_manager
      class: AzureBlobComputeLogManager
      config:
        storage_account: cfadagsterdev
        container: cfadagsterdev
        default_azure_credential:
        prefix: "log-files"
        local_dir: "/tmp/dagster-logs"
        upload_interval: 30
        show_url_only: false

    run_coordinator:
      module: dagster.core.run_coordinator
      class: QueuedRunCoordinator
      config:
        dequeue_use_threads: true
        dequeue_num_workers: 4

    run_launcher:
      module: cfa_dagster
      class: DynamicRunLauncher

    run_monitoring:
      enabled: true

    concurrency:
      default_op_concurrency_limit: 1000
      runs:
        max_concurrent_runs: 1000

    backfills:
      use_threads: true
      num_workers: 4
      num_submit_workers: 4

    """
    # write to ~/.dagster_home/dagster.yaml
    dagster_home = Path.home() / ".dagster_home"
    dagster_home.mkdir(parents=True, exist_ok=True)
    config_path = dagster_home / "dagster.yaml"
    config_path.write_text(dagster_yaml_raw)
    print("Created ~/.dagster_home/dagster.yaml")


def start_dev_env(caller_name: str):
    """
    Parameters:
    -----------
    caller_name: str
        Pass in the module's __name__ (e.g. `start_dev_env(__name__)`).

    Function to set up the local dev server by:
    1. creating a database on the dev server (one time only, or with --configure)
    2. creating a ~/.dagster_home/dagster.yaml file (one time only, or with --configure)
    3. setting `DAGSTER_HOME` environment variable
    4. setting `DAGSTER_USER` environment variable
    5. running `dagster dev -f <script_name>.py` in a subprocess
    6. Validating the DAGSTER_USER environment variable for non-dev scenarios
    """
    is_production = not os.getenv("DAGSTER_IS_DEV_CLI")  # set by dagster cli
    home_dir = Path.home()
    dagster_user = home_dir.name
    dagster_home = home_dir / ".dagster_home"
    dagster_yaml = dagster_home / "dagster.yaml"

    # Start the Dagster UI and set necessary env vars if
    # called directly via `uv run`
    if caller_name == "__main__":
        if "--configure" in sys.argv or not os.path.exists(dagster_yaml):
            create_dev_env()
        # Set environment variables
        os.environ["DAGSTER_USER"] = dagster_user
        os.environ["DAGSTER_HOME"] = str(dagster_home)
        script = sys.argv[0]

        # Run the Dagster webserver
        try:
            subprocess.run(
                [
                    "dagster",
                    "dev",
                    "-h",
                    LOCAL_HOSTNAME,
                    "-p",
                    f"{LOCAL_PORT}",
                    "-f",
                    script,
                ]
            )
        except KeyboardInterrupt:
            print("\nShutting down cleanly...")

    if not is_production:
        print("Running in local dev environment")

    # get the user from the environment, throw an error if variable is not set
    if not os.getenv("DAGSTER_USER"):
        raise RuntimeError("Env var 'DAGSTER_USER' is not set!")


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
