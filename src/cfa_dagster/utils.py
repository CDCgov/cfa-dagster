import importlib.resources
import importlib.util
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import dagster as dg
import psycopg2
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from click.exceptions import Abort, NoArgsIsHelpError
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from dagster_graphql import DagsterGraphQLClient
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from .azure_keyvault import KEY_VAULT_URL_CFA_PREDICT

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

log = logging.getLogger(__name__)

LOCAL_HOSTNAME = "127.0.0.1"
LOCAL_PORT = 4000
DEFAULT_DEFS_FILE = "dagster_defs.py"
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


def configure_dev_db():
    if (
        not os.getenv("CFA_DG_PG_HOSTNAME")
        or not os.getenv("CFA_DG_PG_USERNAME")
        or not os.getenv("CFA_DG_PG_PASSWORD")
    ):
        # Fetch secrets
        credential = DefaultAzureCredential()
        key_vault_url = KEY_VAULT_URL_CFA_PREDICT
        client = SecretClient(vault_url=key_vault_url, credential=credential)
    else:
        return

    if not os.getenv("CFA_DG_PG_HOSTNAME"):
        db_host = client.get_secret("cfa-pg-dagster-dev-host").value
        os.environ["CFA_DG_PG_HOSTNAME"] = db_host
    if not os.getenv("CFA_DG_PG_USERNAME"):
        db_username = client.get_secret(
            "cfa-pg-dagster-dev-admin-username"
        ).value
        os.environ["CFA_DG_PG_USERNAME"] = db_username
    if not os.getenv("CFA_DG_PG_PASSWORD"):
        db_password = client.get_secret(
            "cfa-pg-dagster-dev-admin-password"
        ).value
        os.environ["CFA_DG_PG_PASSWORD"] = db_password

    existing_db_name = "postgres"

    # expecting this to be set with set_env_vars()
    user_db_name = os.environ["DAGSTER_USER"]

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


def set_env_vars():
    dagster_home = str(importlib.resources.files("cfa_dagster"))
    # used by cfa-dagster for database and blob storage locations
    if not os.getenv("DAGSTER_USER"):
        if os.getenv("GITHUB_ACTIONS"):
            os.environ["DAGSTER_USER"] = "github_actions"
        else:
            os.environ["DAGSTER_USER"] = Path.home().name.lower()
    # used by dagster
    if not os.getenv("DAGSTER_HOME"):
        os.environ["DAGSTER_HOME"] = dagster_home


def find_pyproject_toml(start_dir: Path) -> Optional[Path]:
    for parent in [start_dir, *start_dir.parents]:
        candidate = parent / "pyproject.toml"
        if candidate.is_file():
            return candidate
    return None


def get_dg_project_config(
    start_dir: Optional[Path] = None,
) -> tuple[Path, dict[str, Any]] | None:
    pyproj = find_pyproject_toml(start_dir or Path.cwd())
    if not pyproj:
        return None
    try:
        data = tomllib.loads(pyproj.read_text())
    except Exception:
        log.debug(
            "Unable to read [tool.dg] metadata from pyproject.toml",
            exc_info=True,
        )
        return None

    dg_config = data.get("tool", {}).get("dg", {})
    if dg_config.get("directory_type") not in ("project", "workspace"):
        return None

    project_config = dg_config.get("project", {})
    if not isinstance(project_config, dict):
        return None
    return pyproj, project_config


def resolve_project_module_path(
    pyproject_path: Path,
    module_name: str,
) -> Path | None:
    module_parts = module_name.split(".")
    module_path = Path(*module_parts)
    module_file = Path(*module_parts[:-1], f"{module_parts[-1]}.py")

    for base_dir in (pyproject_path.parent, pyproject_path.parent / "src"):
        candidate_file = base_dir / module_file
        log.debug(f"candidate_file: {candidate_file}")
        if candidate_file.is_file():
            return candidate_file.resolve()

        candidate_pkg = base_dir / module_path
        log.debug(f"candidate_pkg: {candidate_pkg}")
        if (
            candidate_pkg.is_dir()
            and (candidate_pkg / "__init__.py").is_file()
        ):
            return candidate_pkg.resolve()

    try:
        spec = importlib.util.find_spec(module_name)
    except (ImportError, ValueError):
        return None
    if not spec:
        return None
    if spec.submodule_search_locations:
        return Path(list(spec.submodule_search_locations)[0]).resolve()
    if spec.origin:
        origin = Path(spec.origin)
        if origin.is_file():
            return origin.resolve()
    return None


def get_defs_target(
    start_dir: Optional[Path] = None,
) -> Optional[str]:
    """
    Return the definitions file resolved from ``[tool.dg]`` metadata in
    ``pyproject.toml`` relative to the project root, or None when there is no
    ``[tool.dg]`` metadata.

    The dotted ``defs_module`` is converted to a relative ``.py`` path and
    checked against two layouts:

    - flat/package layout, e.g.::

          [tool.dg.project]
          root_module = "some_module"
          defs_module = "some_module.dagster_defs"

      checks ``pyproj_parent/some_module/dagster_defs.py``

    - src layout, which additionally checks under ``src/``, e.g.::

          [tool.dg.project]
          root_module = "dagster_defs"
          defs_module = "dagster_defs"

      checks ``pyproj_parent/dagster_defs.py`` and
      ``pyproj_parent/src/dagster_defs.py``

    When ``defs_module`` is omitted, dagster's default of
    ``<root_module>.definitions`` is used, e.g.::

          [tool.dg.project]
          root_module = "some_module"

      checks ``pyproj_parent/some_module/definitions.py`` and
      ``pyproj_parent/src/some_module/definitions.py``.

    Raises
    ------
    RuntimeError
        If ``[tool.dg]`` metadata is configured but cannot be resolved to a
        concrete ``.py`` file.
    """
    config = get_dg_project_config(start_dir)
    if not config:
        return None
    pyproj, project_config = config
    try:
        defs_module = project_config.get("defs_module")
        root_module = project_config.get("root_module")
        if not root_module:
            return None
        # dagster's default when no defs_module is configured is
        # <root_module>.definitions, e.g. src/<root_module>/definitions.py
        defs_module = defs_module or f"{root_module}.definitions"

        module_path = resolve_project_module_path(pyproj, defs_module)
        if module_path and module_path.is_file():
            log.debug(
                f"Resolved configured module '{defs_module}' to file:"
                f" {module_path}"
            )
            return os.path.relpath(module_path, pyproj.parent)
        raise RuntimeError(
            f"Configured defs module '{defs_module}' does not resolve to an"
            " existing .py file and is not importable in this environment."
            " A definitions file is required to launch."
        )
    except RuntimeError:
        raise
    except Exception:
        log.debug(
            "Unable to read [tool.dg] metadata from pyproject.toml",
            exc_info=True,
        )
        return None


def resolve_defs_file(start_dir: Path | None = None) -> str:
    """
    Resolve a concrete definitions file path to launch with.

    Remote executors require launching against a definitions file, so this
    always returns a path. Resolution order:

    1. A ``[tool.dg.project]`` defs target that resolves to an existing
       ``.py`` file on disk relative to ``pyproject.toml``
    2. A configured module that is importable, resolved to its on-disk
       file via :func:`importlib.util.find_spec`
    3. ``DEFAULT_DEFS_FILE`` when there is no ``[tool.dg]`` metadata

    Raises
    ------
    RuntimeError
        If ``[tool.dg]`` metadata is configured but neither the defs file
        nor the module can be resolved, since silently loading the default
        definitions could launch the wrong code.
    """
    defs_file = get_defs_target(start_dir)
    if defs_file:
        log.debug(f"Resolved defs file from pyproject.toml: {defs_file}")
        return defs_file
    return DEFAULT_DEFS_FILE


def _get_flag_value(args: list[str], *flags: str) -> str | None:
    for i, arg in enumerate(args):
        if arg in flags and i + 1 < len(args):
            return args[i + 1]
    return None


def _add_host_port(args: list[str]) -> list[str]:
    """Add default host/port args if not already present."""
    extra = []
    if "-h" not in args and "--host" not in args:
        extra += ["-h", LOCAL_HOSTNAME]
    if "-p" not in args and "--port" not in args:
        extra += ["-p", str(LOCAL_PORT)]
    return [*args, *extra]


def _has_target(args: list[str]) -> bool:
    return any(
        flag in args
        for flag in (
            "-f",
            "--python-file",
            "-m",
            "--module-name",
            "-w",
            "--workspace",
        )
    )


def _run_cli(
    cli,
    env_prefix: str,
    argv: list[str] | None = None,
    defs_file: Optional[str] = None,
    always_add_host_port: bool = False,
    add_defs_file_if_missing: bool = False,
):
    """
    Runs a cli tool, resolving a definitions file via
    :func:`resolve_defs_file` and launching with ``-f <file>`` when no
    python file target is provided.
    """
    set_env_vars()
    configure_dev_db()

    raw_args = argv if argv is not None else sys.argv
    log.debug(f"raw_args: {raw_args}")

    rest = raw_args[1:] if len(raw_args) > 1 else []
    first_subcommand = next(
        (arg for arg in rest if not arg.startswith("-")),
        None,
    )

    if always_add_host_port or first_subcommand in {"dev", "code-server"}:
        args = _add_host_port(rest)
        host = _get_flag_value(rest, "-h", "--host") or LOCAL_HOSTNAME
        port = int(_get_flag_value(rest, "-p", "--port") or str(LOCAL_PORT))
    else:
        args = list(rest)
        host = LOCAL_HOSTNAME
        port = LOCAL_PORT
    log.debug(f"args: {args}")

    if (
        first_subcommand in ("dev", "launch", "code-server")
        or add_defs_file_if_missing
    ):
        if not _has_target(args):
            defs_file = defs_file or resolve_defs_file()
            log.info(f"Using definitions file: {defs_file}")
            args = [*args, "-f", defs_file]

    if first_subcommand == "dev" and defs_file:
        from .hot_reload import start_hot_reloader_for_dev

        try:
            start_hot_reloader_for_dev(
                args=args,
                defs_file=defs_file,
                host=host,
                port=port,
            )
        except Exception:
            log.warning("Failed to start hot-reloader", exc_info=True)

    try:
        cli(args=args, auto_envvar_prefix=env_prefix, standalone_mode=False)
    except NoArgsIsHelpError:
        cli(
            args=["--help"],
            auto_envvar_prefix=env_prefix,
            standalone_mode=False,
        )
        sys.exit(0)
    except (KeyboardInterrupt, Abort):
        sys.exit(0)


def run_dagster_webserver():
    """
    Wrapper for the `dagster-webserver` cli
    """
    from dagster_webserver.cli import cli

    _run_cli(
        cli,
        "DAGSTER_WEBSERVER",
        always_add_host_port=True,
        add_defs_file_if_missing=True,
    )


def run_dagster():
    """
    Wrapper for the `dagster` cli
    """
    from dagster._cli import ENV_PREFIX, cli

    _run_cli(cli, ENV_PREFIX)


def run_dg(
    argv: Optional[list[str]] | None = None, defs_file: Optional[str] = None
):
    """
    Wrapper for the `dg` cli
    """
    from dagster_dg_cli.cli import ENV_PREFIX, cli

    _run_cli(cli, ENV_PREFIX, argv=argv, defs_file=defs_file)


def start_dev_env(caller_name: str):
    """
    Parameters:
    -----------
    caller_name: str
        Pass in the module's __name__ (e.g. `start_dev_env(__name__)`).

    Function to set up the local dev server by:
    1. creating a database on the dev server
    2. setting `DAGSTER_HOME` environment variable
    3. setting `DAGSTER_USER` environment variable
    4. running `dg dev *sys.argv[1]` if flags are included e.g. uv run dagster_defs.py -p 4001
        or
    4. running `dg *sys.argv[1]` if commands are provided e.g. uv run dagster_defs.py launch --job ...
    5. Validating the DAGSTER_USER environment variable for non-dev scenarios
    """
    # Start the Dagster UI and set necessary env vars if
    # called directly via `uv run dagster_defs.py` or `python dagster_defs.py`
    if caller_name == "__main__":
        defs_file = sys.argv[0]
        log.debug(f"defs_file: {defs_file}")
        run_dg(argv=[None, "dev", *sys.argv[1:]], defs_file=defs_file)


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
