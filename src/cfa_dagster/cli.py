import importlib.resources
import importlib.util
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import psycopg2
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from click.exceptions import Abort, NoArgsIsHelpError
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
DEFAULT_WORKSPACE_FILES = ("workspace.yaml", "workspace.yml")
ALLOW_DEFAULT_DEFS_OVERRIDE_ENV = "CFA_DAGSTER_ALLOW_DEFAULT_DEFS_OVERRIDE"
TargetKind = Literal["python_file", "module", "workspace"]


def configure_dev_db():
    if (
        not os.getenv("CFA_DG_PG_HOSTNAME")
        or not os.getenv("CFA_DG_PG_USERNAME")
        or not os.getenv("CFA_DG_PG_PASSWORD")
    ):
        credential = DefaultAzureCredential()
        client = SecretClient(
            vault_url=KEY_VAULT_URL_CFA_PREDICT,
            credential=credential,
        )
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
    if not os.getenv("DAGSTER_USER"):
        if os.getenv("GITHUB_ACTIONS"):
            os.environ["DAGSTER_USER"] = "github_actions"
        else:
            os.environ["DAGSTER_USER"] = Path.home().name.lower()
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
) -> tuple[Path, dict] | None:
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
    """Resolve a [tool.dg] definitions module to a relative Python file."""
    config = get_dg_project_config(start_dir)
    if not config:
        return None
    pyproj, project_config = config
    try:
        defs_module = project_config.get("defs_module")
        root_module = project_config.get("root_module")
        if not root_module:
            return None
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
    """Resolve the configured definitions file or return the default file."""
    defs_file = get_defs_target(start_dir)
    if defs_file:
        log.debug(f"Resolved defs file from pyproject.toml: {defs_file}")
        return defs_file
    return DEFAULT_DEFS_FILE


@dataclass
class _CliTargetPlan:
    args: list[str]
    kind: TargetKind | None = None
    value: str | None = None
    hot_reload_file: str | None = None
    hot_reload_disabled_reason: str | None = None


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


def _get_cli_target(args: list[str]) -> tuple[TargetKind, str] | None:
    targets: tuple[tuple[TargetKind, tuple[str, ...]], ...] = (
        ("python_file", ("-f", "--python-file")),
        ("module", ("-m", "--module-name")),
        ("workspace", ("-w", "--workspace")),
    )
    for i, arg in enumerate(args[:-1]):
        for kind, flags in targets:
            if arg in flags:
                return kind, args[i + 1]
    return None


def _find_default_workspace_file(start_dir: Path | None = None) -> str | None:
    base_dir = start_dir or Path.cwd()
    for filename in DEFAULT_WORKSPACE_FILES:
        candidate = base_dir / filename
        if candidate.is_file():
            return filename
    return None


def _resolve_module_to_python_file(
    module_name: str,
    start_dir: Path | None = None,
) -> str | None:
    pyproject_path = find_pyproject_toml(start_dir or Path.cwd())
    if pyproject_path:
        module_path = resolve_project_module_path(pyproject_path, module_name)
        if module_path and module_path.is_file():
            return os.path.relpath(module_path, pyproject_path.parent)

    try:
        spec = importlib.util.find_spec(module_name)
    except (ImportError, ValueError):
        return None
    if spec and spec.origin:
        origin = Path(spec.origin)
        if origin.is_file():
            return str(origin.resolve())
    return None


def _plan_cli_target(
    args: list[str],
    defs_file: str | None = None,
    add_target_if_missing: bool = False,
    start_dir: Path | None = None,
) -> _CliTargetPlan:
    target = _get_cli_target(args)
    if target:
        kind, value = target
        if kind == "python_file":
            return _CliTargetPlan(
                args=args,
                kind=kind,
                value=value,
                hot_reload_file=value,
            )
        if kind == "workspace":
            return _CliTargetPlan(
                args=args,
                kind=kind,
                value=value,
                hot_reload_disabled_reason=(
                    "Hot reloading is disabled for workspace targets."
                ),
            )

        module_file = _resolve_module_to_python_file(value, start_dir)
        return _CliTargetPlan(
            args=args,
            kind=kind,
            value=value,
            hot_reload_file=module_file,
            hot_reload_disabled_reason=None
            if module_file
            else f"Hot reloading is disabled because module target '{value}' could not resolve to a Python file.",
        )

    if not add_target_if_missing:
        return _CliTargetPlan(args=args)

    if defs_file:
        return _CliTargetPlan(
            args=[*args, "-f", defs_file],
            kind="python_file",
            value=defs_file,
            hot_reload_file=defs_file,
        )

    defs_target = get_defs_target(start_dir)
    if defs_target:
        log.info(f"Using definitions file: {defs_target}")
        return _CliTargetPlan(
            args=[*args, "-f", defs_target],
            kind="python_file",
            value=defs_target,
            hot_reload_file=defs_target,
        )

    workspace_file = _find_default_workspace_file(start_dir)
    if workspace_file:
        log.info(f"Using workspace file: {workspace_file}")
        return _CliTargetPlan(
            args=[*args, "-w", workspace_file],
            kind="workspace",
            value=workspace_file,
            hot_reload_disabled_reason=(
                "Hot reloading is disabled for workspace targets."
            ),
        )

    log.info(f"Using definitions file: {DEFAULT_DEFS_FILE}")
    return _CliTargetPlan(
        args=[*args, "-f", DEFAULT_DEFS_FILE],
        kind="python_file",
        value=DEFAULT_DEFS_FILE,
        hot_reload_file=DEFAULT_DEFS_FILE,
    )


def _replace_default_defs_target(
    args: list[str],
) -> tuple[list[str], str | None]:
    """
    Function to override the -f dagster_defs.py flag that is passed by `dagster code-server start`
    This can be removed once all code locations are using cfa-dagster >= 1.4.4
    """
    next_args = list(args)
    for i, arg in enumerate(next_args[:-1]):
        if arg not in ("-f", "--python-file"):
            continue
        if next_args[i + 1] != DEFAULT_DEFS_FILE:
            continue
        try:
            defs_file = resolve_defs_file()
        except Exception:
            log.debug(
                "Unable to resolve definitions file; keeping %s",
                DEFAULT_DEFS_FILE,
                exc_info=True,
            )
            return next_args, DEFAULT_DEFS_FILE
        if defs_file != DEFAULT_DEFS_FILE:
            log.info(
                "Replacing default definitions file %s with %s",
                DEFAULT_DEFS_FILE,
                defs_file,
            )
            next_args[i + 1] = defs_file
        return next_args, next_args[i + 1]
    return next_args, None


def _invoke_cli(cli, env_prefix: str, args: list[str]):
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


def _run_cli(
    cli,
    env_prefix: str,
    argv: list[str] | None = None,
):
    """Run a subcommand-driven Dagster CLI with target planning."""
    set_env_vars()
    configure_dev_db()

    raw_args = argv if argv is not None else sys.argv
    log.debug(f"raw_args: {raw_args}")

    rest = raw_args[1:] if len(raw_args) > 1 else []
    first_subcommand = next(
        (arg for arg in rest if not arg.startswith("-")),
        None,
    )

    if first_subcommand in {"dev", "code-server"}:
        args = _add_host_port(rest)
        host = _get_flag_value(rest, "-h", "--host") or LOCAL_HOSTNAME
        port = int(_get_flag_value(rest, "-p", "--port") or str(LOCAL_PORT))
    else:
        args = list(rest)
        host = LOCAL_HOSTNAME
        port = LOCAL_PORT
    log.debug(f"args: {args}")
    defs_file = None

    # need to explicitly pass the -f flag for code locations that don't have
    # the fallback behavior for dagster code-server start yet
    # using an env var to override the -f flag with the fallback behavior
    # for code locations that have the latest cfa-dagster code
    if (
        first_subcommand == "code-server"
        and os.getenv(ALLOW_DEFAULT_DEFS_OVERRIDE_ENV) == "true"
    ):
        args, replaced_defs_file = _replace_default_defs_target(args)
        defs_file = replaced_defs_file or defs_file

    should_plan_target = first_subcommand in ("dev", "launch", "code-server")
    plan = _plan_cli_target(
        args,
        defs_file=defs_file,
        add_target_if_missing=should_plan_target,
    )
    args = plan.args

    if first_subcommand == "dev":
        if plan.hot_reload_file:
            from .hot_reload import start_hot_reloader_for_dev

            try:
                start_hot_reloader_for_dev(
                    args=args,
                    defs_file=plan.hot_reload_file,
                    host=host,
                    port=port,
                )
            except Exception:
                log.warning("Failed to start hot-reloader", exc_info=True)
        elif plan.hot_reload_disabled_reason:
            log.warning(plan.hot_reload_disabled_reason)

    _invoke_cli(cli, env_prefix, args)


def run_dagster_webserver():
    """Wrapper for the `dagster-webserver` cli."""
    from dagster_webserver.cli import cli

    set_env_vars()
    configure_dev_db()

    rest = sys.argv[1:] if len(sys.argv) > 1 else []
    args = _add_host_port(rest)

    if not _get_cli_target(args):
        defs_target = get_defs_target()
        if defs_target:
            log.info(f"Using definitions file: {defs_target}")
            args = [*args, "-f", defs_target]
        elif workspace_file := _find_default_workspace_file():
            log.info(f"Using workspace file: {workspace_file}")
            args = [*args, "-w", workspace_file]
        elif (Path.cwd() / DEFAULT_DEFS_FILE).is_file():
            log.info(f"Using definitions file: {DEFAULT_DEFS_FILE}")
            args = [*args, "-f", DEFAULT_DEFS_FILE]
        else:
            log.info("Using empty workspace")
            args = [*args, "--empty-workspace"]

    _invoke_cli(cli, "DAGSTER_WEBSERVER", args)


def run_dagster():
    """Wrapper for the `dagster` cli."""
    from dagster._cli import ENV_PREFIX, cli

    _run_cli(cli, ENV_PREFIX)


def run_dg(
    argv: Optional[list[str]] | None = None,
):
    """Wrapper for the `dg` cli."""
    from dagster_dg_cli.cli import ENV_PREFIX, cli

    _run_cli(cli, ENV_PREFIX, argv=argv)


def start_dev_env(caller_name: str):
    """
    Start a local dg dev server when a definitions file is run directly.

    Pass in the module's __name__, e.g. ``start_dev_env(__name__)``.
    """
    if caller_name == "__main__":
        defs_file = sys.argv[0]
        log.debug(f"defs_file: {defs_file}")
        run_dg(argv=[None, "dev", "-f", defs_file, *sys.argv[1:]])
