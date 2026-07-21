import importlib.util
import logging
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from dagster_graphql import DagsterGraphQLClient

from .utils import find_pyproject_toml

if TYPE_CHECKING:
    from watchdog.observers import Observer

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

log = logging.getLogger(__name__)

RELOAD_MUTATION = """
mutation ReloadWorkspace {
  reloadWorkspace {
    __typename
    ... on Workspace { id }
    ... on PythonError { message stack }
  }
}
"""


def _read_root_module(pyproject_path: Path) -> Optional[str]:
    try:
        with open(pyproject_path, "rb") as f:
            data = tomllib.load(f)
        return (
            data.get("tool", {})
            .get("dg", {})
            .get("project", {})
            .get("root_module")
        )
    except Exception:
        return None


def _resolve_module_path(
    module_name: str,
    search_dir: Optional[Path] = None,
) -> Optional[Path]:
    try:
        spec = importlib.util.find_spec(module_name)
        if spec is not None:
            if spec.submodule_search_locations:
                pkg_dir = list(spec.submodule_search_locations)[0]
                return Path(pkg_dir)
            if spec.origin and spec.origin.endswith(".py"):
                return Path(spec.origin).resolve()
    except (ImportError, ValueError, AttributeError):
        pass

    for base in [search_dir, Path.cwd()] if search_dir else [Path.cwd()]:
        if base is None:
            continue
        candidate_file = (base / module_name).with_suffix(".py")
        if candidate_file.is_file():
            return candidate_file.resolve()
        candidate_pkg = base / module_name
        if (
            candidate_pkg.is_dir()
            and (candidate_pkg / "__init__.py").is_file()
        ):
            return candidate_pkg.resolve()

    return None


def _collect_py_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.rglob("*.py") if p.is_file())


def resolve_target_paths(
    entry_point: Optional[str | Path] = None,
    pyproject_path: Optional[str | Path] = None,
) -> list[Path]:
    targets: set[Path] = set()

    if entry_point:
        ep = Path(entry_point)
        if ep.is_file():
            targets.add(ep.resolve())

    pyproj = (
        Path(pyproject_path)
        if pyproject_path
        else find_pyproject_toml(Path.cwd())
    )
    root_module = _read_root_module(pyproj) if pyproj else None

    if root_module:
        search_dir = pyproj.parent if pyproj else None
        module_path = _resolve_module_path(root_module, search_dir=search_dir)
        if module_path:
            if module_path.is_dir():
                targets.update(_collect_py_files(module_path))
            elif module_path.is_file():
                targets.add(module_path.resolve())
        else:
            log.warning("Could not resolve root_module '%s'", root_module)

    if not targets and entry_point:
        ep = Path(entry_point)
        if ep.is_file():
            targets.add(ep.resolve())

    return sorted(targets)


def _extract_python_file(args: list[str]) -> Optional[str]:
    for i, arg in enumerate(args):
        if arg in ("-f", "--python-file") and i + 1 < len(args):
            return args[i + 1]
    return None


def reload_via_graphql(host: str, port: int) -> bool:
    client = DagsterGraphQLClient(hostname=host, port_number=port)
    try:
        result = client._execute(RELOAD_MUTATION)
        typename = result.get("reloadWorkspace", {}).get("__typename")
        if typename == "Workspace":
            log.info("Reloaded workspace")
            return True
        log.warning("Workspace reload returned: %s", result)
        return False
    except Exception as e:
        log.error("Failed to reload via GraphQL: %s", e)
        return False


def wait_for_server(
    host: str,
    port: int,
    max_retries: int = 15,
    delay: float = 2.0,
) -> bool:
    import requests

    url = f"http://{host}:{port}/graphql"
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                url, json={"query": "{ __typename }"}, timeout=5
            )
            if resp.status_code == 200:
                return True
        except requests.RequestException:
            pass
        if attempt < max_retries - 1:
            log.info(
                "Waiting for Dagster server at %s (attempt %d/%d)...",
                url,
                attempt + 1,
                max_retries,
            )
            time.sleep(delay)
    return False


class HotReloader:
    def __init__(
        self,
        paths: list[str | Path],
        host: str,
        port: int,
    ):
        self._paths = [Path(p).resolve() for p in paths]
        self._host = host
        self._port = port
        self._observer: Optional[Observer] = None  # type: ignore[reportInvalidTypeForm]
        self._server_ready = False

    def start(self):
        try:
            from watchdog.events import FileSystemEventHandler
            from watchdog.observers import Observer
        except ImportError:
            log.warning(
                "watchdog is not installed. "
                "Install it with: pip install watchdog  "
                "or: uv add 'cfa-dagster[dev]'"
            )
            return

        if not self._paths:
            log.info("No paths to watch, skipping hot-reloader")
            return

        resolved = []
        for p in self._paths:
            if not p.exists():
                log.warning("Watch path does not exist: %s", p)
                continue
            resolved.append(p)

        if not resolved:
            log.info("No valid paths to watch, skipping hot-reloader")
            return

        callback = self._on_files_changed
        debounce = 0.5

        class _Handler(FileSystemEventHandler):
            def __init__(self):
                self._timer: Optional[threading.Timer] = None
                self._lock = threading.Lock()

            def on_modified(self, event):
                self._on_event(event.src_path)

            def on_created(self, event):
                self._on_event(event.src_path)

            def _on_event(self, src_path: str):
                if not src_path.endswith(".py"):
                    return
                with self._lock:
                    if self._timer and self._timer.is_alive():
                        self._timer.cancel()
                    self._timer = threading.Timer(debounce, callback)
                    self._timer.daemon = True
                    self._timer.start()

        self._observer = Observer()
        for path in resolved:
            if path.is_file():
                self._observer.schedule(
                    _Handler(), str(path.parent), recursive=False
                )
            else:
                self._observer.schedule(_Handler(), str(path), recursive=True)

        self._observer.daemon = True
        self._observer.start()
        if len(resolved) == 1 and resolved[0].is_file():
            log.info(f"Hot-reloader: watching {resolved[0]}")
        elif all(p.is_file() for p in resolved):
            for p in resolved:
                log.info(f"Hot-reloader: watching {p}")
        else:
            dirs = sorted(
                {str(p.parent if p.is_file() else p) for p in resolved}
            )
            log.info(
                f"Hot-reloader: watching python files under {', '.join(dirs)}"
            )

    def stop(self):
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None

    def _on_files_changed(self):
        if not self._server_ready:
            self._server_ready = wait_for_server(self._host, self._port)
            if not self._server_ready:
                return
        log.info("Hot-reloader: Change detected, reloading workspace...")
        reload_via_graphql(host=self._host, port=self._port)


def start_hot_reloader_for_dev(
    args: list[str],
    defs_file: str,
    host: str,
    port: int,
    pyproject_path: Optional[str | Path] = None,
) -> Optional[HotReloader]:
    ep = _extract_python_file(args) or defs_file
    paths = resolve_target_paths(entry_point=ep, pyproject_path=pyproject_path)
    reloader = HotReloader(paths=paths, host=host, port=port)
    reloader.start()
    return reloader
