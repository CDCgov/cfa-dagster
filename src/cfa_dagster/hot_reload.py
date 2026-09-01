import logging
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Optional

from dagster_graphql import DagsterGraphQLClient

from .cli import get_dg_project_config, resolve_project_module_path

if TYPE_CHECKING:
    from watchdog.observers import Observer

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


def _collect_py_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.rglob("*.py") if p.is_file())


def resolve_target_paths(
    entry_point: Optional[str | Path] = None,
    pyproject_path: Optional[str | Path] = None,
) -> list[Path]:
    targets: set[Path] = set()

    config = (
        get_dg_project_config(Path(pyproject_path).parent)
        if pyproject_path
        else get_dg_project_config(Path.cwd())
    )
    pyproj, project_config = config if config else (None, {})
    root_module = project_config.get("root_module")

    if pyproj and root_module:
        module_path = resolve_project_module_path(pyproj, root_module)
        if module_path:
            if module_path.is_dir():
                targets.update(_collect_py_files(module_path))
            elif module_path.is_file():
                targets.add(module_path.resolve())
        else:
            log.warning("Could not resolve root_module '%s'", root_module)

    if entry_point:
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
        debounce_seconds: float = 0.5,
    ):
        self._paths = [Path(p).resolve() for p in paths]
        self._host = host
        self._port = port
        self._debounce_seconds = debounce_seconds
        self._observer: Optional[Observer] = None  # type: ignore[reportInvalidTypeForm]
        self._server_ready = False
        self._debounce_timer: Optional[threading.Timer] = None
        self._debounce_lock = threading.Lock()
        self._reload_lock = threading.Lock()
        self._pending_changed_paths: set[str] = set()
        self._pending_reload = False
        self._stopped = False

    def start(self):
        self._stopped = False
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

        callback = self._schedule_reload

        class _Handler(FileSystemEventHandler):
            def on_modified(self, event):
                self._on_event(event)

            def on_created(self, event):
                self._on_event(event)

            def on_deleted(self, event):
                self._on_event(event)

            def on_moved(self, event):
                self._on_event(event)

            def _on_event(self, event):
                if event.is_directory:
                    return
                src_path = getattr(event, "dest_path", None) or event.src_path
                if not src_path.endswith(".py"):
                    return
                callback(src_path)

        handler = _Handler()

        self._observer = Observer()
        for path in resolved:
            if path.is_file():
                self._observer.schedule(
                    handler, str(path.parent), recursive=False
                )
            else:
                self._observer.schedule(handler, str(path), recursive=True)

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
        self._stopped = True
        with self._debounce_lock:
            if self._debounce_timer and self._debounce_timer.is_alive():
                self._debounce_timer.cancel()
            self._debounce_timer = None
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None

    def _schedule_reload(self, src_path: str | None = None):
        with self._debounce_lock:
            if self._stopped:
                return
            if src_path:
                self._pending_changed_paths.add(src_path)
            if self._debounce_timer and self._debounce_timer.is_alive():
                self._debounce_timer.cancel()
            self._debounce_timer = threading.Timer(
                self._debounce_seconds,
                self._on_debounce_complete,
            )
            self._debounce_timer.daemon = True
            self._debounce_timer.start()

    def _on_debounce_complete(self):
        with self._debounce_lock:
            if self._stopped:
                return
            changed_paths = self._pending_changed_paths
            self._pending_changed_paths = set()
            self._debounce_timer = None

        if not self._reload_lock.acquire(blocking=False):
            with self._debounce_lock:
                self._pending_changed_paths.update(changed_paths)
                self._pending_reload = True
            return

        should_reschedule = False
        try:
            self._on_files_changed(changed_paths)
        finally:
            self._reload_lock.release()
            with self._debounce_lock:
                should_reschedule = self._pending_reload and not self._stopped
                self._pending_reload = False

        if should_reschedule:
            self._schedule_reload()

    def _on_files_changed(self, changed_paths: set[str]):
        if not changed_paths:
            log.debug("Hot-reloader: skipping reload with no changed paths")
            return
        if not self._server_ready:
            self._server_ready = wait_for_server(self._host, self._port)
            if not self._server_ready:
                return
        change_count = len(changed_paths)
        log.info(
            "Hot-reloader: %d Python file change%s detected, reloading workspace...",
            change_count,
            "" if change_count == 1 else "s",
        )
        if changed_paths:
            log.debug("Hot-reloader changed paths: %s", sorted(changed_paths))
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
