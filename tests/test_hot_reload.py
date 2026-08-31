from cfa_dagster import hot_reload
from cfa_dagster.hot_reload import HotReloader, resolve_target_paths


class FakeTimer:
    instances = []

    def __init__(self, interval, function):
        self.interval = interval
        self.function = function
        self.daemon = False
        self.cancelled = False
        self.started = False
        self._alive = False
        self.__class__.instances.append(self)

    def start(self):
        self.started = True
        self._alive = True

    def cancel(self):
        self.cancelled = True
        self._alive = False

    def is_alive(self):
        return self._alive

    def fire(self):
        self._alive = False
        self.function()


def test_resolve_target_paths_watches_src_root_module(tmp_path):
    project_dir = tmp_path / "project"
    package_dir = project_dir / "src" / "cfa_epinow2_pipeline"
    package_dir.mkdir(parents=True)
    (project_dir / "pyproject.toml").write_text(
        """
[tool.dg]
directory_type = "project"

[tool.dg.project]
root_module = "cfa_epinow2_pipeline"
defs_module = "cfa_epinow2_pipeline.dg_defs"
code_location_target_module = "cfa_epinow2_pipeline.dg_defs"
"""
    )
    init_file = package_dir / "__init__.py"
    defs_file = package_dir / "dg_defs.py"
    asset_file = package_dir / "assets.py"
    init_file.write_text("")
    defs_file.write_text("defs = None\n")
    asset_file.write_text("assets = []\n")

    paths = resolve_target_paths(
        entry_point=defs_file,
        pyproject_path=project_dir / "pyproject.toml",
    )

    assert paths == sorted(
        [init_file.resolve(), asset_file.resolve(), defs_file.resolve()]
    )


def test_resolve_target_paths_watches_flat_root_module(tmp_path):
    project_dir = tmp_path / "project"
    package_dir = project_dir / "my_project"
    package_dir.mkdir(parents=True)
    (project_dir / "pyproject.toml").write_text(
        """
[tool.dg]
directory_type = "project"

[tool.dg.project]
root_module = "my_project"
"""
    )
    init_file = package_dir / "__init__.py"
    defs_file = package_dir / "definitions.py"
    init_file.write_text("")
    defs_file.write_text("defs = None\n")

    paths = resolve_target_paths(
        entry_point=defs_file,
        pyproject_path=project_dir / "pyproject.toml",
    )

    assert paths == sorted([init_file.resolve(), defs_file.resolve()])


def test_resolve_target_paths_falls_back_to_entry_point(tmp_path):
    defs_file = tmp_path / "dagster_defs.py"
    defs_file.write_text("defs = None\n")

    paths = resolve_target_paths(entry_point=defs_file)

    assert paths == [defs_file.resolve()]


def test_hot_reloader_debounces_multiple_file_changes(monkeypatch):
    FakeTimer.instances.clear()
    reloads = []

    monkeypatch.setattr(hot_reload.threading, "Timer", FakeTimer)
    monkeypatch.setattr(hot_reload, "wait_for_server", lambda host, port: True)
    monkeypatch.setattr(
        hot_reload,
        "reload_via_graphql",
        lambda host, port: reloads.append((host, port)) or True,
    )

    reloader = HotReloader([], "localhost", 3000, debounce_seconds=1.25)
    reloader._schedule_reload("a.py")
    reloader._schedule_reload("b.py")

    assert len(FakeTimer.instances) == 2
    assert FakeTimer.instances[0].cancelled is True
    assert FakeTimer.instances[1].interval == 1.25

    FakeTimer.instances[1].fire()

    assert reloads == [("localhost", 3000)]


def test_hot_reloader_schedules_followup_reload_for_changes_during_reload(
    monkeypatch,
):
    FakeTimer.instances.clear()
    reloads = []
    reloader = HotReloader([], "localhost", 3000, debounce_seconds=0.5)

    def fake_reload(host, port):
        reloads.append((host, port))
        if len(reloads) == 1:
            reloader._schedule_reload("b.py")
            FakeTimer.instances[-1].fire()
        return True

    monkeypatch.setattr(hot_reload.threading, "Timer", FakeTimer)
    monkeypatch.setattr(hot_reload, "wait_for_server", lambda host, port: True)
    monkeypatch.setattr(hot_reload, "reload_via_graphql", fake_reload)

    reloader._schedule_reload("a.py")
    FakeTimer.instances[-1].fire()

    assert reloads == [("localhost", 3000)]
    assert len(FakeTimer.instances) == 3

    FakeTimer.instances[-1].fire()

    assert reloads == [("localhost", 3000), ("localhost", 3000)]
