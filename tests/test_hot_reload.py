from cfa_dagster.hot_reload import resolve_target_paths


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
