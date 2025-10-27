#!/usr/bin/env -S uv run --script
# PEP 723 dependency definition: https://peps.python.org/pep-0723/
# /// script
# requires-python = ">=3.13"
# dependencies = [
#    "dagster-azure>=0.27.4",
#    "dagster-docker>=0.27.4",
#    "dagster-postgres>=0.27.4",
#    "dagster-webserver",
#    "dagster==1.11.4",
#    "cfa-dagster @ git+https://github.com/cdcgov/cfa-dagster.git",
#    "pyyaml>=6.0.2",
#    "azure-identity>=1.23.0",
#    "azure-mgmt-appcontainers==3.2.0",
#    "azure-mgmt-resource>=24.0.0",
# ]
# ///
import os
import subprocess
import sys
from pathlib import Path
import yaml
import dagster as dg
from urllib.parse import urlparse

from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.resource.subscriptions import SubscriptionClient

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


# get the user from the environment, throw an error if variable is not set
user = os.environ["DAGSTER_USER"]

CODE_LOCATION_DIR = "/opt/dagster/code_location"
TMP_VENV_DIR = "/tmp/venv/"


@dg.op(out={"repo_name": dg.Out(), "script_path": dg.Out()})
def clone_repo(context: dg.OpExecutionContext, github_url: str) -> tuple[str, Path]:
    # Parse the URL
    parsed = urlparse(github_url)
    path_parts = parsed.path.strip("/").split("/")

    # Validate structure
    if len(path_parts) < 5 or path_parts[2] != "blob":
        raise ValueError("Invalid GitHub file URL format. "
        "Expected: https://github.com/cdcent/cfa-dagster/blob/main/examples/dagster_defs.py")

    owner = path_parts[0]
    repo = path_parts[1]
    file_path = "/".join(path_parts[4:])
    repo_url = f"https://github.com/{owner}/{repo}.git"

    # Clone the repo into a temp directory
    subprocess.run(["git", "clone", "--depth", "1", repo_url],
                   cwd=CODE_LOCATION_DIR,
                   check=True)

    # Construct full path to the script
    script_path = os.path.join(CODE_LOCATION_DIR, file_path)
    return (repo, script_path)


@dg.op(out={"repo_name": dg.Out(), "script_path": dg.Out()})
def OLD_clone_repo(context: dg.OpExecutionContext, url: str, repo_name: str) -> tuple[str, Path]:
    subprocess.run(["git", "clone", url],
                   cwd=CODE_LOCATION_DIR,
                   check=True)
    script_path = Path(f"{CODE_LOCATION_DIR}/{repo_name}")
    context.log.info(f"Cloned {repo_name} to {script_path}")
    return (repo_name, script_path)


@dg.op
def install_deps(context: dg.OpExecutionContext, repo_name: str, script_path: Path) -> Path:
    venv_dir = f"{TMP_VENV_DIR}/{repo_name}/.venv"
    # install dependencies to a temp virtual env
    # ASSUMPTIONS:
    # - file contains PEP723 dependency metadata
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = venv_dir
    subprocess.run(["uv", "sync", "--script", script_path, "--active"],
                   env=env,
                   check=True)
    context.log.info(f"Installed dependencies to {venv_dir}")
    return Path(venv_dir)


@dg.op
def hard_copy_venv(context: dg.OpExecutionContext, script_path: Path, tmp_venv_dir: Path) -> Path:
    local_venv_path = f"{script_path.parent}/.venv"
    subprocess.run(["cp", "-rL", tmp_venv_dir, local_venv_path],
                   check=True)
    # remove temp_venv
    subprocess.run(["rm", "-rf", tmp_venv_dir], check=True)
    context.log.info(f"Copied dependencies to {local_venv_path}")
    return Path(local_venv_path)


@dg.op
def update_workspace_yaml(context: dg.OpExecutionContext,
                          repo_name: str,
                          script_path: Path,
                          venv_path: Path) -> bool:
    """
    Adds a new python_file entry to the load_from list in a Dagster workspace YAML file.

    Args:
        yaml_path (str): Path to the YAML file.
        path (str): Relative path to the new code location.
        executable_path (str): Path to the Python executable.
        location_name (str): Name of the code location.
    """
    WORKSPACE_YAML_PATH = "/opt/dagster/dagster_home/workspace.yaml"
    # Read the existing YAML content
    with open(WORKSPACE_YAML_PATH, 'r') as f:
        data = yaml.safe_load(f)

    # Ensure load_from exists and is a list
    if "load_from" not in data or not isinstance(data["load_from"], list):
        data["load_from"] = []

    # Check if location_name already exists
    for entry in data["load_from"]:
        python_file = entry.get("python_file", {})
        if python_file.get("location_name") == repo_name:
            context.log.info(f"Code location '{repo_name}' already exists in {WORKSPACE_YAML_PATH}")
            return False

    # Append the new code location
    data["load_from"].append({
        "python_file": {
            "relative_path": script_path,
            "executable_path": f"{venv_path}/bin/python",
            "location_name": repo_name
        }
    })

    # Write the updated YAML back to the file
    with open(WORKSPACE_YAML_PATH, 'w') as f:
        yaml.dump(data, f, default_flow_style=False)

    context.log.info(f"Added code location '{repo_name}' to {WORKSPACE_YAML_PATH}")
    return True


@dg.op
def restart_dagster_webserver(context: dg.OpExecutionContext, should_restart: bool):
    if not should_restart:
        return

    RESOURCE_GROUP = "ext-edav-cfa-prd"
    CONTAINER_APP = "dagster"
    credential = DefaultAzureCredential()

    # Get first subscription for logged-in credential
    first_subscription_id = (
        SubscriptionClient(credential)
        .subscriptions
        .list()
        .next()
        .subscription_id
    )

    client = ContainerAppsAPIClient(
        credential=credential, subscription_id=first_subscription_id
    )

    # Find active revision
    revisions = list(
        client.container_apps_revisions
        .list_revisions(RESOURCE_GROUP, CONTAINER_APP)
    )
    active_revision = next((r for r in revisions if r.active), None)

    if not active_revision:
        raise RuntimeError("No active revision found!")

    rev_name = active_revision.name
    client.container_apps_revisions.restart_revision(
        resource_group_name=RESOURCE_GROUP,
        container_app_name=CONTAINER_APP,
        revision_name=rev_name
    )

    context.log.info(f"Restarting container app: {CONTAINER_APP}")


@dg.job()
def add_code_location():
    repo_name, script_path = clone_repo()
    tmp_venv_path = install_deps(repo_name, script_path)
    venv_path = hard_copy_venv(script_path, tmp_venv_path)
    did_update = update_workspace_yaml(repo_name, script_path, venv_path)
    restart_dagster_webserver(did_update)


@dg.job(config=dg.RunConfig(
    ops={"restart_dagster_webserver": {"inputs": {"should_restart": True}}}
))
def restart_webserver():
    restart_dagster_webserver()


# Add assets, jobs, schedules, and sensors here to have them appear in the
# Dagster UI
defs = dg.Definitions(
    jobs=[add_code_location, restart_webserver],
    # setting Docker as the default executor. comment this out to use
    # the default executor that runs directly on your computer
    executor=dg.in_process_executor,
    # executor=docker_executor_configured,
    # executor=azure_caj_executor_configured,
    # executor=azure_batch_executor_configured,
)
