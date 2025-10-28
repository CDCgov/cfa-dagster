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
#    "PyJWT",
#    "cryptography",
#    "requests",
# ]
# ///
import os
import subprocess
import sys
from pathlib import Path
import yaml
import dagster as dg
from urllib.parse import urlparse
import jwt
import time
import requests

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


def create_jwt() -> str:
    """Create a GitHub App JWT using the PEM private key."""
    APP_ID = "924513"  # ID for GH App installed on both cdcgov and cdcent
    PEM_PATH = "/opt/dagster/dagster_home/gh_app.pem"
    with open(PEM_PATH, "r") as f:
        private_key = f.read()

    now = int(time.time())
    payload = {
        "iat": now,
        "exp": now + 9 * 60,  # max 10 minutes
        "iss": APP_ID,
    }

    return jwt.encode(payload, private_key, algorithm="RS256")


def get_installation_token(jwt: str, gh_org: str) -> str:
    """Exchange a JWT for an installation access token."""
    # Installation IDs for the GH App per org
    org_install_ids = {
        "cdcgov": "55092555",
        "cdcent": "51970934",
    }
    install_id = org_install_ids.get(gh_org.lower())

    url = f"https://api.github.com/app/installations/{install_id}/access_tokens"
    headers = {
        "Authorization": f"Bearer {jwt}",
        "Accept": "application/vnd.github+json",
    }
    response = requests.post(url, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()["token"]


CODE_LOCATION_DIR = "/opt/dagster/code_location"
TMP_VENV_DIR = "/tmp/venv"
WORKSPACE_YAML_PATH = "/opt/dagster/dagster_home/workspace.yaml"
GRAPHQL_URL = "http://dagster.apps.edav.ext.cdc.gov/graphql"


@dg.op(out={"repo_name": dg.Out(), "script_path": dg.Out()})
def clone_repo(context: dg.OpExecutionContext, github_url: str) -> tuple[str, Path]:
    context.log.debug(f"github_url: '{github_url}'")
    parsed = urlparse(github_url)
    path_parts = parsed.path.strip("/").split("/")
    context.log.debut(f"parsed: '{parsed}'")
    context.log.debut(f"path_parts: '{path_parts}'")

    # Validate structure
    if len(path_parts) < 5 or path_parts[2] != "blob":
        raise ValueError("Invalid GitHub file URL format. "
        "Expected: https://github.com/cdcent/cfa-dagster/blob/main/examples/dagster_defs.py")

    owner = path_parts[0]
    repo = path_parts[1]
    file_path = "/".join(path_parts[4:])
    jwt = create_jwt()
    token = get_installation_token(jwt, owner)

    repo_url = f"https://x-access-token:{token}@github.com/{owner}/{repo}.git"

    # Clone the repo into a temp directory
    subprocess.run(["git", "clone", "--depth", "1", repo_url],
                   cwd=CODE_LOCATION_DIR,
                   check=True)

    # Construct full path to the script
    script_path = Path(f"{CODE_LOCATION_DIR}/{repo}/{file_path}")
    return (repo, script_path)


@dg.op
def get_temp_venv(repo_name: str) -> Path:
    return Path(f"{TMP_VENV_DIR}/{repo_name}/.venv")


@dg.op
def install_deps(context: dg.OpExecutionContext, script_path: Path, venv_path: Path) -> Path:
    # install dependencies to a temp virtual env
    # ASSUMPTIONS:
    # - file contains PEP723 dependency metadata
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = f"{venv_path}"
    subprocess.run(["uv", "sync", "--script", f"{script_path}", "--active"],
                   env=env,
                   check=True)
    context.log.info(f"Installed dependencies to {venv_path}")
    return venv_path


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
            "relative_path": f"{script_path}",
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


@dg.op
def reload_workspace(should_reload):
    if should_reload is not None:
        return

    query = """
    mutation reload_workspace {
      reloadWorkspace {
        __typename
        ... on Workspace {
          id
          locationEntries {
            id
            name
            loadStatus
            locationOrLoadError {
              __typename
              ... on PythonError {
                message
                stack
              }
            }
          }
        }
        ... on PythonError {
          message
          stack
        }
      }
    }
    """
    requests.post(GRAPHQL_URL, json={"query": query}).raise_for_status()


@dg.job()
def add_code_location():
    repo_name, script_path = clone_repo()
    tmp_venv_path = get_temp_venv(repo_name)
    tmp_venv_path = install_deps(script_path, tmp_venv_path)
    venv_path = hard_copy_venv(script_path, tmp_venv_path)
    did_update = update_workspace_yaml(repo_name, script_path, venv_path)
    # restart_dagster_webserver(did_update)
    reload_workspace(did_update)


@dg.op(out={"relative_path": dg.Out(), "executable_path": dg.Out()})
def get_code_location(location_name: str) -> tuple[Path, Path]:
    """Reads from the workspace.yaml and returns executable_path and relative_path."""
    # Open and parse the workspace.yaml file
    with open(WORKSPACE_YAML_PATH, 'r') as file:
        workspace_data = yaml.safe_load(file)

    # Assuming the structure has a 'code_locations' key with a list of locations
    code_locations = workspace_data.get('load_from')

    # Validate if code_locations is non-empty
    if not code_locations:
        raise ValueError(f"No code locations found in {WORKSPACE_YAML_PATH}")

    location = next(
        (loc.get('python_file') for loc in code_locations if loc.get('python_file', {}).get('location_name') == location_name),
        None)

    if not location:
        raise ValueError(f"Location '{location_name}' not found!")


    executable_path = location.get('executable_path')
    if not executable_path:
        raise ValueError(f"Location '{location_name}' does not have an executable_path")

    return (Path(location.get('relative_path')), Path(executable_path.split('/bin/python')[0]))


def get_current_remote_url(repo_path):
    """Get the current 'origin' remote URL of the Git repository."""
    result = subprocess.run(
        ["git", "remote", "get-url", "origin"],
        cwd=repo_path,
        text=True,
        capture_output=True,
        check=True
    )
    return result.stdout.strip()


def update_git_remote_url(repo_path):
    """Update the 'origin' remote URL with the new token."""
    # Step 1: Get the current 'origin' remote URL
    current_url = get_current_remote_url(repo_path)

    parsed = urlparse(current_url)
    path_parts = parsed.path.strip("/").split("/")
    owner = path_parts[0]

    # Step 2: Replace the token in the URL
    # The pattern assumes the current URL has the format:
    # https://x-access-token:<token>@github.com/...
    current_token = current_url.split(':')[-1].split('@')[0]

    jwt = create_jwt()
    new_token = get_installation_token(jwt, owner)

    # Construct the new remote URL with the new token
    new_remote_url = current_url.replace(current_token, new_token)

    # Step 3: Update the 'origin' remote URL with the new one
    subprocess.run(
        ["git", "remote", "set-url", "origin", new_remote_url],
        cwd=repo_path,
        check=True
    )

    print(f"Updated remote URL to: {new_remote_url}")
    return repo_path

@dg.op
def pull_repo(venv_path: Path) -> Path:
    update_git_remote_url(venv_path)
    subprocess.run(["git", "pull"], cwd=venv_path, check=True)
    return venv_path

@dg.op
def update_dependencies(relative_path: Path, venv_path: Path) -> bool:
    env = os.environ.copy()
    env["VIRTUAL_ENV"] = f"{venv_path}"
    subprocess.run(["uv", "sync", "--script", f"{relative_path}", "--active"],
                   cwd=venv_path,
                   env=env,
                   check=True)
    return True

@dg.job
def update_code_location():
    relative_path, venv_path = get_code_location()
    venv_path = pull_repo(venv_path)
    venv_path = install_deps(relative_path, venv_path)
    reload_workspace(venv_path)


@dg.job(config=dg.RunConfig(
    ops={"restart_dagster_webserver": {"inputs": {"should_restart": True}}}
))
def restart_webserver():
    restart_dagster_webserver()


# Add assets, jobs, schedules, and sensors here to have them appear in the
# Dagster UI
defs = dg.Definitions(
    jobs=[add_code_location, update_code_location, restart_webserver],
    # setting Docker as the default executor. comment this out to use
    # the default executor that runs directly on your computer
    executor=dg.in_process_executor,
    # executor=docker_executor_configured,
    # executor=azure_caj_executor_configured,
    # executor=azure_batch_executor_configured,
)
