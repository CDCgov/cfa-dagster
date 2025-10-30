# /// script
# requires-python = ">=3.11"
# dependencies = [ "requests" ]
# ///
import json
import requests
import argparse
from urllib.parse import urlparse

DESCRIPTION = ('Script to register your Dagster workflows with the central '
               'Dagster instance for scheduling and event-based triggering')
DAGSTER_BASE_URL = "http://dagster.apps.edav.ext.cdc.gov"
# DAGSTER_BASE_URL = "http://127.0.0.1:3000"
DAGSTER_GRAPHQL_URL = f"{DAGSTER_BASE_URL}/graphql"
EXAMPLE_GITHUB_URL = 'https://github.com/cdcent/cfa-dagster/blob/main/examples/dagster_defs.py'


def main(github_url: str):
    f""" ${DESCRIPTION} """

    # validate URL
    parsed = urlparse(github_url)
    path_parts = parsed.path.strip("/").split("/")

    # basic url validation
    if len(path_parts) < 5 or path_parts[2] != "blob":
        raise ValueError(
            "Invalid GitHub file URL format. "
            f"Expected: {EXAMPLE_GITHUB_URL}"
        )
    if not github_url.endswith(".py"):
        raise ValueError(
            "Must link to python file. "
            f"Expected: {EXAMPLE_GITHUB_URL}"
        )

    query = """
    mutation runJob($runConfigData: RunConfigData) {
      launchRun(
        executionParams: {
          selector: {
            jobName: "add_code_location",
            repositoryName: "__repository__",
            repositoryLocationName: "cfa-dagster"
          },
          runConfigData: $runConfigData
        }
      ) {
        __typename
        ... on LaunchRunSuccess {
          run {
            runId
            status
          }
        }
        ... on RunConfigValidationInvalid {
          pipelineName
          errors {
            __typename
            message
          }
        }
      }
    }
    """
    variables = {
        "runConfigData": {
            "ops": {
                "clone_repo": {
                    "inputs": {
                        "github_url": github_url
                    }
                }
            }
        }
    }
    response = requests.post(
        DAGSTER_GRAPHQL_URL,
        json={
            "query": query,
            "variables": variables
        },
    )
    response.raise_for_status()
    result = response.json()

    if "errors" in result:
        raise Exception(result["errors"])

    launch_run_result = result["data"]["launchRun"]
    if launch_run_result["__typename"] == "LaunchRunSuccess":
        run_id = launch_run_result["run"]["runId"]
        print("Adding code location. Monitor progress here: "
              f"'{DAGSTER_BASE_URL}/runs/{run_id}'")
    else:
        print("Failed to add code location: " 
              + json.dumps(launch_run_result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=DESCRIPTION
    )
    parser.add_argument(
        '--github_url',
        required=True,
        type=str,
        help=('Required. Github URL pointing to your Dagster definitions e.g. ' +
              EXAMPLE_GITHUB_URL +
              ' NOTE: you must provide the default branch e.g. main, master, prod'
              )
    )
    args = parser.parse_args()

    github_url = args.github_url
    main(github_url)
