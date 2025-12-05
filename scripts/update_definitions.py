# /// script
# requires-python = ">=3.11"
# dependencies = [ "requests" ]
# ///
import argparse
import json

import requests

DESCRIPTION = (
    "Script to update your Dagster code "
    "locations on the central Dagster server"
)
DAGSTER_BASE_URL = "http://dagster.apps.edav.ext.cdc.gov"
# DAGSTER_BASE_URL = "http://127.0.0.1:3000"
DAGSTER_GRAPHQL_URL = f"{DAGSTER_BASE_URL}/graphql"


def main():
    """ Script to update infra/dagster_defs.py on the Dagster instance"""

    query = """
    mutation runJob {
      launchRun(
        executionParams: {
          selector: {
            jobName: "update_definitions",
            repositoryName: "__repository__",
            repositoryLocationName: "cfa_dagster"
          }
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
    response = requests.post(
        DAGSTER_GRAPHQL_URL,
        json={"query": query},
    )
    response.raise_for_status()
    result = response.json()

    if "errors" in result:
        raise Exception(result["errors"])

    launch_run_result = result["data"]["launchRun"]
    if launch_run_result["__typename"] == "LaunchRunSuccess":
        run_id = launch_run_result["run"]["runId"]
        print(
            "Updating definitions. Monitor progress here: "
            f"'{DAGSTER_BASE_URL}/runs/{run_id}'"
        )
    else:
        print(
            "Failed to update definitions: "
            + json.dumps(launch_run_result, indent=2)
        )


if __name__ == "__main__":
    main()
