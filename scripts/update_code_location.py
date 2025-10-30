# /// script
# requires-python = ">=3.11"
# dependencies = [ "requests" ]
# ///
import json
import requests
import argparse

DESCRIPTION = ('Script to update your Dagster code locations on the central Dagster server')
DAGSTER_BASE_URL = "http://dagster.apps.edav.ext.cdc.gov"
# DAGSTER_BASE_URL = "http://127.0.0.1:3000"
DAGSTER_GRAPHQL_URL = f"{DAGSTER_BASE_URL}/graphql"


def main(location_name: str):
    f""" ${DESCRIPTION} """

    query = """
    mutation runJob($runConfigData: RunConfigData) {
      launchRun(
        executionParams: {
          selector: {
            jobName: "update_code_location",
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
                "get_code_location": {
                    "inputs": {
                        "location_name": location_name
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
        print("Updating code location. Monitor progress here: "
              f"'{DAGSTER_BASE_URL}/runs/{run_id}'")
    else:
        print("Failed to update code location: "
              + json.dumps(launch_run_result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=DESCRIPTION
    )
    parser.add_argument(
        '--location_name',
        required=True,
        type=str,
        help=('Required. The name of your code location. '
              'This is usually your GitHub repo name')
    )
    args = parser.parse_args()

    location_name = args.location_name
    main(location_name)
