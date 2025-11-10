# /// script
# requires-python = ">=3.11"
# dependencies = [ "requests" ]
# ///
import json
import requests
import argparse

DESCRIPTION = ('Script to update your Dagster code '
               'locations on the central Dagster server')
DAGSTER_BASE_URL = "http://dagster.apps.edav.ext.cdc.gov"
# DAGSTER_BASE_URL = "http://127.0.0.1:3000"
DAGSTER_GRAPHQL_URL = f"{DAGSTER_BASE_URL}/graphql"


def main(registry_image: str):
    f""" ${DESCRIPTION} """
    if registry_image.startswith('ghcr.io/cdcent'):
        raise ValueError("Cannot use images from private ghcr.io/cdcent repos")

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
                "get_code_location_name": {
                    "inputs": {
                        "registry_image": registry_image
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
        '--registry_image',
        required=True,
        type=str,
        help=('Required. The registry image for your code location. '
              'e.g. cfaprdbatchcr.azurecr.io/cfa-dagster:latest, '
              'ghcr.io/cdcgov/cfa-dagster:latest. '
              'Images from ghcr.io/cdcent cannot be used!'
              )
    )
    args = parser.parse_args()

    main(args.registry_image)
