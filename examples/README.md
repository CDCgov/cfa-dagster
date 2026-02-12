## Overview

This basic example should help you get up and running with local Dagster development and serve as a blueprint for integration with existing repos.

It is highly recommended that you complete the Dagster quickstart [tutorial](https://docs.dagster.io/getting-started/quickstart) (~10 mins) before following these steps to familiarize yourself with basic Dagster concepts and the UI.

## Getting Started

1. Start the Dagster UI by running `uv run dagster_defs.py` and clicking the link in your terminal (usually [http://127.0.0.1:3000/])
2. Build your image by navigating to the [build_image_job](http://127.0.0.1:3000/locations/dagster_defs.py/jobs/build_image_job/playground) and clicking `Launch Run` in the bottom right
3. Materialize an asset!
    - In the Dagster UI, navigate to the Lineage page and click `basic_blob_asset`. ([Here](http://127.0.0.1:3000/asset-groups/basic_blob_asset?open-nodes%5B0%5D=dagster_defs.py&open-nodes%5B1%5D=dagster_defs.py%3Adefault) if you are on the default port 3000)
    - Click `Materialize selected` and watch for your run to start on the Asset sidebar
    - Click `View Logs` on the Asset sidebar to monitor progress (stdout & stderr available!)

## Next Steps

- Try materializing partitioned_r_asset
- Try materializing multiple assets at once
- Try materializing your Asset on Azure Container App Jobs
    1. Push your updated image to ACR by running the `build_image_job` from above with `should_push: true` in the Launchpad
    2. Modify the `dynamic_executor` in the `Definitions` of the dagster_defs.py file to use the `azure_caj_config` instead of the `docker_config`
    3. Reload your Definitions from the Lineage or Deployment page
    4. Materialize your Asset again! (See `Getting Started`)
