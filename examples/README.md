## Overview

This basic example should help you get up and running with local Dagster development and serve as a blueprint for integration with existing repos.

It is highly recommended that you complete the Dagster quickstart [tutorial](https://docs.dagster.io/getting-started/quickstart) (~10 mins) before following these steps to familiarize yourself with basic Dagster concepts and the UI.

## Basic Terminology
- Materialize: to run an Asset or Job to get its output
- Asset: a python function that returns some output
- Job: a python function that materializes an Asset with specific configuration

## Getting Started


1. If you have never set up Dagster on your VAP before, you will need to set up a `~/.dagster_home/dagster.yaml` file: `uv run https://raw.githubusercontent.com/CDCgov/cfa-dagster/refs/heads/main/scripts/setup.py`
2. Build the initial image for your test asset: `docker build -t basic-r-asset .`
3. Start the Dagster UI by running `uv run dagster_defs.py --dev` and clicking the link in your terminal (usually [http://127.0.0.1:3000/])
4. Materialize an asset!
    - In the Dagster UI, navigate to the Assets page and click `basic_blob_asset`. ([Here](http://127.0.0.1:3000/asset-groups/basic_blob_asset?open-nodes%5B0%5D=dagster_defs.py&open-nodes%5B1%5D=dagster_defs.py%3Adefault) if you are on the default port 3000)
    - Click `Materialize selected` and watch for your run to start on the Asset sidebar
    - Click `View Logs` on the Asset sidebar to monitor progress (stdout & stderr available!)

## Next Steps

- Try materializing partitioned_r_asset
- Try materializing multiple assets at once
- Try materializing your Asset on Azure Container App Jobs
    1. Push your updated image to ACR: `az login --identity && az acr login -n cfaprdbatchcr && docker build -t cfaprdbatchcr.azurecr.io/cfa-dagster-sandbox:$(basename $HOME) . --push`
    2. Modify the dagster_defs.py file to use the `azure_caj_executor` instead of the `docker_executor`
    4. Materialize your Asset again! (See `Getting Started`)

## Future Development

- A Blob IO Manager to pass data between assets in a file format
