# Going to Production

For cfa-dagster, going to production means pushing your Dagster workflow as an isolated code location to the central Dagster server hosted in the Azure EXT environment. You can access the server from your VAP at <https://dagster.apps.edav.ext.cdc.gov>.

Since each user's local machine can act as a Dagster server, referred to as a development server, the primary reasons for going to production are:

- scheduling e.g. run daily at 9am
- event-trigger e.g. run when nssp gold data is available
- workflows too important to live on someone's personal computer

## Testing requirements on development server

Follow these steps if you are testing your workflow on your local machine (VAP):

- If using an `in_process_executor` or `multiprocess_executor`, save your file.
- If using a `docker_executor`, save your file and build image.
- If using an `azure_batch_executor` or `azure_container_app_jobs_executor`, save file, build image and push image.

## Testing requirements on production server

If using an `azure_batch_executor` or `azure_container_app_jobs_executor`,

1. Save your file
2. Build the image
3. Push image
4. Update code location (See [Using a script](#using-a-script) and [Using the central Dagster server](#using-the-central-dagster-server))

## Using a script

To create or update your production workflow manually or from a GitHub actions workflow, simply run `uv run https://raw.githubusercontent.com/CDCgov/cfa-dagster/refs/heads/main/scripts/update_code_location.py --registry_image <your_registry_image>`.

Your registry image can be `cfaprdbatchcr.azurecr.io/{image}:{tag}` or `ghcr.io/cdcgov/{image}:{tag}`. Images from cdcent cannot be used due to privacy and credential restrictions.

## Using the central Dagster server

You can also create and update your workflows from the Dagster UI on the central server with the update code location [job](https://dagster.apps.edav.ext.cdc.gov/locations/cfa_dagster/jobs/update_code_location/playground) by providing your registry image and clicking Launch Run.
<image src="update_code_location_job.png" alt="Dagster UI update code location job" width="75%" height="75%">

## Resource constraints

When you go to production, your workflow is turned into a Dagster code location on an Azure Container App. Since the code locations have to be available 24/7, they have 0.5 CPU and 1 GB of RAM to keep costs down.

While all your assets and ops run on infrastructure determined by your executor (e.g. your machine, Docker, Azure Batch, Azure CAJ), the follow Dagster features are handled by the code location:

- [@schedules](https://docs.dagster.io/guides/automate/schedules/defining-schedules)
- [@sensors](https://docs.dagster.io/guides/automate/sensors)
- [custom automation conditions](https://docs.dagster.io/guides/automate/declarative-automation/customizing-automation-conditions/arbitrary-python-automation-conditions)

Given the model infrastructure available to each code location, you'll want to keep the code that executes in these features as quick and lightweight as possible. Executing high-memory or long-running, blocking network requests can either crash the code location or cause it to appear as unavailable to the central Dagster server.
