# Concepts

The purpose of this page is to provide a high-level overview of the tools commonly used in CFA Dagster projects. This is not a comprehensive list of all Dagster capabilities. For additional information on Dagster concepts, click on [this link](https://docs.dagster.io/getting-started/concepts).

Dagster is an **asset-centric workflow orchestration tool** versus a task-centric workflow tool. An asset-centric workflow focuses on outputs and has upstream assets that are used as inputs to create their downstream [dependencies](https://docs.dagster.io/dagster-basics-tutorial/dependencies). 

A task-centric tool focuses on steps taken in a workflow. For example, if we were baking cookies, a task-centric workflow would include steps like gathering ingredients, combining ingredients, adding chocolate chips, baking in the oven, and eating the cookies. 

On the other hand, with the cookie example, an asset-centric workflow approach would look like the following: to create cookie dough, combine the wet ingredients with the dry ingredients; to create chocolate chip cookie dough, mix in the chocolate chips into the cookie dough; bake the chocolate chip cookie dough to eat freshly baked chocolate chip cookies. (Cookie example from [Dagster University Dagster Essentials course](https://courses.dagster.io/courses/dagster-essentials))

## Assets

A Dagster [asset](https://docs.dagster.io/dagster-basics-tutorial/assets) is a piece of data that your data pipeline creates, updates, or manages. Instead of focusing on what code runs, Dagster focuses on what data exists and how it depends on other data. Dagster treats your data platform as a graph of datasets (assets) and their relationships, then orchestrates the work needed to keep those datasets current.

Once an asset is created, Dagster does not automatically run the code for the asset, it must be [materialized](https://docs.dagster.io/guides/build/assets/configuring-assets) first. When an asset is materialized, Dagster runs the asset’s function and creates the asset. When a materialization begins, it kicks off a run.

```python

@dg.asset()
def meaning_of_life():
  the_meaning = calculate_meaning_of_life()
  return the_meaning  # 42
```

## Asset Jobs

[Asset jobs](https://docs.dagster.io/guides/build/jobs/asset-jobs) execute and monitor specified assets. In the example of baking cookies, a job could be made to add the chocolate chips to the cookie dough once you have successfully made the cookie dough.

## Definitions

Each workflow (containing assets, resources or schedules) must be part of a [Definitions](https://docs.dagster.io/api/dagster/definitions#definitions) object. For CFA specifically, the definitions object is located within the [dagster_defs.py](https://github.com/CDCgov/cfa-dagster/blob/main/examples/dagster_defs.py) file. The `dagster_defs.py` file contains the Dagster-specific configurations, assets, jobs, and/or schedules in addition to the Definitions object.

`cfa-dagster` exports a helper function to collect all the Dagster definitions in a file e.g.:

```python
from cfa_dagster import collect_definitions

collected_defs = collect_definitions(globals())

# Create Dagster definitions
defs = dg.Definitions(
    **collected_defs, # assets, asset checks, jobs, schedules, sensors
    ... # other configuration like resources, executor, loggers, metadata, etc.
)
```

## Dynamic Graph Assets

[Dynamic graph assets](../api.md#cfa_dagster.dynamic_graph_asset.dynamic_graph_asset) combine two existing Dagster concepts, [graph assets](https://docs.dagster.io/guides/build/assets/graph-backed-assets#defining-graph-backed-assets) and [dynamic outputs](https://docs.dagster.io/guides/build/ops/dynamic-graphs#a-dynamic-job), into one decorator to provide easy, runtime-configurable parallelism. Unlike normal Dagster partitions, dynamic graph assets allows you to parallelize logic against more than two dimensions.

```python

class ParallelAssetConfig(dg.ConfigurableResource):
    ingredient: GraphDimension[str] = GraphDimension(["sugar", "milk", "flour"])


@dynamic_graph_asset(
    description="A parallel asset that runs R code for different diseases",
)
def parallel_asset(
    context: dg.OpExecutionContext,
    parallel_asset_config: ParallelAssetConfig,
):
    ingredient = parallel_asset_config.ingredient.current_value
    context.log.info(f"Running for ingredient: {ingredient}")

defs = dg.Definitions(
    **collected_defs,
    resources={
        "parallel_asset_config": ParallelAssetConfig(),
    },
)
```

## Executors

[Executors](https://docs.dagster.io/guides/operate/run-executors) manage how each step or asset in a job is executed. In the cookie example, this would be the head baker deciding who should be performing what tasks and making sure those tasks get done in the proper order. The specific head baker in the bakery that day could be the head baker who likes to have multiple bakers to assemble the cookie dough at the same time or the head baker who wants one baker to make the dough.

Some of the most common executors used in CFA are:

- `in_process_executor` or `multiprocess_executor` for running workflow through Dagster locally.
- `docker_executor` for running workflow using Docker containers.
- [`azure_batch_executor`](../api.md#cfa_dagster.azure_batch_executor) or [`azure_container_app_job_executor`](../api.md#cfa_dagster.azure_container_app_job_executor) for running workflow through Azure.

You can create an executor via [`SelectorConfig`](../api.md#cfa_dagster.SelectorConfig):

```python
docker_execution_config = ExecutionConfig(
    executor=SelectorConfig(
        class_name=docker_executor.__name__,
        config={
            "image": image,
            "container_kwargs": {
                "volumes": [
                    # bind the ~/.azure folder for optional cli login
                    f"/home/{user}/.azure:/root/.azure",
                    # bind current file so we don't have to rebuild
                    # the container image for workflow changes
                    f"{__file__}:{workdir}/{os.path.basename(__file__)}",
                ]
            },
        },
    )
)
```

And configure it for all your assets via `Definitions`:

```python
defs = dg.Definitions(
    **collected_defs,
    executor=dynamic_executor(
        # specify the default executor
        default_config=docker_execution_config,
        # alternate configs show you default values in the Launchpad on hover
        alternate_configs=[
            default_execution_config,
            docker_execution_config,
            azure_caj_execution_config,
            azure_batch_execution_config,
        ],
    ),
)

```

Directly on a job:

```python
docker_job = dg.define_asset_job(
    name="docker_job",
    selection=[some_asset],
    config=docker_execution_config.to_run_config(),
)

```

Or via tags on an `asset`, `RunRequest`, `@schedule`, or `@sensor`:

```python
@dg.asset(op_tags=docker_execution_config.to_run_tags())
def always_runs_on_docker():
    print("Hello from docker")

@dg.schedule(target=[some_asset])
    return dg.RunRequest(tags=docker_execution_config.to_run_tags())

```

## Ops

[Ops](https://docs.dagster.io/guides/build/ops) are single units of work in the pipeline. In the cookie example, an op could be getting the mixing bowl out, getting the flour from the pantry, or picking up the whisk before mixing the ingredients together. Dagster treats each op as a managed step, which allows the user to track whether the step succeeded or failed, record logs and metadata, retry failed steps, and visualize the pipeline in the Dagster UI.

Use an `op` over an asset for tasks that don't produce tracked artifacts e.g.:

```python
@dg.op
def build_image():
  subprocess.run(["docker", "build", "-t", "my_image", "."], check=True)

@dg.job()
def build_image_job():
  build_image()
```

## Partitions

[Partitions](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-ops#non-partitioned-job-with-date-config) divide the workflow into smaller pieces, which could speed up computation by using parallel processing. Users can test on an individual partition before trying to run larger ranges of data. In the cookie example, this could be scaling the recipe back to make 8 cookies instead of making the cookie dough for 80 cookies.

Since partitions only allow you to run a workflow in parallel against two dimensions, you generally want to use a dynamic graph asset for parallelism instead. Time-based partitions are used most frequently to track asset outputs as they differ on a daily basis.

```python
# create a daily partition
tz = "America/New_York"
daily_partitions_def = dg.DailyPartitionsDefinition(
    start_date=dt.datetime.now(ZoneInfo(tz)) - dt.timedelta(days=1),
    end_offset=1,
    timezone=tz,
)

@dg.asset(partitions_def=daily_partitions_def)
def daily_reports(context: dg.AssetExecutionContext):
  # daily partitions are represented as YYYY-mm-dd
  report_date = context.partition_key
  context.log.info(f"Generating a report for date: {report_date}")

```

## Resources

A [resource](https://docs.dagster.io/dagster-basics-tutorial/resources#step-4-view-the-resource) in Dagster is something your assets need to do their work, but not the data you're producing. In the example of baking cookies, resources would be a mixing bowl, spoon, a baking sheet, and the oven.

Resources often represent:

- Database connections
- Data warehouse clients
- API clients
- Cloud storage clients
- Credentials and secrets
- Logging systems
- I/O managers
- Configuration dictionaries

```python
from dagster_azure.blob import (
    AzureBlobStorageDefaultCredential,
    AzureBlobStorageResource,
)
from cfa_dagster import ADLS2PickleIOManager

class ParallelAssetConfig(dg.ConfigurableResource):
    ingredient: GraphDimension[str] = GraphDimension(["sugar", "milk", "flour"])

# Create Dagster definitions
defs = dg.Definitions(
    **collected_defs,
    resources={
        # This IOManager lets Dagster serialize asset outputs and store them
        # in Azure to pass between assets
        "io_manager": ADLS2PickleIOManager(),
        # an example storage account
        "azure_blob_storage": AzureBlobStorageResource(
            account_url=f"{storage_account}.blob.core.windows.net",
            credential=AzureBlobStorageDefaultCredential(),
        ),
        "parallel_asset_config": ParallelAssetConfig(),
    },

```

## Run Launchers

A [run launcher](https://docs.dagster.io/deployment/execution/run-launchers) allocates the necessary computational resources to carry out a run execution and then starts the execution. In the cookie example, this would be like clearing off the counters, getting all of your necessary components (mixing bowl, whisk, ingredients, etc.) out on the counter before you start making the cookie dough. Then, once you have everything set up, you begin making cookies. In `cfa-dagster`, the run launcher used is typically the [`DynamicRunLauncher`](../api.md#cfa_dagster.DynamicRunLauncher), which instantiates a concrete launcher at runtime (`DefaultRunLauncher`, `DockerRunLauncher`, or [`AzureContainerAppJobLauncher`](../api.md#cfa_dagster.AzureContainerAppJobRunLauncher)) based on configuration found on the run, run tags, or repository metadata, then delegates launch/resume/health/terminate operations to that concrete launcher.

In most cases, you will not need to configure a run launcher - you will inherit the default. The `DynamicRunLauncher` will automatically choose the `DefaultRunLauncher` when running locally and the `AzureContainerAppJobRunLauncher` when running in production.

## Schedules

[Schedules](https://docs.dagster.io/guides/automate/schedules) define a fixed time interval to run your pipeline. In the example of the cookies, this could be planning to bake the cookies at 1 pm.

<!-- prettier-ignore-start -->
!!! warning
    When going to production, be mindful that sensors run in a constrained computing environment. See [Going to Production](./production.md#resource-constraints) for more details
<!-- prettier-ignore-end -->

## Sensors

[Sensors](https://docs.dagster.io/guides/automate/sensors) check for events at regular time intervals, and if triggered, will start a job or other action. Sensors allow the workflow to run automatically without someone needing to manually start the pipeline. In the baking cookies example, this would be like having an assistant take the cookies out of the oven when the timer goes off.

You might use a sensor to track external data sources e.g.:

```python
@dg.sensor(
    job=my_job,
    minimum_interval_seconds=5,
    default_status=dg.DefaultSensorStatus.RUNNING,  # Sensor is turned on by default
)
def new_file_sensor():
    new_files = check_for_new_files()
    # New files, run `my_job`
    if new_files:
        for filename in new_files:
            yield dg.RunRequest()
    # No new files, skip the run and log the reason
    else:
        yield dg.SkipReason("No new files found")
```

<!-- prettier-ignore-start -->
!!! warning
    When going to production, be mindful that sensors run in a constrained computing environment. See [Going to Production](./production.md#resource-constraints) for more details
<!-- prettier-ignore-end -->
