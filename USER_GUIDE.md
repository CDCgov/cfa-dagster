# User Guide for cfa-dagster

## Purpose

The purpose of this document is to explain Dagster jargon and provide guidance for Dagster workflow structure

See the [examples](examples/) first if you haven't done so already

## Basic Terminology

- [Materialize](https://docs.dagster.io/api/dagster/execution#dagster.materialize): to run an Asset or Job to get its output
- [Asset](https://docs.dagster.io/dagster-basics-tutorial/assets): a python function that generates persistent artifacts
- [Op](https://docs.dagster.io/guides/build/ops#inputs): a python function that doesn’t return anything, or only produces temporary results.
- [Job](https://docs.dagster.io/guides/build/jobs#uses): a python function that runs Assets or Ops with some configuration
- [Partition](https://docs.dagster.io/guides/build/partitions-and-backfills/partitioning-assets): a parameter to an Asset that allows parallel compute
- [Run Launcher](https://docs.dagster.io/deployment/execution/run-launchers#custom-run-launchers): the python class that determines the Dagster run environment e.g. on your computer, in Docker, on Azure Batch, or on Azure Container App Job
- [Executor](https://docs.dagster.io/guides/operate/run-executors#for-a-code-location): the python class that determines how steps (Ops, Assets) are executed in a Dagster run e.g. sequentially or in parallel in the Run Launcher environment, parallelized across Azure Batch tasks, or parallelized across Azure Container App Job executions
- [Backfill](https://docs.dagster.io/guides/build/partitions-and-backfills/backfilling-data): Materializing multiple partitions of an Asset at once aka running a python function parallelized against a set of parameters

## Using Dagster in an Existing Repo

Using Dagster in an existing repo is as easy as adding a `dagster_defs.py` file to your repo root and including it and its dependencies in your `Dockerfile`. See the [examples](examples/) for reference files.

### Dockerfile requirements

Your `Dockerfile` must:
- Have `uv` installed
- Copy `dagster_defs.py` in the `WORKDIR`
- Sync Dagster's python dependencies and add them to the `PATH`

If your repo already has a `pyproject.toml` it is recommended to add your Dagster dependencies there instead of directly in the `dagster_defs.py`. That will allow your python package manager (`uv`) to perform dependency resolution and detect any conflicts.
