#!/usr/bin/env -S uv run --script
# PEP 723 dependency definition: https://peps.python.org/pep-0723/
# /// script
# requires-python = ">=3.13,<3.14"
# dependencies = [
#    "dagster-azure>=0.27.4",
#    "dagster-postgres>=0.27.4",
#    "dagster-webserver>=1.12.2",
#    "dagster>=1.12.2",
#    "cfa-dagster @ git+https://github.com/cdcgov/cfa-dagster.git",
# ]
# ///

import os

# from time import sleep
from typing import List

import dagster as dg

# ruff: noqa: F401
from cfa_dagster import (
    ADLS2PickleIOManager,
    DynamicGraphAssetExecutionContext,
    collect_definitions,
    dynamic_graph_asset,
    start_dev_env,
)

# function to start the dev server
start_dev_env(__name__)

user = os.getenv("DAGSTER_USER")


class MyAssetConfig(dg.Config):
    disease: List[str] = ["covid", "flu", "rsv"]


@dynamic_graph_asset(
    graph_dimensions=["disease"],
)
def my_asset(
    context: DynamicGraphAssetExecutionContext, config: MyAssetConfig
):
    disease = context.graph_dimension["disease"]
    context.log.info(f"Watch out for: '{disease}'")


collected_defs = collect_definitions(globals())

# Create Definitions object
defs = dg.Definitions(
    **collected_defs,
    resources={
        # This IOManager lets Dagster serialize asset outputs and store them
        # in Azure to pass between assets
        "io_manager": ADLS2PickleIOManager(),
    },
)
