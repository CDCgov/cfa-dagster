#!/usr/bin/env -S uv run --script
# PEP 723 dependency definition: https://peps.python.org/pep-0723/
# /// script
# requires-python = ">=3.13,<3.14"
# dependencies = [
#    "cfa-dagster[dev] @ git+https://github.com/cdcgov/cfa-dagster.git",
# ]
# ///

import dagster as dg

from cfa_dagster import (
    collect_definitions,
    GraphDimension,
    dynamic_graph_asset,
    start_dev_env,
)

# function to start the dev server
start_dev_env(__name__)


class MyAssetConfig(dg.ConfigurableResource):
    disease: GraphDimension[str] = GraphDimension(["covid", "flu", "rsv"])


@dynamic_graph_asset
def my_asset(
    context: dg.OpExecutionContext, my_asset_config: MyAssetConfig
):
    disease = my_asset_config.disease.current_value
    context.log.info(f"Watch out for: '{disease}'")


collected_defs = collect_definitions(globals())

# Create Definitions object
defs = dg.Definitions(
        **collected_defs,
        resources={
            "my_asset_config": MyAssetConfig()
            }
        )
