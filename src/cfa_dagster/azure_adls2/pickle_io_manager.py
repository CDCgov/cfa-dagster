import os
from typing import Any

from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ResourceDependency,
)
from dagster._utils.cached_method import cached_method
from dagster_azure.adls2 import (
    ADLS2DefaultAzureCredential,
    PickledObjectADLS2IOManager,
)
from dagster_azure.adls2.resources import ADLS2Resource
from pydantic import Field

from ..utils import is_production


class ADLS2PickleIOManager(ConfigurableIOManager):
    """Persistent IO manager using Azure Data Lake Storage Gen2 for storage.

    Serializes objects via pickling. Suitable for objects storage for distributed executors, so long
    as each execution node has network connectivity and credentials for ADLS and the backing
    container.

    Assigns each op output to a unique filepath containing run ID, step key, and output name.
    Assigns each asset to a single filesystem path, at "<base_dir>/<asset_key>". If the asset key
    has multiple components, the final component is used as the name of the file, and the preceding
    components as parent directories under the base_dir.

    Subsequent materializations of an asset will overwrite previous materializations of that asset.
    With a base directory of "/my/base/path", an asset with key
    `AssetKey(["one", "two", "three"])` would be stored in a file called "three" in a directory
    with path "/my/base/path/one/two/".

    Example usage:

    1. Attach this IO manager to a set of assets.

    .. code-block:: python

        from dagster import Definitions, asset
        from cfa_dagster import ADLS2PickleIOManager

        @asset
        def asset1():
            # create df ...
            return df

        @asset
        def asset2(asset1):
            return df[:5]

        Definitions(
            assets=[asset1, asset2],
            resources={
                "io_manager": ADLS2PickleIOManager(),
            },
        )


    2. Attach this IO manager to your job to make it available to your ops.

    .. code-block:: python

        from dagster import job
        from cfa_dagster import ADLS2PickleIOManager

        @job(
            resource_defs={
                "io_manager": ADLS2PickleIOManager(),
            },
        )
        def my_job():
            ...
    """

    use_production: bool = Field(
        description="Whether to use the production storage account for IO",
        default=is_production(),
    )
    adls2: ResourceDependency[ADLS2Resource]
    overrides: dict[str, Any] = Field(
        description=(
            "Override an upstream dependency input with a configured value e.g."
            "`upstream_asset: 'static_value_for_testing'`"
        ),
        default_factory=dict,
    )

    @property
    @cached_method
    def _internal_io_manager(self) -> PickledObjectADLS2IOManager:
        adls2 = self.adls2 or ADLS2Resource(
            storage_account="cfadagster"
            if self.use_production
            else "cfadagsterdev",
            credential=ADLS2DefaultAzureCredential(kwargs={}),
        )
        user = "prod" if self.use_production else os.getenv("DAGSTER_USER")

        return PickledObjectADLS2IOManager(
            file_system=adls2.storage_account,
            adls2_client=adls2.adls2_client,
            blob_client=adls2.blob_client,
            lease_client_constructor=adls2.lease_client_constructor,
            prefix=f"dagster-files/{user}",
            lease_duration=-1,
        )

    def load_input(self, context: "InputContext") -> Any:
        upstream_key = context.upstream_output.asset_key.to_user_string()
        context.log.debug(f"upstream_key: {upstream_key}")

        if upstream_key in self.overrides:
            override = self.overrides[upstream_key]
            context.log.debug(
                f"found key: {upstream_key} returning override: {override}"
            )
            return override
        return self._internal_io_manager.load_input(context)

    def handle_output(self, context: "OutputContext", obj: Any) -> None:
        self._internal_io_manager.handle_output(context, obj)
