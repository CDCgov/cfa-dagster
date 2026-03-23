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
    use_production: bool = Field(
        description="Whether to use the production storage account for IO",
        default=is_production(),
    )
    adls2: ResourceDependency[ADLS2Resource]

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
        return super().load_input(context)

    def handle_output(self, context: "OutputContext", obj: Any) -> None:
        super().handle_output(context, obj)
