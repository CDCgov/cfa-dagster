from typing import Any

from dagster import (
    InputContext,
    OutputContext,
    ResourceDependency,
    # io_manager,
)
from dagster._utils.cached_method import cached_method
from pydantic import Field

import dagster_azure.adls2 as dagster_azure_adls2
from dagster_azure.adls2 import (
    PickledObjectADLS2IOManager,
    ADLS2DefaultAzureCredential,
)
from dagster_azure.adls2.resources import ADLS2Resource
import os

is_production = not os.getenv("DAGSTER_IS_DEV_CLI")  # set by dagster cli


class ADLS2PickleIOManager(dagster_azure_adls2.ADLS2PickleIOManager):
    __doc__ = dagster_azure_adls2.ADLS2PickleIOManager.__doc__

    _storage_account = "cfadagster" if is_production else "cfadagsterdev"
    _user = os.getenv("DAGSTER_USER")

    adls2: ResourceDependency[ADLS2Resource]
    adls2_file_system: str = Field(
        description="ADLS Gen2 file system name.",
        default=_storage_account,
    )
    adls2_prefix: str = Field(
        default=f"dagster-files/{_user}", description="ADLS Gen2 file system prefix to write to."
    )
    lease_duration: int = Field(
        default=-1,
        description="Lease duration in seconds. Must be between 15 and 60 seconds or -1 for infinite.",
    )

    @property
    @cached_method
    def _internal_io_manager(self) -> PickledObjectADLS2IOManager:
        if not self.adls2:
            self.adls2 = ADLS2Resource(
                storage_account=self._storage_account,
                credential=ADLS2DefaultAzureCredential(kwargs={}),
            )

        return PickledObjectADLS2IOManager(
            self.adls2_file_system,
            self.adls2.adls2_client,
            self.adls2.blob_client,
            self.adls2.lease_client_constructor,
            self.adls2_prefix,
            self.lease_duration,
        )

    def load_input(self, context: "InputContext") -> Any:
        return self._internal_io_manager.load_input(context)

    def handle_output(self, context: "OutputContext", obj: Any) -> None:
        self._internal_io_manager.handle_output(context, obj)


