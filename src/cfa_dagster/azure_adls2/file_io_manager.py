import asyncio
import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, List, Union

import aiofiles
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobLeaseClient, BlobServiceClient
from azure.storage.filedatalake import (
    DataLakeLeaseClient,
    DataLakeServiceClient,
)
from dagster import (
    InputContext,
    OutputContext,
    ResourceDependency,
    io_manager,
)
from dagster import (
    _check as check,
)
from dagster._config.pythonic_config import ConfigurableIOManager
from dagster._core.execution.context.init import InitResourceContext
from dagster._core.storage.upath_io_manager import UPathIOManager

# from dagster._core.storage.dagster_type_storage import File, Directory
from dagster._utils.cached_method import cached_method
from dagster_azure.adls2.resources import ADLS2Resource
from pydantic import Field
from upath import UPath


class FileOrDirectoryADLS2IOManager(UPathIOManager):
    def __init__(
        self,
        file_system: str,
        adls2_client: DataLakeServiceClient,
        blob_client: BlobServiceClient,
        lease_client_constructor: Union[
            type[DataLakeLeaseClient], type[BlobLeaseClient]
        ],
        prefix: str = "dagster",
        lease_duration: int = 60,
    ):
        self.adls2_client = adls2_client
        self.file_system_client = self.adls2_client.get_file_system_client(
            file_system
        )
        self.blob_client = blob_client
        self.blob_container_client = self.blob_client.get_container_client(
            file_system
        )
        self.prefix = prefix
        self.lease_client_constructor = lease_client_constructor
        self.lease_duration = lease_duration
        super().__init__(base_path=UPath(self.prefix))

    def get_op_output_relative_path(
        self, context: Union[InputContext, OutputContext]
    ) -> UPath:
        parts = context.get_identifier()
        run_id = parts[0]
        output_parts = parts[1:]
        return UPath("storage", run_id, "files", *output_parts)

    @contextmanager
    def _acquire_lease(
        self, client: Any, is_rm: bool = False
    ) -> Iterator[str]:
        lease_client = self.lease_client_constructor(client=client)
        lease_client.acquire(lease_duration=self.lease_duration)
        try:
            yield lease_client.id
        finally:
            if not is_rm:
                lease_client.release()

    def path_exists(self, path: UPath) -> bool:
        try:
            self.file_system_client.get_file_client(
                path.as_posix()
            ).get_file_properties()
            return True
        except ResourceNotFoundError:
            return False

    def unlink(self, path: UPath) -> None:
        file_client = self.file_system_client.get_file_client(path.as_posix())
        with self._acquire_lease(file_client, is_rm=True) as lease:
            file_client.delete_file(lease=lease, recursive=True)

    async def _async_upload_file(self, local_path: Path, remote_path: UPath):
        file_client = self.file_system_client.create_file(
            remote_path.as_posix()
        )
        lease_client = self.lease_client_constructor(client=file_client)
        lease_client.acquire(lease_duration=self.lease_duration)
        try:
            with open(local_path, "rb") as f:
                file_client.upload_data(
                    f.read(), lease=lease_client.id, overwrite=True
                )
        finally:
            lease_client.release()
        return {
            "original_path": str(local_path),
            "blob_path": f"{remote_path.as_posix()}",
        }

    def dump_to_path(
        self, context: OutputContext, obj: Any, path: UPath
    ) -> None:
        if isinstance(obj, str):
            obj = Path(obj)

        if not isinstance(obj, (Path, list)):
            raise TypeError(
                "Unsupported type: must be a Path to a file, list of file Paths, or Path to a directory"
            )

        files_to_upload = []

        if isinstance(obj, Path) and obj.is_file():
            rel_path = path / obj.name
            files_to_upload.append((obj, rel_path))

        elif isinstance(obj, list) and all(
            isinstance(p, Path) and p.is_file() for p in obj
        ):
            for file_path in obj:
                rel_path = path / file_path.name
                files_to_upload.append((file_path, rel_path))

        elif isinstance(obj, Path) and obj.is_dir():
            for file_path in obj.rglob("*"):
                if file_path.is_file():
                    rel_path = path / file_path.relative_to(obj)
                    files_to_upload.append((file_path, rel_path))

        else:
            raise TypeError(
                "Unsupported type: must be a Path to a file, list of file Paths, or Path to a directory"
            )

        # Run uploads in parallel
        loop = asyncio.get_event_loop()
        tasks = [
            self._async_upload_file(local_path, remote_path)
            for local_path, remote_path in files_to_upload
        ]
        manifest = loop.run_until_complete(asyncio.gather(*tasks))
        manifest_json = json.dumps(manifest, indent=2)
        context.log.debug(f"manifest: '{manifest_json}'\n")

        manifest_blob_path = path / "manifest.json"
        file_client = self.file_system_client.create_file(
            manifest_blob_path.as_posix()
        )
        with self._acquire_lease(file_client) as lease:
            file_client.upload_data(
                manifest_json.encode("utf-8"), lease=lease, overwrite=True
            )

    async def _async_download_file(self, blob_path: str, local_path: Path):
        file_client = self.file_system_client.get_file_client(blob_path)
        stream = file_client.download_file()
        local_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiofiles.open(local_path, "wb") as f:
            for chunk in stream.chunks():
                await f.write(chunk)

    def load_from_path(self, context: InputContext, path: UPath) -> Path:
        context.log.debug(f"path: '{path}'\n")
        context.log.debug(f"path.name: '{path.name}'\n")
        local_dir = Path(path.name)
        local_dir.mkdir(parents=True, exist_ok=True)

        # Download manifest
        file_client = self.file_system_client.get_file_client(
            (path / "manifest.json").as_posix()
        )
        stream = file_client.download_file()
        manifest = json.loads(stream.readall())

        # Download files in parallel
        loop = asyncio.get_event_loop()
        tasks = [
            self._async_download_file(
                blob_path=item["blob_path"],
                local_path=local_dir / Path(item["original_path"]).name,
            )
            for item in manifest
        ]
        loop.run_until_complete(asyncio.gather(*tasks))

        return local_dir

    def WORKING_dump_to_path(
        self, context: OutputContext, obj: Any, path: UPath
    ) -> None:
        if self.path_exists(path):
            context.log.warning(f"Removing existing ADLS2 key: {path}")
            self.unlink(path)

        if isinstance(obj, str):
            obj = Path(obj)

        if not isinstance(obj, Path) or not obj.is_dir():
            raise TypeError("Expected a Path to a directory")

        original_dir_name = obj.name
        temp_dir = tempfile.mkdtemp()
        zip_path = Path(temp_dir) / f"{original_dir_name}.zip"

        # Zip the directory
        shutil.make_archive(zip_path.with_suffix(""), "zip", root_dir=obj)

        # Upload the zip file with metadata
        file_client = self.file_system_client.create_file(path.as_posix())
        with self._acquire_lease(file_client) as lease:
            with open(zip_path, "rb") as f:
                file_client.upload_data(f.read(), lease=lease, overwrite=True)
            file_client.set_metadata(
                {"original_dir_name": original_dir_name}, lease=lease
            )

        # Clean up temp zip
        zip_path.unlink()
        Path(temp_dir).rmdir()

    # WORKING!!
    # TODO: update to write to local dir instead of temp dir
    def WORKING_load_from_path(
        self, context: InputContext, path: UPath
    ) -> Path:
        if context.dagster_type.typing_type == type(None):
            return None

        file_client = self.file_system_client.get_file_client(path.as_posix())
        metadata = file_client.get_file_properties().metadata
        original_dir_name = metadata.get("original_dir_name", "unzipped")

        # Download the zip file
        temp_dir = Path(tempfile.mkdtemp())
        zip_path = temp_dir / f"{original_dir_name}.zip"
        stream = file_client.download_file()
        with open(zip_path, "wb") as f:
            for chunk in stream.chunks():
                f.write(chunk)

        # Unzip to target directory
        target_dir = temp_dir / original_dir_name
        context.log.debug(f"Downloaded to '{target_dir}'\n")
        shutil.unpack_archive(str(zip_path), extract_dir=str(target_dir))

        # Clean up zip file
        zip_path.unlink()

        return target_dir

    def ANNOTATED_dump_to_path(
        self, context: OutputContext, obj: Any, path: UPath
    ) -> None:
        if self.path_exists(path):
            context.log.warning(f"Removing existing ADLS2 key: {path}")
            self.unlink(path)

        # create a Path from a string
        if isinstance(obj, str):
            obj = Path(obj)
        # if the path is to a directory
        # zip the directory
        # upload the zipped file to azure with metadata containing the original directory name
        # delete the zipped directory
        zipped_dir = "TBD"

        file = self.file_system_client.create_file(path.as_posix())
        with self._acquire_lease(file) as lease:
            file.upload_data(zipped_dir, lease=lease, overwrite=True)

    def ANNOTATED_load_from_path(
        self, context: InputContext, path: UPath
    ) -> Any:
        if context.dagster_type.typing_type == type(None):
            return None
        file = self.file_system_client.get_file_client(path.as_posix())
        stream = file.download_file()
        # get the original directory name from the blob metadata
        # download the zipped directory to the local directory
        # unzip the directory and name it the original directory name
        # delete the zipped directory
        local_path = "TBD"
        return local_path

    def OLD_dump_to_path(
        self, context: OutputContext, obj: Any, path: UPath
    ) -> None:
        if isinstance(obj, str):
            obj = Path(obj)
        if isinstance(obj, Path) and obj.is_file():
            self._upload_file(obj, path)

        elif isinstance(obj, list) and all(
            isinstance(p, Path) and p.is_file() for p in obj
        ):
            for file_path in obj:
                rel_path = path / file_path.name
                self._upload_file(file_path, rel_path)

        elif isinstance(obj, Path) and obj.is_dir():
            for file_path in obj.rglob("*"):
                if file_path.is_file():
                    rel_path = path / file_path.relative_to(obj)
                    self._upload_file(file_path, rel_path)

        else:
            raise TypeError(
                "Unsupported type: must be a Path to a file, list of file Paths, or Path to a directory"
            )
        # update this code to use async to upload files in parallel to a configured path in blob
        # create a list with this format [{"original_path": "relative/path/to/file.extension", "blob_path": "azure_uri/to/file"}]
        # and write that list to a file that will be uploaded to the original path: UPath argument

    def _upload_file(self, local_path: Path, remote_path: UPath) -> None:
        file_client = self.file_system_client.create_file(
            remote_path.as_posix()
        )
        with self._acquire_lease(file_client) as lease:
            with open(local_path, "rb") as f:
                file_client.upload_data(f.read(), lease=lease, overwrite=True)

    def OLD_load_from_path(self, context: InputContext, path: UPath) -> Path:
        local_dir = Path(f"/tmp/{path.name}")
        local_dir.mkdir(parents=True, exist_ok=True)

        paths = self.file_system_client.get_paths(
            path=path.as_posix(), recursive=True
        )
        for p in paths:
            if not p.is_directory:
                remote_file_path = p.name
                local_file_path = local_dir / Path(
                    remote_file_path
                ).relative_to(path.as_posix())
                local_file_path.parent.mkdir(parents=True, exist_ok=True)
                file_client = self.file_system_client.get_file_client(
                    remote_file_path
                )
                stream = file_client.download_file()
                with open(local_file_path, "wb") as f:
                    for chunk in stream.chunks():
                        f.write(chunk)

        return local_dir
        # update this code to download the file from the original path: UPath and read it to memory
        # it should a list with this format [{"original_path": "relative/path/to/file.extension", "blob_path": "azure_uri/to/file"}]
        # use async code to download files in parallel to a configured path on the local filesystem


class ADLS2FileIOManager(ConfigurableIOManager):
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
        from dagster_azure.adls2 import ADLS2PickleIOManager, ADLS2Resource, ADLS2SASToken

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
                "io_manager": ADLS2PickleIOManager(
                    adls2_file_system="my-cool-fs",
                    adls2_prefix="my-cool-prefix",
                    adls2=ADLS2Resource(
                        storage_account="my-storage-account",
                        credential=ADLS2SASToken(token="my-sas-token"),
                    ),
                ),
            },
        )


    2. Attach this IO manager to your job to make it available to your ops.

    .. code-block:: python

        from dagster import job
        from dagster_azure.adls2 import ADLS2PickleIOManager, ADLS2Resource, ADLS2SASToken

        @job(
            resource_defs={
                "io_manager": ADLS2PickleIOManager(
                    adls2_file_system="my-cool-fs",
                    adls2_prefix="my-cool-prefix",
                    adls2=ADLS2Resource(
                        storage_account="my-storage-account",
                        credential=ADLS2SASToken(token="my-sas-token"),
                    ),
                ),
            },
        )
        def my_job():
            ...
    """

    adls2: ResourceDependency[ADLS2Resource]
    adls2_file_system: str = Field(description="ADLS Gen2 file system name.")
    adls2_prefix: str = Field(
        default="dagster",
        description="ADLS Gen2 file system prefix to write to.",
    )
    lease_duration: int = Field(
        default=60,
        description="Lease duration in seconds. Must be between 15 and 60 seconds or -1 for infinite.",
    )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @property
    @cached_method
    def _internal_io_manager(self) -> FileOrDirectoryADLS2IOManager:
        return FileOrDirectoryADLS2IOManager(
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


# @dagster_maintained_io_manager
@io_manager(
    config_schema=ADLS2FileIOManager.to_config_schema(),
    required_resource_keys={"adls2"},
)
def adls2_file_io_manager(
    init_context: InitResourceContext,
) -> FileOrDirectoryADLS2IOManager:
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

    Attach this IO manager to a set of assets.

    .. code-block:: python

        from dagster import Definitions, asset
        from dagster_azure.adls2 import adls2_pickle_io_manager, adls2_resource

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
                "io_manager": adls2_pickle_io_manager.configured(
                    {"adls2_file_system": "my-cool-fs", "adls2_prefix": "my-cool-prefix"}
                ),
                "adls2": adls2_resource,
            },
        )


    Attach this IO manager to your job to make it available to your ops.

    .. code-block:: python

        from dagster import job
        from dagster_azure.adls2 import adls2_pickle_io_manager, adls2_resource

        @job(
            resource_defs={
                "io_manager": adls2_pickle_io_manager.configured(
                    {"adls2_file_system": "my-cool-fs", "adls2_prefix": "my-cool-prefix"}
                ),
                "adls2": adls2_resource,
            },
        )
        def my_job():
            ...
    """
    adls_resource = init_context.resources.adls2
    adls2_client = adls_resource.adls2_client
    blob_client = adls_resource.blob_client
    lease_client = adls_resource.lease_client_constructor
    return FileOrDirectoryADLS2IOManager(
        init_context.resource_config["adls2_file_system"],
        adls2_client,
        blob_client,
        lease_client,
        init_context.resource_config.get("adls2_prefix"),
        init_context.resource_config.get("lease_duration"),
    )
