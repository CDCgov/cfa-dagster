import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Union

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import (
    DataLakeDirectoryClient,
    DataLakeFileClient,
    DataLakeServiceClient,
    FileSystemClient,
)
from dagster import (
    AssetKey,
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ResourceDependency,
)
from dagster._core.storage.upath_io_manager import UPathIOManager
from dagster._utils.cached_method import cached_method
from dagster_azure.adls2 import ADLS2DefaultAzureCredential, ADLS2Resource
from pydantic import Field
from upath import UPath

from ..dynamic_graph_asset import DynamicGraphAssetMetadata
from ..utils import is_production

log = logging.getLogger(__name__)
azure_http_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")

InputMode = Literal["path", "download", "reference"]

# TODO: replace DynamicGraphAssetMetadata-based behavior with explicit feature metadata native to this IOManager
@dataclass(frozen=True)
class ADLS2Path:
    """
    Represents a path in ADLS2.

    This object provides authenticated access to the underlying Azure SDK
    clients while also exposing convenience methods for common operations.

    Users can always drop down to the Azure SDK if they need functionality
    beyond the convenience methods.

    Examples
    --------

    Download entire asset:

        local_dir = data.download()

    Download only a subfolder:

        parquet_dir = data.download("parquet")

    Access Azure SDK directly:

        directory_client = data.get_directory_client()

        for item in directory_client.get_paths():
            ...
    """

    _io_manager: "FilesystemADLS2IOManager"
    _path: str
    local_dir: Path

    @property
    def uri(self) -> str:
        return self._io_manager._uri_for_prefix(self._path)

    @property
    def file_system_client(self) -> FileSystemClient:
        return self._io_manager._file_system_client

    def get_directory_client(self) -> DataLakeDirectoryClient:
        return self.file_system_client.get_directory_client(self._path)

    def get_file_client(self) -> DataLakeFileClient:
        return self.file_system_client.get_file_client(self._path)

    def list(self, recursive: bool = False):
        """
        List paths beneath this ADLS location.

        Returns Azure SDK PathProperties objects.
        """
        return self.file_system_client.get_paths(
            path=self._path,
            recursive=recursive,
        )

    def download(
        self,
        relative_path: str | None = None,
        local_dir: Path | None = None,
    ) -> Path:
        """
        Download this path or a subpath.

        Examples
        --------

        Download entire asset:

            data.download()

        Download parquet subdirectory:

            data.download("parquet")

        Download nested path:

            data.download("parquet/2026/01")
        """

        target_path = self._path

        if relative_path:
            target_path = (Path(target_path) / relative_path).as_posix()
            if not local_dir:
                local_dir = self.local_dir / relative_path

        return self._io_manager.download_prefix(
            adls2_prefix=target_path,
            local_dir=local_dir,
        )

    def __str__(self) -> str:
        return self.uri

    def __repr__(self) -> str:
        return f"ADLS2Path(uri='{self.uri}')"


class FilesystemADLS2IOManager(UPathIOManager):
    """An IOManager that stores directories and files on ADLS2.

    Assets should return a local ``pathlib.Path`` pointing to either a file or a directory.
    The IOManager will upload the file or directory to ADLS2 and make it available to
    downstream assets.

    Downstream assets receive a local ``pathlib.Path`` pointing to the downloaded file or
    directory. If the downstream asset's type annotation is ``str``, the ADLS2 path is
    returned directly without downloading (e.g. ``abfss://container@account.dfs.core.windows.net/...``).

    Partitioned assets are fully supported. When loading multiple partitions, the downstream
    asset receives a ``Dict[str, Path]`` mapping partition keys to local paths, consistent
    with Dagster's built-in IOManager behavior.

    Args:
        file_system (str): The ADLS2 file system (container) name.
        adls2_client (DataLakeServiceClient): An authenticated ADLS2 service client.
        prefix (str): The path prefix within the file system. Defaults to ``"dagster"``.
        max_concurrency (int): Number of parallel chunks per file transfer. Higher values
            improve throughput for large files at the cost of memory. Defaults to ``4``,
            which gives a good balance for files in the hundreds of MB to GB range.

    Example:

        .. code-block:: python

            from azure.storage.filedatalake import DataLakeServiceClient
            from dagster import Definitions, asset
            from pathlib import Path

            @asset(io_manager_key="adls2_dir")
            def my_asset() -> Path:
                out = Path("/tmp/my_output")
                out.mkdir(exist_ok=True)
                (out / "results.csv").write_text("a,b,c")
                return out

            @asset(io_manager_key="adls2_dir")
            def downstream_asset(my_asset: Path) -> None:
                df = pd.read_csv(my_asset / "results.csv")

            @asset(io_manager_key="adls2_dir")
            def path_only_asset(my_asset: str) -> None:
                # receives the raw ADLS2 path without downloading
                print(my_asset)  # abfss://container@account.dfs.core.windows.net/dagster/my_asset

            adls2_client = DataLakeServiceClient(
                account_url="https://<account>.dfs.core.windows.net",
                credential="<credential>",
            )

            defs = Definitions(
                assets=[my_asset, downstream_asset, path_only_asset],
                resources={
                    "adls2_dir": ADLS2FilesystemIOManager(
                        file_system="my-container",
                        adls2_client=adls2_client,
                    )
                },
            )
    """

    def __init__(
        self,
        file_system: str,
        adls2_client: DataLakeServiceClient,
        input_mode: InputMode,
        prefix: str = "dagster",
        max_concurrency: int = 4,
        delete_after_upload: bool = False,
    ):
        self._file_system = file_system
        self._adls2_client = adls2_client
        self._file_system_client = adls2_client.get_file_system_client(
            file_system
        )
        self._prefix = prefix
        self._max_concurrency = max_concurrency
        self._input_mode = input_mode
        self._delete_after_upload = delete_after_upload

        # Verify the file system exists and we have access
        self._file_system_client.get_file_system_properties()

        # Base path for UPathIOManager — we use a plain UPath here since UPathIOManager
        # uses it only for path construction, not for actual I/O (we override dump_to_path
        # and load_from_path to use the Azure SDK directly)
        super().__init__(base_path=UPath(self._prefix))

    def handle_output(self, context: OutputContext, obj: Any):
        output_metadata = context.output_metadata
        log.debug(f"output_metadata: '{output_metadata}'")
        dga_metadata = DynamicGraphAssetMetadata.from_metadata(output_metadata)
        if dga_metadata:
            log.debug(
                "@dynamic_graph_asset, modifying context to mimic an asset"
            )
            has_asset_partitions = not not dga_metadata.asset_partition_keys
            context.__class__.asset_partition_keys = property(
                lambda self: dga_metadata.asset_partition_keys
            )
            context.__class__.has_asset_partitions = property(
                lambda self: has_asset_partitions
            )
            context.__class__.asset_key = property(
                lambda self: AssetKey(dga_metadata.asset_key)
            )
            context.__class__.has_asset_key = property(
                lambda self: not not dga_metadata.asset_key
            )
            log.debug(
                f"context.has_asset_partitions: '{context.has_asset_partitions}'"
            )
        super().handle_output(context, obj)

    def _get_paths_for_partitions(
        self, context: Union[InputContext, OutputContext]
    ) -> dict[str, "UPath"]:
        paths = super()._get_paths_for_partitions(context)
        # 2026-04-07 14:40:55,994 - cfa_dagster.azure_adls2.filesystem_io_manager - DEBUG - _get_paths_for_partitions: '{'2026-04-06': PosixUPath('dagster-files/gio/cfa_county_rt/2026-04-06')}'
        log.debug(f"_get_paths_for_partitions: '{paths}'")
        if isinstance(context, InputContext):
            io_metadata = context.definition_metadata
            log.debug(f"input_metadata: '{io_metadata}'")
        else:
            io_metadata = context.output_metadata
            log.debug(f"output_metadata: '{io_metadata}'")
        dga_metadata = DynamicGraphAssetMetadata.from_metadata(io_metadata)

        if not dga_metadata or dga_metadata.should_return_parent:
            return paths

        # Build a new dict of paths with graph_dimensions appended
        new_paths = {}
        for partition_key, base_path in paths.items():
            # Append the graph dimensions to the UPath
            final_path = base_path
            for dim in dga_metadata.graph_dimensions:
                final_path = final_path / dim
            new_paths[partition_key] = final_path
        log.debug(
            f"_get_paths_for_partitions (with graph_dimensions): '{new_paths}'"
        )
        return new_paths

    def _get_path_without_extension(
        self, context: Union[InputContext, OutputContext]
    ) -> "UPath":
        path = super()._get_path_without_extension(context)
        log.debug(f"_get_path_without_extension: '{path}'")

        if isinstance(context, InputContext):
            io_metadata = context.definition_metadata
            log.debug(f"input_metadata: '{io_metadata}'")
        else:
            io_metadata = context.output_metadata
            log.debug(f"output_metadata: '{io_metadata}'")
        dga_metadata = DynamicGraphAssetMetadata.from_metadata(io_metadata)

        if (
            not dga_metadata
            or dga_metadata.asset_partition_keys
            or dga_metadata.should_return_parent
        ):
            return path
        # Append graph_dimensions to the path
        for dim in dga_metadata.graph_dimensions:
            path = path / dim

        log.debug(
            f"_get_path_without_extension (with graph_dimensions): '{path}'"
        )
        return path

    def load_input(self, context: InputContext) -> Union[Any, dict[str, Any]]:
        input_metadata = context.definition_metadata
        log.debug(f"input_metadata: '{input_metadata}'")
        dga_metadata = DynamicGraphAssetMetadata.from_metadata(input_metadata)
        if dga_metadata:
            log.debug("@dynamic_graph_asset, returning dummy abfss://")
            return
        return super().load_input(context)

    # -------------------------------------------------------------------------
    # Path construction
    # -------------------------------------------------------------------------

    def make_directory(self, path: UPath) -> None:
        # ADLS2 does not require explicit directory creation
        return None

    # -------------------------------------------------------------------------
    # Core IOManager methods
    # -------------------------------------------------------------------------

    def dump_to_path(
        self, context: OutputContext, obj: Any, path: UPath
    ) -> None:
        """Upload a local file or directory to ADLS2.

        ``obj`` should be a ``pathlib.Path`` pointing to a local file or directory.
        ``path`` is the ADLS2 destination path as constructed by UPathIOManager.
        """
        output_metadata = context.output_metadata
        log.debug(f"output_metadata: '{output_metadata}'")
        dga_metadata = DynamicGraphAssetMetadata.from_metadata(output_metadata)
        if dga_metadata and dga_metadata.should_return_parent:
            log.info(
                "@dynamic_graph_asset.should_return_parent==True, ignoring dump_to_path"
            )
            return

        if not isinstance(obj, Path):
            raise TypeError(
                f"{self.__class__.__name__} requires assets to return a pathlib.Path, "
                f"got {type(obj)}"
            )
        if not obj.exists():
            raise FileNotFoundError(
                f"Asset returned path does not exist: {obj}"
            )

        # path from UPathIOManager is e.g. UPath("dagster/my_asset") or UPath("dagster/my_asset/2024-01-01")
        # We use it as the ADLS2 directory prefix
        adls2_prefix = str(path)

        # Clean up any existing blobs at this prefix to ensure a clean overwrite
        self._delete_directory(adls2_prefix)

        if obj.is_file():
            self._upload_file(
                local_path=obj, adls2_path=f"{adls2_prefix}/{obj.name}"
            )
            file_count = 1
            byte_count = obj.stat().st_size
        else:
            file_count, byte_count = self._upload_directory(
                local_dir=obj,
                adls2_prefix=adls2_prefix,
            )

        context.log.debug(
            f"Uploaded {file_count} file(s) ({byte_count} bytes) to "
            f"{self._uri_for_prefix(adls2_prefix)}"
        )

        if self._delete_after_upload:
            try:
                if obj.is_file():
                    obj.unlink()
                    context.log.debug(f"Deleted local file: {obj}")
                else:
                    # remove directory tree
                    for p in sorted(obj.rglob("*"), reverse=True):
                        if p.is_file():
                            p.unlink()
                        elif p.is_dir():
                            try:
                                p.rmdir()
                            except OSError:
                                pass
                    obj.rmdir()
                    context.log.debug(f"Deleted local directory: {obj}")

            except Exception as e:
                context.log.warning(
                    f"Failed to delete local path {obj} after upload: {e}"
                )

    def load_from_path(
        self, context: InputContext, path: UPath
    ) -> Union[Path, str]:
        """Download a directory from ADLS2 to the local filesystem.

        If the downstream asset's type annotation is ``str``, returns the ADLS2
        path directly without downloading. Otherwise downloads all files under
        the path and returns a local ``pathlib.Path`` to the directory.
        """
        adls2_prefix = str(path)

        input_metadata = context.definition_metadata
        log.debug(f"input_metadata: '{input_metadata}'")
        dga_metadata = DynamicGraphAssetMetadata.from_metadata(input_metadata)
        # If the downstream asset wants a string, return the ADLS2 URI directly
        # so the asset can use the SDK to selectively download files itself
        ttype = context.dagster_type.typing_type
        log.debug(f"ttype: {ttype}")

        # Use the input name (as declared in `ins`) as the local folder name so that
        # the downloaded directory matches what the asset code expects, e.g.:
        #   unpartitioned: ./raw_data/
        #   partitioned:   ./my_raw_data/2024-01-01/
        # Falls back to the asset key path if no input name is available.
        input_name = (
            context.name if context.name else "__".join(context.asset_key.path)
        )
        prefix_parts = len(Path(self._prefix).parts)
        asset_relative_parts = path.parts[
            prefix_parts:
        ]  # e.g. ('my_asset',) or ('my_asset', '2024-01-01')
        # For unpartitioned assets the relative path is just the asset name, which we
        # replace with the input name. For partitioned assets we keep the partition key.
        if len(asset_relative_parts) > 1:
            local_dir = Path(input_name) / Path(*asset_relative_parts[1:])
        else:
            local_dir = Path(input_name)

        # ------------------------------------------------------------
        # MODE 1: return raw abfss path (no download)
        # ------------------------------------------------------------
        if (
            # DEPRECATED, using the annotated type is not reliable
            # maintaining for backwards compatibility until replaced
            ttype is str or self._input_mode == "path"
        ):
            return self._uri_for_prefix(adls2_prefix)

        # ------------------------------------------------------------
        # MODE 2: return rich ADLS2 handle
        # ------------------------------------------------------------
        if self._input_mode == "reference" or dga_metadata:
            return ADLS2Path(
                _io_manager=self,
                _path=adls2_prefix,
                local_dir=local_dir,
            )

        # ------------------------------------------------------------
        # MODE 3 (default): download locally
        # ------------------------------------------------------------

        local_dir.mkdir(parents=True, exist_ok=True)

        file_count = self._download_directory(
            adls2_prefix=adls2_prefix,
            local_dir=local_dir,
        )

        context.log.debug(
            f"Downloaded {file_count} file(s) from "
            f"{self._uri_for_prefix(adls2_prefix)} to {local_dir}"
        )

        return local_dir

    # -------------------------------------------------------------------------
    # Upload helpers
    # -------------------------------------------------------------------------

    def _upload_file(self, local_path: Path, adls2_path: str) -> int:
        """Upload a single file to ADLS2. Returns the number of bytes uploaded."""
        file_client = self._file_system_client.get_file_client(adls2_path)
        with local_path.open("rb") as f:
            file_client.upload_data(
                f, overwrite=True, max_concurrency=self._max_concurrency
            )
        return local_path.stat().st_size

    def _upload_directory(
        self, local_dir: Path, adls2_prefix: str
    ) -> tuple[int, int]:
        """Recursively upload a local directory to ADLS2.

        Returns:
            Tuple of (file_count, total_bytes)
        """
        file_count = 0
        byte_count = 0

        for local_file in sorted(local_dir.rglob("*")):
            if not local_file.is_file():
                continue

            relative_path = local_file.relative_to(local_dir)
            adls2_path = f"{adls2_prefix}/{relative_path.as_posix()}"

            byte_count += self._upload_file(
                local_path=local_file, adls2_path=adls2_path
            )
            file_count += 1

        return file_count, byte_count

    # -------------------------------------------------------------------------
    # Download helpers
    # -------------------------------------------------------------------------

    def _download_directory(self, adls2_prefix: str, local_dir: Path) -> int:
        """Download all files under an ADLS2 prefix to a local directory.

        Returns:
            Number of files downloaded.
        """
        paths = self._file_system_client.get_paths(
            path=adls2_prefix, recursive=True
        )

        file_count = 0
        for path_item in paths:
            if path_item.is_directory:
                continue

            # Reconstruct the relative path by stripping the prefix
            adls2_path: str = path_item.name
            relative_path = adls2_path[len(adls2_prefix) :].lstrip("/")

            local_file = local_dir / relative_path
            local_file.parent.mkdir(parents=True, exist_ok=True)

            file_client = self._file_system_client.get_file_client(adls2_path)
            with local_file.open("wb") as f:
                download = file_client.download_file(
                    max_concurrency=self._max_concurrency
                )
                download.readinto(f)

            file_count += 1

        return file_count

    # -------------------------------------------------------------------------
    # Deletion helpers
    # -------------------------------------------------------------------------

    def _delete_directory(self, adls2_prefix: str) -> None:
        """Delete all blobs under an ADLS2 prefix. Silently succeeds if the prefix
        does not exist."""
        try:
            directory_client = self._file_system_client.get_directory_client(
                adls2_prefix
            )
            directory_client.delete_directory()
        except ResourceNotFoundError:
            pass

    # -------------------------------------------------------------------------
    # Utility
    # -------------------------------------------------------------------------

    def _uri_for_prefix(self, prefix: str) -> str:
        account_name = self._adls2_client.account_name
        return f"abfss://{self._file_system}@{account_name}.dfs.core.windows.net/{prefix}"

    def path_exists(self, path: UPath) -> bool:
        try:
            self._file_system_client.get_file_client(
                str(path)
            ).get_file_properties()
            return True
        except ResourceNotFoundError:
            return False

    def unlink(self, path: UPath) -> None:
        file_client = self._file_system_client.get_file_client(str(path))
        file_client.delete_file()

    def download_prefix(
        self,
        adls2_prefix: str,
        local_dir: Path | None = None,
    ) -> Path:
        """
        Download an ADLS prefix to a local directory.

        Parameters
        ----------
        adls2_prefix:
            ADLS path relative to the filesystem root.

        local_dir:
            Destination directory. If omitted, a temporary
            directory is created.

        Returns
        -------
        Path
            Local directory containing downloaded files.
        """

        if local_dir is None:
            local_dir = Path(tempfile.mkdtemp())

        local_dir.mkdir(parents=True, exist_ok=True)

        self._download_directory(
            adls2_prefix=adls2_prefix,
            local_dir=local_dir,
        )

        return local_dir


class ADLS2FilesystemIOManager(ConfigurableIOManager):
    """An IOManager that stores directories and files on ADLS2.

    Assets should return a local ``pathlib.Path`` pointing to either a file or a directory.
    The IOManager will upload the file or directory to ADLS2 and make it available to
    downstream assets.

    Downstream assets receive a local ``pathlib.Path`` pointing to the downloaded file or
    directory. If the downstream asset's type annotation is ``str``, the ADLS2 path is
    returned directly without downloading (e.g. ``abfss://container@account.dfs.core.windows.net/...``).

    Partitioned assets are fully supported. When loading multiple partitions, the downstream
    asset receives a ``Dict[str, Path]`` mapping partition keys to local paths, consistent
    with Dagster's built-in IOManager behavior.

    Args:
        use_production (bool): whether to access production storage or user-specific development storage
        max_concurrency (int): Number of parallel chunks per file transfer. Higher values
            improve throughput for large files at the cost of memory. Defaults to ``4``,
            which gives a good balance for files in the hundreds of MB to GB range.

    Example:

        .. code-block:: python

            from dagster import Definitions, asset
            from pathlib import Path

            @asset(io_manager_key="adls2_dir")
            def my_asset() -> Path:
                out = Path("/tmp/my_output")
                out.mkdir(exist_ok=True)
                (out / "results.csv").write_text("a,b,c")
                return out

            @asset(io_manager_key="adls2_dir")
            def downstream_asset(my_asset: Path) -> None:
                df = pd.read_csv(my_asset / "results.csv")

            @asset(io_manager_key="adls2_dir")
            def path_only_asset(my_asset: str) -> None:
                # receives the raw ADLS2 path without downloading
                print(my_asset)  # abfss://container@account.dfs.core.windows.net/dagster/my_asset


            defs = Definitions(
                assets=[my_asset, downstream_asset, path_only_asset],
                resources={
                    "adls2_dir": ADLS2FilesystemIOManager()
                },
            )
    """

    use_production: bool = Field(
        description="Whether to use the production storage account for IO",
        default=is_production(),
    )
    log_azure_http_io: bool = Field(
        description="Whether to display Azure http policy logs in stderr always or only when explciitly setting the log level to DEBUG or higher.",
        default=False
    )
    adls2: ResourceDependency[ADLS2Resource]
    overrides: dict[str, Any] = Field(
        description=(
            "Override an upstream dependency input with a configured value e.g."
            "`upstream_asset: 'static_value_for_testing'`"
        ),
        default_factory=dict,
    )
    max_concurrency: int = Field(
        default=4,
        description=(
            "Number of parallel chunks per file transfer. Higher values"
            "improve throughput for large files at the cost of memory. Defaults to `4`,"
            "which gives a good balance for files in the hundreds of MB to GB range."
        ),
    )
    input_mode: InputMode = Field(
        default="download",
        description=(
            "Mode to determine behavior on loading inputs from upstream. "
            "'download': Download the file(s) from upstream, "
            "'path': return an abfss:// formatted path, "
            "'reference': return an ADLS2Path which includes an authenticated ADLS2 client."
        ),
    )
    delete_after_upload: bool = Field(
        default=False,
        description=(
            "Whether files/directories should be deleted after upload."
            "Default: False"
        ),
    )

    # By default, this will be false, thus we will only log azure http requests if
    # the log level is otherwise set to WARNING.
    if not log_azure_http_io:
        azure_http_logger.setLevel(logging.DEBUG)

    @property
    @cached_method
    def _internal_io_manager(self) -> FilesystemADLS2IOManager:
        adls2 = self.adls2 or ADLS2Resource(
            storage_account="cfadagster"
            if self.use_production
            else "cfadagsterdev",
            credential=ADLS2DefaultAzureCredential(kwargs={}),
        )
        user = "prod" if self.use_production else os.getenv("DAGSTER_USER")

        return FilesystemADLS2IOManager(
            file_system=adls2.storage_account,
            adls2_client=adls2.adls2_client,
            input_mode=self.input_mode,
            delete_after_upload=self.delete_after_upload,
            prefix=f"dagster-files/{user}",
            max_concurrency=self.max_concurrency,
        )

    def load_input(self, context: "InputContext") -> Any:
        if context.upstream_output and context.upstream_output.has_asset_key:
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
