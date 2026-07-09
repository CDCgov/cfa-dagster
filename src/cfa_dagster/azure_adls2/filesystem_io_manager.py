import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Union, cast

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient
from dagster import (
    AssetKey,
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ResourceDependency,
)
from dagster._core.definitions.partitions.utils import MultiPartitionKey
from dagster._core.definitions.partitions.utils.multi import (
    MULTIPARTITION_KEY_DELIMITER,
)
from dagster._core.storage.upath_io_manager import UPathIOManager
from dagster._utils.cached_method import cached_method
from dagster_azure.adls2 import ADLS2DefaultAzureCredential, ADLS2Resource
from pydantic import Field
from upath import UPath

from ..utils import is_production
from .filesystem_metadata import (
    ADLS2FilesystemIOManagerMetadata,
    InputMode,
    OnInputConflict,
)
from .filesystem_path import ADLS2Path

log = logging.getLogger(__name__)


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
        on_input_conflict: OnInputConflict = "overwrite",
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
        self._on_input_conflict = on_input_conflict
        self._delete_after_upload = delete_after_upload

        # Verify the file system exists and we have access
        self._file_system_client.get_file_system_properties()

        # Base path for UPathIOManager — we use a plain UPath here since UPathIOManager
        # uses it only for path construction, not for actual I/O (we override dump_to_path
        # and load_from_path to use the Azure SDK directly)
        super().__init__(base_path=UPath(self._prefix))

    @staticmethod
    def _expand_and_combine(
        real_keys: list[str | MultiPartitionKey],
        synthetic_partition_keys: list[str],
    ) -> list[str]:
        """
        Normalize partition keys to slash-separated path segments and append
        graph dimensions.

        MultiPartitionKey already sorts dimensions by name in its __new__,
        so the pipe-separated string is already in the correct order — we
        just replace the delimiter.
        """

        def normalize(key: str) -> str:
            return key.replace(MULTIPARTITION_KEY_DELIMITER, "/")

        expanded = [normalize(k) for k in real_keys]
        dim_suffix = "/".join(synthetic_partition_keys)

        if expanded and dim_suffix:
            return [f"{key}/{dim_suffix}" for key in expanded]
        elif dim_suffix:
            return [dim_suffix]
        else:
            return expanded

    @staticmethod
    def _patch_context(
        context: InputContext | OutputContext,
        meta: ADLS2FilesystemIOManagerMetadata,
    ):
        """
        Monkey-patch context class properties so the parent UPathIOManager
        sees the synthetic keys as if they were real Dagster partition keys.

        Patching at the class level is required because Dagster context
        properties are defined as class-level descriptors.
        """
        if meta.synthetic_partition_keys:
            real_keys = (
                context.asset_partition_keys
                if context.has_asset_partitions
                else meta.asset_partition_keys or []
            )
            synthetic_keys = FilesystemADLS2IOManager._expand_and_combine(
                real_keys=real_keys,
                synthetic_partition_keys=meta.synthetic_partition_keys,
            )
            has_partitions = bool(synthetic_keys)

            context.__class__.asset_partition_keys = property(
                lambda self: synthetic_keys
            )
            context.__class__.has_asset_partitions = property(
                lambda self: has_partitions
            )
            log.debug(
                f"Patched context: synthetic_partition_keys={synthetic_keys}"
            )

        if meta.asset_key_path:
            context.__class__.asset_key = property(
                lambda self: AssetKey(meta.asset_key_path)
            )
            context.__class__.has_asset_key = property(lambda self: True)
            log.debug(f"Patched context: asset_key={meta.asset_key_path}")

    def handle_output(self, context: OutputContext, obj: Any):
        meta = ADLS2FilesystemIOManagerMetadata.from_metadata(
            context.output_metadata or {}
        )

        output_metadata = context.output_metadata
        log.debug(f"output_metadata: '{output_metadata}'")
        meta = ADLS2FilesystemIOManagerMetadata.from_metadata(output_metadata)

        if meta and meta.skip_output:
            log.info("dump_to_path: skip_output=True, skipping upload")
            return

        if meta:
            if meta.skip_output:
                log.debug(
                    f"handle_output found output metadata, patching context: {meta}"
                )

            log.debug(
                f"handle_output found output metadata, patching context: {meta}"
            )
            self._patch_context(context, meta)
        else:
            log.debug("handle_output no metadata found.")

        super().handle_output(context, obj)

    def load_input(self, context: InputContext) -> Union[Any, dict[str, Any]]:
        input_metadata = context.definition_metadata
        log.debug(f"input_metadata: '{input_metadata}'")
        meta = ADLS2FilesystemIOManagerMetadata.from_metadata(input_metadata)

        if meta:
            if meta.skip_input:
                log.debug("load_input: skip_input=True, returning None")
                return

            # TODO: is this needed?
            # if meta.synthetic_partition_keys:
            #     self._patch_context(context, meta)

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
    ) -> Union[ADLS2Path, Path, str]:
        """Download a directory from ADLS2 to the local filesystem.

        If the downstream asset's type annotation is ``str``, returns the ADLS2
        path directly without downloading. Otherwise downloads all files under
        the path and returns a local ``pathlib.Path`` to the directory.
        """
        adls2_prefix = str(path)

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

        # Extract per-asset metadata override for on_input_conflict
        meta = ADLS2FilesystemIOManagerMetadata.from_metadata(
            context.definition_metadata
        )
        effective_conflict = cast(
            OnInputConflict,
            meta.on_input_conflict
            if meta and meta.on_input_conflict
            else self._on_input_conflict,
        )
        log.debug(f"effective_conflict: {effective_conflict}")

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
        if self._input_mode == "reference":
            return ADLS2Path(
                _io_manager=self,
                _path=adls2_prefix,
                local_dir=local_dir,
            )

        # ------------------------------------------------------------
        # MODE 3 (default): download locally
        # ------------------------------------------------------------
        log.debug(f"local_dir: {local_dir}")

        # Check for local conflicts before downloading
        if local_dir.exists() and any(local_dir.iterdir()):
            if effective_conflict == "fail":
                raise FileExistsError(
                    f"Local directory already exists and is non-empty: {local_dir}. "
                    f"Set on_input_conflict='overwrite' to overwrite existing files."
                )
            elif effective_conflict == "warn":
                context.log.warning(
                    f"Local directory already exists and is non-empty: {local_dir}. "
                    f"Downloaded files may overwrite existing content."
                )
            elif effective_conflict == "skip":
                context.log.warning(
                    f"Local directory already exists and is non-empty: {local_dir}. "
                    f"Skipping download and returning existing directory."
                )
                return local_dir

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
    on_input_conflict: OnInputConflict = Field(
        default="overwrite",
        description=(
            "Behavior when the local download target already exists. "
            "'overwrite': Overwrite existing files (default), "
            "'fail': Raise an error if the local directory exists and is non-empty, "
            "'warn': Log a warning and proceed with the download, "
            "'skip': Log a warning and return the existing directory without downloading."
        ),
    )
    delete_after_upload: bool = Field(
        default=False,
        description=(
            "Whether files/directories should be deleted after upload."
            "Default: False"
        ),
    )

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
        log.debug(f"self.on_input_conflict: {self.on_input_conflict}")

        return FilesystemADLS2IOManager(
            file_system=adls2.storage_account,
            adls2_client=adls2.adls2_client,
            input_mode=self.input_mode,
            on_input_conflict=self.on_input_conflict,
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
