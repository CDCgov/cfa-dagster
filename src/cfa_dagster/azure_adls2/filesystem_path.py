import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from azure.storage.filedatalake import (
    DataLakeDirectoryClient,
    DataLakeFileClient,
    FileSystemClient,
)

from .filesystem_metadata import DownloadResult

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .filesystem_io_manager import FilesystemADLS2IOManager
    from .filesystem_metadata import OnInputConflict


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
        on_conflict: "OnInputConflict" = "overwrite",
    ) -> DownloadResult:
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
            on_conflict=on_conflict,
        )

    def __str__(self) -> str:
        return self.uri

    def __repr__(self) -> str:
        return f"ADLS2Path(uri='{self.uri}')"
