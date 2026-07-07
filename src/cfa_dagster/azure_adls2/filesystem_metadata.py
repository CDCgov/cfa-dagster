from dataclasses import asdict, dataclass
from typing import Literal, Optional

import dagster as dg

InputMode = Literal["path", "download", "reference"]
OnInputConflict = Literal["overwrite", "fail", "warn", "skip"]

METADATA_KEY = "adls2_fs_io_manager"


@dataclass
class ADLS2FilesystemIOManagerMetadata:
    """
    Per-asset metadata contract for FilesystemADLS2IOManager.

    Set via input/output metadata to override IOManager behavior for a specific asset:

        @asset(
            metadata={
                "adls2_io_manager": {
                    "skip_output": True,
                }
            }
        )

        @asset(
            ins={
                "upstream": AssetIn(
                    metadata={
                        "adls2_io_manager": {
                            "input_mode": "reference",
                            "synthetic_partition_keys": ["nodeA", "nodeB"],
                        }
                    }
                )
            }
        )
    """

    skip_input: bool = False
    skip_output: bool = False
    input_mode: Optional[InputMode] = (
        None  # overrides IOManager-level input_mode if set
    )
    on_input_conflict: Optional[OnInputConflict] = (
        None  # overrides IOManager-level on_input_conflict if set
    )
    synthetic_partition_keys: list[str] = ([],)
    asset_partition_keys: Optional[list] = None
    asset_key_path: Optional[list[str]] = None

    @classmethod
    def from_metadata(
        cls, metadata: dict
    ) -> Optional["ADLS2FilesystemIOManagerMetadata"]:
        raw = metadata.get(METADATA_KEY)
        if raw is None:
            return None
        if isinstance(raw, dg.MetadataValue):
            raw = raw.value
        if not isinstance(raw, dict):
            raise TypeError(
                f"Expected 'adls2_io_manager' metadata to be a dict, got {type(raw)}"
            )
        return cls(
            skip_input=raw.get("skip_input", False),
            skip_output=raw.get("skip_output", False),
            input_mode=raw.get("input_mode", None),
            on_input_conflict=raw.get("on_input_conflict", None),
            asset_key_path=raw.get("asset_key_path", None),
            asset_partition_keys=raw.get("asset_partition_keys", None),
            synthetic_partition_keys=raw.get("synthetic_partition_keys", []),
        )

    def to_dict(self) -> dict:
        return {METADATA_KEY: asdict(self)}
