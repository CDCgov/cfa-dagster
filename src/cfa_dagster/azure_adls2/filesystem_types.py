from typing import Literal

InputMode = Literal["path", "download", "reference"]
OnInputConflict = Literal["overwrite", "fail", "warn", "skip", "merge"]
