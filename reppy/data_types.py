import csv
import dataclasses
from os import PathLike
from typing import Union, List, Optional, Any, TextIO

PathLike = Union[str, PathLike]


@dataclasses.dataclass(repr=True)
class MergeState:
    rows: int = 0
    prefix: int = 0
    is_opened: bool = False


