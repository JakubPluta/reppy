import csv
import dataclasses
from os import PathLike
from typing import Union, Any
from reppy.utils import add_no_prefix_to_file_path


PathLike = Union[str, PathLike]


@dataclasses.dataclass(repr=True)
class OpenedFile:
    output_path: PathLike
    prefix: Any = None

    _opened: bool = False

    def __post_init__(self):
        self.output_path = (
            add_no_prefix_to_file_path(self.output_path, self.prefix)
            if self.prefix
            else self.output_path
        )
        self._csv_file = open(self.output_path, "w", newline="")
        self._writer = csv.writer(self._csv_file)
        self._opened = True

    @property
    def opened(self):
        return self._opened

    @property
    def csv_file(self):
        return self._csv_file

    @property
    def writer(self):
        return self._writer
