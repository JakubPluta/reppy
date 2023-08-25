import functools
from pathlib import Path
from typing import Callable

from reppy.data_types import PathLike


def valid_file_path(func: Callable):
    @functools.wraps(func)
    def wrapper(file_path, *args, **kwargs):
        if not isinstance(file_path, PathLike):
            raise TypeError("path must be a string or PathLike object")
        file_path = Path(file_path)
        if file_path.exists() is False:
            raise FileNotFoundError(f"file {file_path} not exists")
        return func(file_path, *args, **kwargs)

    return wrapper


def mkdir_if_not_exists(func):
    @functools.wraps(func)
    def wrapper(file_path, *args, **kwargs):
        if not isinstance(file_path, PathLike):
            file_path = Path(file_path)
        if file_path.exists() is False:
            file_path.mkdir(parents=True, exist_ok=True)
        return func(file_path, *args, **kwargs)

    return wrapper
