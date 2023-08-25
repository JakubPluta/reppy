import functools
import itertools
from os import PathLike
from pathlib import Path
from typing import Any, Iterable, Iterator, List


from reppy.data_types import FilePath
from reppy.log import get_logger

logger = get_logger(__name__)


def valid_file_path(func):
    @functools.wraps(func)
    def wrapper(file_path, *args, **kwargs):
        if not isinstance(file_path, (str, PathLike)):
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


def remove_last_character(file_path: FilePath):
    """open file and remove last character"""
    with open(file_path, "rb+") as file:
        file.seek(-1, 2)
        file.truncate()


def chunk_generator(iterable: Iterable, batch_size: int = 1000) -> Iterator[List[Any]]:
    """Yield chunks of an iterable.

    Parameters
    ----------
    iterable : Iterable
        The iterable to chunk.
    batch_size : int, optional
        The size of each chunk. Defaults to 1000.

    Returns
    -------
    Iterator[List[Any]]
        An iterator that yields chunks of the iterable.
    """
    it = iter(iterable)
    while True:
        # slice iterable [0, chunk_size] and returns generator
        chunk_it = itertools.islice(it, batch_size)
        try:
            first_el = next(chunk_it)
        except (
            StopIteration
        ):  # if iterator was exhausted and StopIteration raised breaks.
            return
        # joins first element and chunk without first element into one list. more: itertools.chain docs
        yield list(itertools.chain((first_el,), chunk_it))
