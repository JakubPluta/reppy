import csv
import dataclasses
import io
import itertools
from pathlib import Path
from typing import Any, Iterable, Iterator, List, AnyStr, Union

from reppy.data_types import PathLike
from reppy.decorators import valid_file_path
from reppy.log import get_logger

logger = get_logger(__name__)


@valid_file_path
def remove_last_character(file_path: PathLike):
    """open file and remove last character"""
    with open(file_path, "rb+") as file:
        file.seek(-1, 2)
        file.truncate()


@valid_file_path
def write_closing_bracket(file_path: PathLike):
    with open(file_path, "a") as file:
        file.write("]")


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


def add_missing_suffix(file_path: PathLike, file_extension: str) -> PathLike:
    """
    Add file extension to file path.

    Parameters
    ----------
    file_path: Union[str, Path]
        The path to the file.
    file_extension: str
        The file extension to add.

    Returns
    -------
    Union[str, Path]
        The file path with the extension added.
    """

    if file_path.endswith(file_extension):
        return file_path
    return (
        f"{file_path}{file_extension}"
        if file_path.endswith(".")
        else f"{file_path}.{file_extension}"
    )


def _skip_header(file_index: int, chunk_idx: int) -> bool:
    return file_index > 0 and chunk_idx == 0


def add_no_prefix_to_file_path(file_path: PathLike, name_suffix: Union[AnyStr, int]):
    p = Path(file_path)
    return Path(p.parent, f"{p.stem}_{name_suffix}{p.suffix}").resolve()
