import csv
import itertools
from typing import Any, Optional, Iterable, Iterator, List
from pathlib import Path
import os

import ijson
from reppy.data_types import FilePath
from reppy.ext import NotSupportedFileFormat
from reppy.log import get_logger

logger = get_logger(__name__)

SUPPORTED_FILE_SUFFIXES = (
    "csv",
    "json",
    "parquet",
    "xlsx",
)


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


def get_file_suffix(path: str, dot: bool = True):
    return Path(path).suffix if dot else Path(path).suffix[1:]


def check_extension(suffix: str) -> Optional[str]:
    """
    Checks if the file suffix is supported.

    Parameters
    ----------
    suffix: str
        The file suffix.

    Returns
    -------
    str
        The file suffix if it is supported, or raises an exception otherwise.

    Raises
    ------
    NotSupportedFileFormat
        If the file suffix is not supported.
    """
    assert isinstance(suffix, str), "suffix should be a string"
    try:
        *_, suffix = suffix.split(".")
    except (ValueError, AttributeError) as vae:
        logger.warning(f"{str(vae)}")
    else:
        if suffix in SUPPORTED_FILE_SUFFIXES:
            return suffix
        raise NotSupportedFileFormat(
            f"The file suffix '{suffix}' is not supported. Supported file suffixes are: {SUPPORTED_FILE_SUFFIXES}"
        )


def file_exists(path: str):
    return Path(path).exists()


def is_dir(path: str):
    return Path(path).is_dir()


def write_file(path: str, data: Any) -> None:
    with open(path, "w") as f:
        f.write(data)


def list_files(path: str, extension: str = None) -> Optional[list]:
    files = (file for file in os.scandir(path) if get_file_suffix(file.name) != "")
    return (
        list(files)
        if extension is None
        else [file for file in files if extension in file.name]
    )


def read_json(file_path: FilePath, chunk_size: int = 1000):
    """read json file in chunks in lazy way"""

    def _read_json():
        with open(file_path, "rb") as f:
            for record in ijson.items(f, "item"):
                del record["_id"]
                yield record

    yield from chunk_generator(_read_json(), chunk_size)


def read_csv(file_path: FilePath, chunk_size: int = 1000):
    with open(file_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        for chunk in chunk_generator(reader, chunk_size):
            yield chunk
