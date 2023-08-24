import csv
import itertools
from typing import Any, Iterable, Iterator, List
import pyarrow.parquet as pq
import ijson

from reppy.data_types import FilePath
from reppy.log import get_logger

logger = get_logger(__name__)


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


def read_json(file_path: FilePath, chunk_size: int = 1000):
    """read json file in chunks in lazy way"""

    def _read_json():
        with open(file_path, "rb") as f:
            for record in ijson.items(f, "item"):
                if "_id" in record:
                    del record["_id"]
                yield record

    yield from chunk_generator(_read_json(), chunk_size)


def read_csv(file_path: FilePath, chunk_size: int = 5000):
    with open(file_path, "r") as csv_file:
        reader = csv.reader(csv_file)
        for chunk in chunk_generator(reader, chunk_size):
            yield chunk


def read_parquet(file_path: FilePath, chunk_size: int = 5000):
    parquet_file = pq.ParquetFile(file_path)
    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        yield batch
