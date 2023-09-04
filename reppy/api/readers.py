import csv
from typing import Any, Dict, Generator, List

import ijson
import pyarrow.parquet as pq
from pyarrow import RecordBatch

from reppy.api.decorators import valid_file_path
from reppy.data_types import PathLike
from reppy.log import get_logger
from reppy.utils import chunk_generator

logger = get_logger(__name__)


@valid_file_path
def read_json(
    file_path: PathLike, chunk_size: int = 1000, **kwargs
) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Read JSON file in chunks in lazy way.

    Parameters
    ----------
    file_path: Union[str, Path]
        The path to the JSON file.
    chunk_size: int
        The chunk size.

    Yields
    ------
    Generator[Dict[str, Any], None, None]
        A generator that yields the records in the JSON file in chunks.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    """

    def _read_json():
        with open(file_path, "rb", **kwargs) as f:
            for record in ijson.items(f, "item"):
                if "_id" in record:  # for mongo documents
                    del record["_id"]
                yield record

    yield from chunk_generator(_read_json(), chunk_size)


@valid_file_path
def read_csv(
    file_path: PathLike, chunk_size: int = 5000, **kwargs
) -> Generator[List[Any], None, None]:
    """
    Read CSV file in chunks in lazy way.

    Parameters
    ----------
    file_path: Union[str, Path]
        The path to the CSV file.
    chunk_size: int
        The chunk size.
    **kwargs:
        Additional keyword arguments passed to the `csv.reader()` constructor.

    Yields
    ------
    Generator[List[Union[Any]], None, None]
        A generator that yields the records in the CSV file in chunks.

    """
    with open(file_path, "r") as csv_file:
        reader = csv.reader(csv_file, **kwargs)
        for chunk in chunk_generator(reader, chunk_size):
            yield chunk


@valid_file_path
def read_parquet(
    file_path: PathLike, chunk_size: int = 5000
) -> Generator[RecordBatch, None, None]:
    """
    Read Parquet file in chunks in lazy way.

    Parameters
    ----------
    file_path: Union[str, Path]
        The path to the Parquet file.
    chunk_size: int
        The chunk size.

    Yields
    ------
    Generator[RecordBatch, None, None]:
        A generator that yields the records in the Parquet file in chunks.

    """
    parquet_file = pq.ParquetFile(file_path)
    for batch in parquet_file.iter_batches(batch_size=chunk_size):
        yield batch


@valid_file_path
def read_text(
    file_path: PathLike, chunk_size: int = 5000, **kwargs
) -> Generator[List[Any], None, None]:
    """
    Read text file in chunks in lazy way.

    Parameters
    ----------
    file_path: Union[str, Path]
        The path to the text file.
    chunk_size: int
        The chunk size.
    **kwargs:
        Additional keyword arguments passed to the `open()` function.

    Yields
    ------
    Generator[List[Any], None, None]
        A generator that yields the chunks of text from the file.
    """

    with open(file_path, "r", **kwargs) as file:
        while chunk := file.read(chunk_size):
            yield chunk
