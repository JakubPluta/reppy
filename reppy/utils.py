import functools
import itertools
from typing import Any, Iterable, Iterator, List

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
