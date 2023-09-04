import itertools
from typing import Any, Iterable, Iterator, List
from reppy.log import get_logger

logger = get_logger(__name__)


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


def _skip_header(file_index: int, chunk_idx: int) -> bool:
    """Skip the header.

    Parameters
    ----------
    file_index: The file index.
    chunk_idx: The chunk index.

    Returns
    -------
    bool: Whether to skip the header.

    """
    return file_index > 0 and chunk_idx == 0
