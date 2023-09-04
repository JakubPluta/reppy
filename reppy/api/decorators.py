import functools
from typing import Callable

from reppy.api.file_handlers import mkdir_if_not_exists, validate_file_path


def valid_file_path(func: Callable) -> Callable:
    """Decorator to validate the file path.

    Parameters
    ----------
    func: The function to decorate.

    Returns
    -------
    Callable: The decorated function.

    """

    @functools.wraps(func)
    def wrapper(file_path, *args, **kwargs):
        file_path = validate_file_path(file_path)
        return func(file_path, *args, **kwargs)

    return wrapper


def mkdir_decorator(func: Callable) -> Callable:
    """Decorator to create the directory if it does not exist.

    Parameters
    ----------
    func: The function to decorate.

    Returns
    -------
    Callable: The decorated function.

    """

    @functools.wraps(func)
    def wrapper(file_path, *args, **kwargs):
        mkdir_if_not_exists(file_path)
        return func(file_path, *args, **kwargs)

    return wrapper
