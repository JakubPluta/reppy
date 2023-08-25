import functools
from typing import Callable
from reppy.fh import validate_file_path, mkdir_if_not_exists


def valid_file_path(func: Callable):
    @functools.wraps(func)
    def wrapper(file_path, *args, **kwargs):
        file_path = validate_file_path(file_path)
        return func(file_path, *args, **kwargs)

    return wrapper


def mkdir_decorator(func: Callable):
    @functools.wraps(func)
    def wrapper(file_path, *args, **kwargs):
        mkdir_if_not_exists(file_path)
        return func(file_path, *args, **kwargs)

    return wrapper
