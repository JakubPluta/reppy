import os.path
import csv
import os
import pathlib
from pathlib import Path
from typing import AnyStr, Generator, List, Optional, Union, Dict

from reppy.data_types import FilePath
from reppy.ext import NotSupportedFileFormat
from reppy.log import get_logger


SUPPORTED_FILE_SUFFIXES = (
    "csv",
    "json",
    "parquet",
    # "xlsx", # NOT SUPPORTED
)

logger = get_logger(__name__)


def get_file_suffix(path: str, dot: bool = True) -> str:
    """
    Get the file suffix.

    Parameters
    ----------
    path: str
        The path to the file.
    dot: bool
        Whether to include the dot in the suffix.

    Returns
    -------
    str
        The file suffix.

    Raises
    ------
    TypeError
        If the path is not a string.
    """

    if not isinstance(path, str):
        raise TypeError("path must be a string")

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


def file_exists(path: str) -> bool:
    """
    Checks if a file exists.

    Parameters
    ----------
    path: str
        The path to the file.

    Returns
    -------
    bool
        Whether the file exists.

    Raises
    ------
    TypeError
        If the path is not a string.
    """

    if not isinstance(path, str):
        raise TypeError("path must be a string")
    return Path(path).exists()


def get_delimiter(line: AnyStr) -> Optional[str]:
    """
    Get the delimiter from a line of text.

    Parameters
    ----------
    line: AnyStr
        The line of text to parse.

    Returns
    -------
    Optional[str]
        The delimiter, or `None` if it could not be determined.

    Raises
    ------
    ValueError
        If the line is not a string or bytes.
    """

    if not isinstance(line, str):
        raise ValueError("line must be a string or bytes")

    sniffer = csv.Sniffer()
    delimiter = sniffer.sniff(line).delimiter
    return delimiter


def _check_number_of_columns(file_path: str) -> int:
    """
    Checks the number of columns in a CSV file.

    Parameters
    ----------
    file_path: str
        The path to the CSV file.

    Returns
    -------
    int
        The number of columns in the CSV file.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    """

    with open(file_path, "r") as f:
        line = f.readline()
    return len(line.split(","))


def _check_all_files(file_paths: List[str]) -> Dict[FilePath, int]:
    """
    Checks the number of columns in all CSV files in a list.

    Parameters
    ----------
    file_paths: List[str]
        A list of paths to CSV files.

    Returns
    -------
    Dict[FilePath, int]
        A dictionary mapping each file path to the number of columns in the file.

    Raises
    ------
    FileNotFoundError
        If any of the files do not exist.
    """

    return {
        file: _check_number_of_columns(file)
        for file in file_paths
        if Path(file).is_file()
    }


def list_files(
    dir_path: str,
    ext: Optional[str] = None,
    recursive: bool = False,
    pattern: Optional[str] = None,
) -> List[Path]:
    """
    Get all paths matching the specified pattern in the specified directory.

    Parameters
    ----------
    dir_path: str
        The directory path.
    ext: Optional[str]
        The file extension to filter by.
    recursive: bool
        Whether to search recursively.
    pattern: Optional[str]
        The glob pattern to match.

    Returns
    -------
    List[Path]
        A list of paths matching the specified pattern.
    """
    pattern = "*" if pattern is None else pattern
    paths: Generator[pathlib.Path] = (
        Path(dir_path).rglob(pattern) if recursive else Path(dir_path).glob(pattern)
    )
    f: Path
    files = [f.resolve() for f in filter(os.path.isfile, paths)]
    return (
        list(files) if ext is None else [file for file in files if ext in file.suffix]
    )
