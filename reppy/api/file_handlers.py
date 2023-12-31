import csv
import os
import os.path
import pathlib
from pathlib import Path
from typing import Any, AnyStr, Dict, Generator, List, Optional, Union

from reppy.api.decorators import valid_file_path
from reppy.data_types import PathLike
from reppy.ext import NotSupportedFileFormat
from reppy.log import get_logger

SUPPORTED_FILE_SUFFIXES = (
    "csv",
    "json",
    "parquet",
    "txt",
    # "xlsx", # NOT SUPPORTED
)

logger = get_logger(__name__)


def add_number_prefix_to_file_path(
    file_path: PathLike, name_suffix: Union[AnyStr, int]
) -> PathLike:
    """Add a number prefix to the file path.

    Parameters
    ----------
    file_path: The file path.
    name_suffix: The name suffix.

    Returns
    -------
    PathLike: The file path with the number prefix.

    """

    p = Path(file_path)
    return Path(p.parent, f"{p.stem}_{name_suffix}{p.suffix}").resolve()


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


def get_file_suffix(path: PathLike, dot: bool = True) -> str:
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


def file_exists(path: PathLike) -> bool:
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


def get_delimiter(line: AnyStr) -> Optional[Any]:
    """
    Get the delimiter from a line of text.

    Parameters
    ----------
    line: AnyStr
        The line of text to parse.

    Returns
    -------
    Optional[Any]
        The delimiter, or `None` if it could not be determined.

    Raises
    ------
    ValueError
        If the line is not a string or bytes.
    """

    if not isinstance(line, str):
        raise ValueError("line must be a string or bytes")

    sniffer = csv.Sniffer()
    return sniffer.sniff(line).delimiter


def _check_number_of_columns_in_csv_file(file_path: PathLike) -> int:
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


def _check_number_of_columns_in_all_csv_files(
    file_paths: List[PathLike],
) -> Dict[PathLike, int]:
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
        file: _check_number_of_columns_in_csv_file(file)
        for file in file_paths
        if Path(file).is_file()
    }


def _check_all_files_have_same_columns(file_paths: List[PathLike]) -> bool:
    """
    Checks if all the CSV files have the same number of columns.

    Parameters
    ----------
    file_paths: List[PathLike]
        A list of paths to the CSV files.

    Returns
    -------
    bool
        `True` if all the files have the same number of columns, `False` otherwise.
    """

    files: Dict[PathLike, int] = _check_number_of_columns_in_all_csv_files(file_paths)
    shapes = list(files.values())
    return all([x == shapes[0] for x in shapes])


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


def _file_partitionable(file_path: PathLike, column: AnyStr) -> bool:
    """Check if the file is partitionable.

    Parameters
    ----------
    file_path (PathLike): The file path.
    column: The column to check.

    Returns
    -------
    bool: Whether the file is partitionable.

    """
    with open(file_path, "r") as f:
        line = f.readline()
    return _partitionable(line, column)


def _partitionable(header_line: AnyStr, column: AnyStr) -> bool:
    """Check if the line is partitionable by checking if column exists in headers line

    Parameters
    ----------
    line: The line to check.
    column: The column to check.

    Returns
    -------
    bool: Whether the line is partitionable.

    """
    sep = get_delimiter(header_line)
    return column in header_line.split(sep or ",")


def validate_file_path(file_path: PathLike) -> PathLike:
    """Validate the file path if it exists and has proper format.

    Parameters
    ----------
    file_path: The file path.

    Returns
    -------
    Path: The validated file path.

    Raises
    ------
    TypeError: If the file path is not a string or PathLike object.
    FileNotFoundError: If the file does not exist.

    """
    if not isinstance(file_path, (str, PathLike)):
        raise TypeError("path must be a string or PathLike object")
    file_path = Path(file_path)
    if file_path.exists() is False:
        raise FileNotFoundError(f"file {file_path} not exists")
    return file_path


def mkdir_if_not_exists(file_path: PathLike) -> None:
    """Create the directory if it does not exist.

    Parameters
    ----------
    file_path: The file path.

    """

    file_path = Path(file_path)
    directory = file_path.parent
    if directory.exists() is False:
        directory.mkdir(parents=True, exist_ok=True)


@valid_file_path
def remove_last_character(file_path: PathLike) -> None:
    """Open file and remove the last character from it.

    Parameters
    ----------
    file_path: The file path.

    """
    with open(file_path, "rb+") as file:
        file.seek(-1, 2)
        file.truncate()


@valid_file_path
def write_closing_bracket(file_path: PathLike) -> None:
    """Write the closing bracket to the file. It's needed for combining jsons in chunk into one json file

    Parameters
    ----------
    file_path: The file path.

    """
    with open(file_path, "a") as file:
        file.write("]")
