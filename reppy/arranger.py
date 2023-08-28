import csv
import dataclasses
import json
from typing import Optional, List, Any, AnyStr, Tuple, TextIO

import pyarrow.parquet as pq

from reppy.data_types import PathLike
from reppy.decorators import mkdir_decorator
from reppy.ext import InvalidFileFormat, NoFilesInDirectory
from reppy.fh import (
    list_files,
    validate_file_path,
    mkdir_if_not_exists,
    get_file_suffix,
)
from reppy.log import get_logger
from reppy.readers import read_csv, read_json, read_parquet, read_text
from reppy.utils import (
    remove_last_character,
    add_missing_suffix,
    _skip_header,
    add_no_prefix_to_file_path,
    write_closing_bracket,
)

logger = get_logger(__name__)


@dataclasses.dataclass(repr=True)
class MergeTracker:
    rows: int = 0
    prefix: int = 0
    is_open: bool = False


class CSVCombiner:
    def __init__(
        self,
        input_dir: PathLike,
        output_path: PathLike,
        pattern: Optional[str] = None,
        recursive: bool = False,
        chunk_size: int = 10_000,
        max_file_records: Optional[int] = None,
    ):
        self.input_dir = validate_file_path(input_dir)
        self.pattern = pattern
        self.recursive = recursive
        self.chunk_size = chunk_size
        self.max_file_records = max_file_records

        self.output_path = add_missing_suffix(output_path, "csv")
        self.ext = get_file_suffix(self.output_path, True)
        if self.ext != ".csv":
            raise InvalidFileFormat(
                f"for output_path={self.output_path} file format is invalid {self.ext}, should be .csv "
            )
        mkdir_if_not_exists(self.output_path)

        self._files_list = list_files(
            self.input_dir, self.ext, self.recursive, self.pattern
        )
        if not self._files_list:
            raise NoFilesInDirectory(
                f"there are no files to read in directory {self.input_dir}"
            )

    @staticmethod
    def _prep_chunk(
        is_open: bool,
        idx: int,
        chunk: List[Any],
        header: List[Any],
    ) -> List[Any]:
        """
        Prepares a chunk of data for writing to a CSV file.

        Parameters
        ----------
        is_open: bool
            Whether the CSV file is already open.
        idx: int
            The index of the chunk.
        chunk: list
            The chunk of data.
        header: list
            The header for the CSV file.

        Returns
        -------
        list
            The prepared chunk of data.
        """
        if is_open:
            return chunk if idx == 0 else [header, *chunk]
        return chunk[1:] if idx == 0 else chunk

    @staticmethod
    def _open_new_file(output_path: PathLike) -> Tuple[TextIO, csv.writer]:
        """
        Opens a new CSV file for writing.

        Parameters
        ----------
        output_path: PathLike
            The path to the CSV file to open.

        Returns
        -------
        Tuple[TextIO, csv.writer]
            A tuple of the opened CSV file and a csv.writer object for writing to the file.
        """

        csv_file = open(output_path, "w", newline="")
        writer = csv.writer(csv_file)
        return csv_file, writer

    @staticmethod
    def _cleanup(csv_file: Optional[TextIO]) -> Tuple[None, None]:
        """
        Closes a CSV file if it is open.

        Parameters
        ----------
        csv_file: Optional[TextIO]
            The CSV file to close.

        Returns
        -------
        Tuple[None, None]
            A tuple of None, None.
        """

        if csv_file is not None:
            csv_file.close()
        return None, None

    def _write_file(
        self, writer: csv.writer, file_index: int, file_path: PathLike
    ) -> int:
        """
        Writes the contents of a CSV file to a new CSV file.

        Parameters
        ----------
        writer: csv.writer
            The writer object to use for writing to the new CSV file.
        file_index: int
            The index of the CSV file to write.
        file_path: PathLike
            The path to the CSV file to read.

        Returns
        -------
        int
            The number of rows written to the new CSV file.
        """

        rows = 0
        for chunk_idx, chunk in enumerate(read_csv(file_path, self.chunk_size)):
            logger.debug(f"Writing file {file_path} chunk: {chunk_idx}")
            chunk = chunk[1:] if _skip_header(file_index, chunk_idx) else chunk
            writer.writerows(chunk)
            rows += len(chunk)
        logger.debug(f"from {file_path}: rows {rows} written")
        return rows

    def merge(self) -> None:
        """
        Merges multiple CSV files into a single CSV file.

        Returns
        -------
        None
        """

        n_rows = 0
        logger.debug(f"Writing data to {self.output_path}")
        with open(self.output_path, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            for file_index, file_path in enumerate(self._files_list):
                n_rows += self._write_file(writer, file_index, file_path)
        logger.debug(
            f"Writing done: {file_index + 1} files with {n_rows} rows written to {self.output_path}"
        )

    def merge_many(self):
        t = MergeTracker()
        csv_file, output_path, header, writer = None, None, None, None
        try:
            for file_index, file_path in enumerate(self._files_list):
                for chunk_idx, chunk in enumerate(read_csv(file_path, self.chunk_size)):
                    header = chunk[0] if header is None and chunk_idx == 0 else header

                    if (
                        csv_file is None
                    ):  # if file is closed, open new file and get writer
                        output_path = add_no_prefix_to_file_path(
                            self.output_path, t.prefix
                        )
                        csv_file, writer = self._open_new_file(output_path)
                        t.is_open = True

                    logger.debug(
                        f"writing file {file_path} chunk: {chunk_idx} to {output_path}"
                    )
                    chunk = self._prep_chunk(t.is_open, chunk_idx, chunk, header)

                    writer.writerows(chunk)
                    t.rows += len(chunk)

                    if t.rows >= self.max_file_records:
                        csv_file, writer = self._cleanup(csv_file)
                        t.prefix += 1
                        t.rows = 0

                    t.is_open = False

        except Exception as e:
            logger.error(e)
            self._cleanup(csv_file)


class JSONCombiner:
    def __init__(
        self,
        input_dir: PathLike,
        output_path: PathLike,
        pattern: Optional[str] = None,
        recursive: bool = False,
        chunk_size: int = 10_000,
        max_file_records: Optional[int] = None,
    ):
        self.input_dir = validate_file_path(input_dir)
        self.pattern = pattern
        self.recursive = recursive
        self.chunk_size = chunk_size
        self.max_file_records = max_file_records

        self.output_path = add_missing_suffix(output_path, "json")
        self.ext = get_file_suffix(self.output_path, True)
        if self.ext != ".json":
            raise InvalidFileFormat(
                f"for output_path={self.output_path} file format is invalid {self.ext}, should be .json "
            )
        mkdir_if_not_exists(self.output_path)

        self._files_list = list_files(
            self.input_dir, self.ext, self.recursive, self.pattern
        )
        if not self._files_list:
            raise NoFilesInDirectory(
                f"there are no files to read in directory {self.input_dir}"
            )

    def _write_file(self, file: TextIO, file_path: PathLike) -> int:
        rows = 0
        for chunk_idx, chunk in enumerate(read_json(file_path)):
            file.write(json.dumps(chunk)[1:-1])
            file.write(",")
            rows += len(chunk)
            logger.debug(
                f"writing file {file_path} chunk: {chunk_idx} to {self.output_path}"
            )
        return rows

    def merge(self) -> None:
        logger.debug(f"writing data to {self.output_path}")
        rows = 0
        with open(self.output_path, "w") as f:
            f.write("[")
            for file_idx, file_path in enumerate(self._files_list):
                rows += self._write_file(f, file_path)

        remove_last_character(self.output_path)
        write_closing_bracket(self.output_path)
        logger.debug(f"writing done {rows} rows")


@mkdir_decorator
def _combine_parquet(input_dir: PathLike, output_path: PathLike, **kwargs):
    logger.debug(f"writing data to {output_path}")
    rows = 0
    output_path += ".parquet" if "parquet" not in output_path else ""

    writer: Optional[pq.ParquetWriter] = None  # create variable for writer

    try:
        for f_idx, file in enumerate(list_files(input_dir, "parquet")):
            logger.debug(f"{f_idx} {file} opened")
            for chunk_idx, chunk in enumerate(read_parquet(file)):
                logger.debug(f"writing file {file} chunk: {chunk_idx} to {output_path}")
                if writer is None:
                    writer = pq.ParquetWriter(
                        output_path, chunk.schema
                    )  # schema is available when first chunk read
                writer.write_batch(chunk)
                rows += len(chunk)
    except Exception as e:  # TODO: Improve this try/except block to be more implicit
        logger.error(str(e))
    finally:
        if writer:
            writer.close()
    logger.debug(f"writing done {rows} rows")
