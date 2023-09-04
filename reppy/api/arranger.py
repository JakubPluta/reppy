import abc
import csv
import json
from typing import Any, List, Optional, TextIO, Tuple

import pyarrow.parquet as pq

from reppy.api.file_handlers import (
    add_missing_suffix,
    add_number_prefix_to_file_path,
    get_file_suffix,
    list_files,
    mkdir_if_not_exists,
    remove_last_character,
    validate_file_path,
    write_closing_bracket,
)
from reppy.api.readers import read_csv, read_json, read_parquet, read_text
from reppy.data_types import PathLike, MergeState
from reppy.ext import InvalidFileFormat, NoFilesInDirectory
from reppy.log import get_logger
from reppy.utils import _skip_header

logger = get_logger(__name__)


class _Combiner:
    """Base class for combiners.

    Parameters
    ----------
    input_dir (PathLike): The input directory for the data.
    output_path (PathLike): The output path for the data.
    pattern: Optional[str] = None,
         The file pattern to match.
    recursive: bool = False,
         Whether to search recursively.
    chunk_size: int = 10_000,
         The chunk size for reading the files.
    max_file_records: Optional[int] = None,
         The maximum number of records to write to each file.

    Raises
    ------
    NoFilesInDirectory: If there are no files to read in the input directory.

    """

    file_extension: str

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
        self.max_file_records = min(max_file_records, 1) if max_file_records else None

        self._prepare_output_paths(output_path, self.file_extension)
        self._files_list = list_files(
            self.input_dir, self.ext, self.recursive, self.pattern
        )
        if not self._files_list:
            raise NoFilesInDirectory(
                f"there are no files to read in directory {self.input_dir}"
            )

    def _prepare_output_paths(self, output_path: PathLike, file_extension: str) -> None:
        """Prepare the output paths for the data.

        Parameters
        ----------
        output_path (PathLike): The output path for the data.
        file_extension (str): The file extension for the data.

        Returns
        -------
        None.

        Raises
        ------
        InvalidFileFormat: If the file format is invalid.

        """

        # Add the missing file extension to the output path.
        output_path = add_missing_suffix(output_path, file_extension)
        # Get the file extension from the output path
        ext = get_file_suffix(output_path, True)

        # Check if the file extension is valid.
        if file_extension not in ext:
            raise InvalidFileFormat(
                f"for output_path={output_path} file format is invalid {ext}, should be {self.file_extension} "
            )

        # Create the output directory if it does not exist.
        mkdir_if_not_exists(output_path)
        self.output_path = output_path
        self.ext = ext

    @abc.abstractmethod
    def merge(self):
        raise NotImplementedError


class CSVCombiner(_Combiner):
    file_extension = ".csv"

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
            logger.debug(f"writing file {file_path} chunk: {chunk_idx}")
            chunk = chunk[1:] if _skip_header(file_index, chunk_idx) else chunk
            writer.writerows(chunk)
            rows += len(chunk)
        logger.debug(f"from {file_path}: rows {rows} written")
        return rows

    def merge_single(self) -> None:
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
        csv_file, output_path, header, writer = None, None, None, None
        ms = MergeState()
        try:
            for file_index, file_path in enumerate(self._files_list):
                for chunk_idx, chunk in enumerate(read_csv(file_path, self.chunk_size)):
                    header = chunk[0] if header is None and chunk_idx == 0 else header

                    if (
                        csv_file is None
                    ):  # if file is closed, open new file and get writer
                        output_path = add_number_prefix_to_file_path(
                            self.output_path, ms.prefix
                        )
                        csv_file, writer = self._open_new_file(output_path)
                        ms.is_opened = True

                    logger.debug(
                        f"writing file {file_path} chunk: {chunk_idx} to {output_path}"
                    )
                    chunk = self._prep_chunk(ms.is_opened, chunk_idx, chunk, header)

                    writer.writerows(chunk)
                    ms.rows += len(chunk)

                    if ms.rows >= self.max_file_records:
                        csv_file, writer = self._cleanup(csv_file)
                        ms.prefix += 1
                        ms.rows = 0

                    ms.is_opened = False

        except Exception as e:
            logger.error(e)
            self._cleanup(csv_file)

    def merge(self):
        if self.max_file_records:
            self.merge_many()
        else:
            self.merge_single()


class JSONCombiner(_Combiner):
    file_extension = ".json"

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


class ParquetCombiner(_Combiner):
    file_extension = ".parquet"

    @staticmethod
    def _cleanup(writer: pq.ParquetWriter):
        if writer:
            writer.close()

    def merge(self):
        logger.debug(f"writing data to {self.output_path}")
        rows = 0
        writer: Optional[pq.ParquetWriter] = None  # create variable for writer

        try:
            for file_idx, file_path in enumerate(self._files_list):
                logger.debug(f"{file_idx} {file_path} opened")
                for chunk_idx, chunk in enumerate(read_parquet(file_path)):
                    logger.debug(
                        f"writing file {file_idx} chunk: {chunk_idx} to {self.output_path}"
                    )
                    if writer is None:
                        writer = pq.ParquetWriter(
                            self.output_path, chunk.schema
                        )  # schema is available when first chunk read
                    writer.write_batch(chunk)
                    rows += len(chunk)
        except Exception as e:
            logger.error(str(e))
        finally:
            if writer:
                writer.close()
        logger.debug(f"writing done {rows} rows")


class TextCombiner(_Combiner):
    file_extension = ".txt"

    def _write_file(self, file: TextIO, file_path: PathLike):
        rows = 0
        for chunk_idx, chunk in enumerate(read_text(file_path)):
            file.writelines(chunk)
            rows += len(chunk)
            logger.debug(
                f"writing file {file_path} chunk: {chunk_idx} to {self.output_path}"
            )
        return rows

    def merge(self):
        logger.debug(f"writing data to {self.output_path}")
        rows = 0
        with open(self.output_path, "w") as file:
            for file_idx, file_path in enumerate(self._files_list):
                rows += self._write_file(file, file_path)
        logger.debug(f"writing done {rows} rows")
