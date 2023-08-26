import csv
import json
from typing import Optional, List, Any, Union, AnyStr, Iterable
import pyarrow.parquet as pq

from reppy.data_types import PathLike
from reppy.decorators import mkdir_decorator
from reppy.ext import InvalidFileFormat, NoFilesInDirectory, PartitionColumnNotFound
from reppy.fh import (
    list_files,
    validate_file_path,
    mkdir_if_not_exists,
    get_file_suffix,
    _file_partitionable,
)
from reppy.log import get_logger
from reppy.readers import read_csv, read_json, read_parquet, read_text
from reppy.utils import remove_last_character, add_missing_suffix, _skip_header

logger = get_logger(__name__)


class CSVCombiner:
    def __init__(
        self,
        input_dir: PathLike,
        output_path: PathLike,
        pattern: Optional[str] = None,
        recursive: bool = False,
        chunk_size: int = 5000,
        partition_by: Optional[AnyStr] = None,
        check_partitions_threshold: float = 1.0,
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
        self.partition_by = partition_by
        self.check_partitions_threshold = check_partitions_threshold
        if self.partition_by is not None:
            self.partition_by = self._can_partition_by(partition_by)

    def _write_file(
        self, writer: csv.writer, file_index: int, file_path: PathLike
    ) -> int:
        rows = 0
        for chunk_idx, chunk in enumerate(read_csv(file_path, self.chunk_size)):
            logger.debug(f"writing file {file_path} chunk: {chunk_idx}")
            chunk = chunk[1:] if _skip_header(file_index, chunk_idx) else chunk
            writer.writerows(chunk)
            rows += len(chunk)
        logger.debug(f"from {file_path}: rows {rows} written")
        return rows

    def _can_partition_by(self, column: AnyStr) -> AnyStr:
        n_files = int(len(self._files_list) * self.check_partitions_threshold)
        files_list = self._files_list[:n_files]
        if not any([_file_partitionable(fp, column) for fp in files_list]):
            raise PartitionColumnNotFound(
                f"couldn't find partition column {column} in any of files {files_list}"
            )
        return column

    def merge_files(self) -> None:
        n_rows = 0
        logger.debug(f"writing data to {self.output_path}")
        with open(self.output_path, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            for file_index, file_path in enumerate(self._files_list):
                n_rows += self._write_file(writer, file_index, file_path)
        logger.debug(
            f"writing done: {file_index+1} files with {n_rows} rows written to {self.output_path}"
        )


@mkdir_decorator
def _combine_json(input_dir: PathLike, output_path: PathLike, **kwargs):
    logger.debug(f"writing data to {output_path}")
    rows = 0
    output_path += ".json" if "json" not in output_path else ""

    with open(output_path, "w") as f:
        f.write("[")

        for f_idx, file in enumerate(list_files(input_dir, "json")):
            logger.debug(f"{f_idx} {file} opened")
            for chunk_idx, chunk in enumerate(read_json(file)):
                f.write(json.dumps(chunk)[1:-1])
                f.write(",")
                rows += len(chunk)
                logger.debug(f"writing file {file} chunk: {chunk_idx} to {output_path}")

    remove_last_character(output_path)
    with open(output_path, "a") as file:
        file.write("]")
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
