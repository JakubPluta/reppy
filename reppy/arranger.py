import csv
import json
from pathlib import Path
from typing import Optional

import pyarrow.parquet as pq

from reppy.data_types import PathLike
from reppy.decorators import mkdir_decorator
from reppy.ext import InvalidFileFormat
from reppy.fh import (
    list_files,
    validate_file_path,
    mkdir_if_not_exists,
    get_file_suffix,
)
from reppy.log import get_logger
from reppy.readers import read_csv, read_json, read_parquet, read_text
from reppy.utils import remove_last_character, add_extension_to_file_path

logger = get_logger(__name__)


class CSVCombiner:
    def __init__(
        self,
        *,
        input_dir: PathLike,
        output_path: PathLike,
        pattern: str = None,
        recursive: bool = False,
        chunk_size: int = 1000,
        max_file_records: int = None,
        max_file_size: int = None,
        **kwargs,
    ):
        self.input_dir = validate_file_path(input_dir)

        output_path = add_extension_to_file_path(output_path, "csv")
        self.ext = get_file_suffix(output_path, False)
        if self.ext != ".csv":
            raise InvalidFileFormat(
                f"for output_path={output_path} file format is invalid {self.ext}, should be .csv "
            )
        mkdir_if_not_exists(output_path)

        self.output_path = Path(output_path)
        self.pattern = pattern
        self.recursive = recursive
        self.chunk_size = chunk_size
        self.max_file_records = max_file_records
        self.max_file_size = max_file_size

        self._files_list = list_files(
            self.input_dir, "csv", self.recursive, self.pattern
        )

    def merge_files(self):
        rows = 0
        logger.debug(f"writing data to {self.output_path}")
        with open(self.output_path, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            for file_index, file_path in enumerate(self._files_list):
                logger.debug(f"{file_index} {file_path} opened")
                for chunk_idx, chunk in enumerate(read_csv(file_path)):
                    if file_index > 0 and chunk_idx == 0:
                        chunk = chunk[1:]
                    rows += len(chunk)
                    logger.debug(
                        f"writing file {file_path.name} chunk: {chunk_idx} to {self.output_path}"
                    )
                    writer.writerows(chunk)
        logger.debug(f"writing done {rows} rows")


@mkdir_decorator
def _combine_csv(input_dir: PathLike, output_path: PathLike, **kwargs):
    logger.debug(f"writing data to {output_path}")
    rows = 0
    output_path += ".csv" if "csv" not in output_path else ""
    with open(output_path, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        for f_idx, file in enumerate(list_files(input_dir)):
            logger.debug(f"{f_idx} {file} opened")
            for chunk_idx, chunk in enumerate(read_csv(file)):
                if f_idx > 0 and chunk_idx == 0:
                    chunk = chunk[1:]
                rows += len(chunk)
                logger.debug(f"writing file {file} chunk: {chunk_idx} to {output_path}")
                writer.writerows(chunk)

    logger.debug(f"writing done {rows} rows")


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
