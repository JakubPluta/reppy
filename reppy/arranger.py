import csv
import dataclasses
import json
from typing import Optional, List, Any, AnyStr

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
)

logger = get_logger(__name__)


@dataclasses.dataclass(repr=True)
class MergeTracker:
    chunk_rows: int = 0
    all_rows: int = 0
    file_prefix: int = 0
    recently_opened_file: bool = False


class CSVCombiner:
    def __init__(
        self,
        input_dir: PathLike,
        output_path: PathLike,
        pattern: Optional[str] = None,
        recursive: bool = False,
        chunk_size: int = 5000,
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

    def merge_files(self) -> None:
        n_rows = 0
        logger.debug(f"writing data to {self.output_path}")
        with open(self.output_path, "w", newline="") as csv_file:
            writer = csv.writer(csv_file)
            for file_index, file_path in enumerate(self._files_list):
                n_rows += self._write_file(writer, file_index, file_path)
        logger.debug(
            f"writing done: {file_index + 1} files with {n_rows} rows written to {self.output_path}"
        )

    @staticmethod
    def _add_header_to_chunk(
        is_recently_opened_file: bool,
        chunk_idx: int,
        chunk: List[Any],
        header: List[AnyStr],
    ) -> List[Any]:
        if is_recently_opened_file:
            return chunk if chunk_idx == 0 else [header, *chunk]
        return chunk[1:] if chunk_idx == 0 else chunk

    @staticmethod
    def _open_new_file(output_path):
        csv_file = open(output_path, "w", newline="")
        writer = csv.writer(csv_file)
        recently_opened_file = True
        return csv_file, writer, recently_opened_file

    @staticmethod
    def _cleanup(csv_file):
        if csv_file is not None:
            csv_file.close()
        return None, None

    def merge_many(self):
        mtr = MergeTracker()
        csv_file, output_path, header, writer = None, None, None, None
        try:
            for file_index, file_path in enumerate(self._files_list):
                for chunk_idx, chunk in enumerate(read_csv(file_path, self.chunk_size)):
                    header = chunk[0] if header is None and chunk_idx == 0 else header

                    if (
                        csv_file is None
                    ):  # if file is closed, open new file and get writer
                        output_path = add_no_prefix_to_file_path(
                            self.output_path, mtr.file_prefix
                        )
                        csv_file, writer, recently_opened_file = self._open_new_file(
                            output_path
                        )

                    logger.debug(
                        f"writing file {file_path} chunk: {chunk_idx} to {output_path}"
                    )
                    chunk = self._add_header_to_chunk(
                        mtr.recently_opened_file, chunk_idx, chunk, header
                    )

                    writer.writerows(chunk)
                    mtr.chunk_rows += len(chunk)

                    if mtr.chunk_rows < self.max_file_records:
                        mtr.recently_opened_file = False
                        continue
                    else:
                        csv_file, writer = self._cleanup(csv_file)
                        mtr.file_prefix += 1
                        mtr.chunk_rows = 0

        except Exception as e:
            logger.error(e)
            self._cleanup(csv_file)


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
