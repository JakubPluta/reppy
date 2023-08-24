import csv
import json
from pathlib import Path
from typing import Optional
import pyarrow.parquet as pq
from reppy.log import get_logger
from reppy.utils import (
    list_files,
    read_csv,
    read_json,
    remove_last_character,
    read_parquet,
)

logger = get_logger(__name__)


class Combiner:
    def _check_dir(self, path_dir: str):
        if not Path(path_dir).is_dir():
            raise Exception("is not directory")

    def combine(self):
        pass


def _combine_csv(input_dir: str, output_path: str, **kwargs):
    if not Path(input_dir).is_dir():
        raise Exception("is not directory")

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


def _combine_json(input_dir: str, output_path: str, **kwargs):
    if not Path(input_dir).is_dir():
        raise Exception("is not directory")
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


def _combine_parquet(input_dir: str, output_path: str, **kwargs):
    if not Path(input_dir).is_dir():
        raise Exception("is not directory")
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
