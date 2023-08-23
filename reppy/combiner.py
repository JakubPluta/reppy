import csv
from pathlib import Path

from reppy.log import get_logger
from reppy.utils import list_files, read_csv

logger = get_logger(__name__)


def _combine_csv(input_dir: str, output_path: str, extension: str = None, **kwargs):
    if not Path(input_dir).is_dir():
        raise Exception("is not directory")

    logger.debug(f"writing data to {output_path}")

    rows = 0
    if "csv" not in output_path:
        output_path += ".csv"

    with open(output_path, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        for f_idx, file in enumerate(list_files(input_dir, extension)):
            logger.debug(f"{f_idx} {file.path} opened")
            for chunk_idx, chunk in enumerate(read_csv(file.path)):
                if f_idx > 0 and chunk_idx == 0:
                    chunk = chunk[1:]
                rows += len(chunk)
                logger.debug(
                    f"writing file {file.path} chunk: {chunk_idx} to {output_path}"
                )
                writer.writerows(chunk)

    logger.debug(f"writing done {rows} rows")
