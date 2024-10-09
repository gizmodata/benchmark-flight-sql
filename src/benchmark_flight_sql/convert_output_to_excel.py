import json
from pathlib import Path

import click
import pandas as pd

from .config import get_logger, BENCHMARK_OUTPUT_FILEPATH, BENCHMARK_EXCEL_OUTPUT_FILEPATH

DATA_DIR = Path("data").resolve()


@click.command()
@click.option(
    "--input-filename",
    type=str,
    default=BENCHMARK_OUTPUT_FILEPATH.as_posix(),
    required=True,
    show_default=True,
    help="The JSON file output by the benchmark utility"
)
@click.option(
    "--output-excel-filename",
    type=str,
    default=BENCHMARK_EXCEL_OUTPUT_FILEPATH.as_posix(),
    required=True,
    show_default=True,
    help="The Excel file to create from the source JSON (will be output to the 'data' directory)"
)
def convert_output_to_excel(input_filename: str,
                            output_excel_filename: str
                            ):
    logger = get_logger()

    logger.info(msg=f"Function called with args: {locals()}")

    run_data_filepath = Path(input_filename)

    # Read the data into a dict
    with open(file=run_data_filepath, mode="r") as data:
        run_data_dict = json.loads(data.read())

    df = pd.json_normalize(data=run_data_dict,
                           record_path=["query_run_results"],
                           meta=["run_date", "hostname", "port", "disable_certificate_validation", "username",
                                 "schema", "query_yaml_filename", "database_version",
                                 "overall_start_datetime", "overall_start_time", "overall_success_count",
                                 "overall_failure_count", "overall_end_datetime", "overall_end_time",
                                 "overall_run_time"]
                           )

    output_excel_filepath = Path(output_excel_filename)
    df.to_excel(excel_writer=output_excel_filepath)

    logger.info(msg=f"Successfully created Excel file: '{output_excel_filepath.as_posix()}'")


if __name__ == '__main__':
    convert_output_to_excel()
