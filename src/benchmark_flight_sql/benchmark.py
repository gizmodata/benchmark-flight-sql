import time

import adbc_driver_flightsql.dbapi as flight_sql
import click
import json
import os
import yaml
from codetiming import Timer
from contextlib import contextmanager
from datetime import datetime
from dotenv import load_dotenv
from munch import Munch
from pathlib import Path

from .config import logging, get_logger, TIMER_TEXT, BENCHMARK_OUTPUT_FILEPATH, QUERIES_DIR

# Load our environment
load_dotenv(".env")


class FlightSQLBenchmark(object):
    def __init__(self,
                 hostname: str,
                 port: int,
                 disable_certificate_validation: bool,
                 username: str,
                 password: str,
                 schema: str,
                 query_yaml_filename: str,
                 num_query_runs: int,
                 output_filename: Path,
                 output_file_mode: str,
                 logger
                 ):
        self.logger = logger

        self._hostname = hostname
        self._port = port
        self._disable_certificate_validation = disable_certificate_validation

        self._username = username
        self._schema = schema
        self._query_yaml_filename = query_yaml_filename
        self._num_query_runs = num_query_runs
        self._output_file_mode = output_file_mode

        self.logger.info(msg=(f"Connecting to Flight SQL hostname: {self._hostname}\n"
                              f"Port: {self._port}\n"
                              f"Validate Server Certificate: {(not self._disable_certificate_validation)}\n"
                              f"Schema: {self._schema}\n"
                              f"- with username: {self._username}"
                              )
                         )

        self.con = flight_sql.connect(uri=f"grpc+tls://{hostname}:{port}",
                                      db_kwargs={"username": self._username,
                                                 "password": password,
                                                 "adbc.flight.sql.client_option.tls_skip_verify": str(
                                                     self._disable_certificate_validation).lower()
                                                 }
                                      )

        # Set the schema...
        with self.con.cursor() as cur:
            cur.execute(operation=f"SET schema={self._schema}")

        # Load the benchmark queries
        with open(file=Path(self._query_yaml_filename), mode="r") as yaml_file:
            self.benchmark_queries = Munch(yaml.safe_load(yaml_file.read()))

        self.output_file_path = Path(output_filename)

    def close_connection(self):
        self.con.close()

    @contextmanager
    def execute_sql(self,
                    command: str,
                    params: list = None,
                    timer_logger=None
                    ):
        if not timer_logger:
            timer_logger = self.logger.info

        with Timer(name=f"Execute SQL:\n{command}\n------\nParams: {params}",
                   text=TIMER_TEXT,
                   initial_text=True,
                   logger=timer_logger
                   ):
            cursor = self.con.cursor()
            try:
                cursor.execute(operation=command,
                               parameters=params
                               )
            except Exception as e:
                self.logger.exception(msg=f"function: execute_sql - encountered exception: {str(e)}")
                raise
            else:
                yield cursor
            finally:
                cursor.close()

    def execute_basic_sql(self,
                          command: str,
                          params: list = None,
                          timer_logger=None
                          ):
        with self.execute_sql(command=command,
                              params=params,
                              timer_logger=timer_logger) as _:
            pass

    def run_benchmark_query(self,
                            query: Munch,
                            run_number: int
                            ):
        query_run_details = Munch(run_number=run_number,
                                  start_datetime=datetime.now().astimezone(),
                                  start_time=time.time()
                                  )
        with Timer(name=f"Run query: '{query.query_id}' - with SQL:\n{query.sql}",
                   text=TIMER_TEXT,
                   initial_text=True,
                   logger=self.logger.info
                   ):
            try:
                with self.execute_sql(command=query.sql) as cursor:
                    result_pyarrow_table = cursor.fetch_arrow_table()
                    query_run_details.row_count = result_pyarrow_table.num_rows
            except Exception as e:
                query_run_details.status = "ERROR"
                query_run_details.exception = str(e)
            else:
                query_run_details.status = "SUCCESS"
            finally:
                query_run_details.end_time = time.time()
                query_run_details.end_datetime = datetime.now().astimezone()
                query_run_details.run_time = query_run_details.end_time - query_run_details.start_time

        return query_run_details

    def run_query_batch(self,
                        query: Munch
                        ):
        query_batch_run_details = Munch(query=query,
                                        batch_start_datetime=datetime.now().astimezone(),
                                        batch_start_time=time.time(),
                                        run_count=0,
                                        success_count=0,
                                        failure_count=0,
                                        runs=[]
                                        )
        run_times = []
        for i in range(1, self._num_query_runs + 1):
            query_batch_run_details.run_count += 1
            query_run_details = self.run_benchmark_query(query=query,
                                                         run_number=i
                                                         )
            if query_run_details.status == "SUCCESS":
                query_batch_run_details.success_count += 1
            elif query_run_details.status == "ERROR":
                query_batch_run_details.failure_count += 1

            run_times.append(query_run_details.run_time)
            self.logger.info(msg=f"Query: {query.query_id} - run number: {i} - details: {query_run_details}")
            query_batch_run_details.runs.append(query_run_details)

        query_batch_run_details.batch_end_datetime = datetime.now().astimezone()
        query_batch_run_details.batch_end_time = time.time()
        query_batch_run_details.batch_run_time = query_batch_run_details.batch_end_time - query_batch_run_details.batch_start_time

        if query_batch_run_details.success_count > 0:
            query_batch_run_details.mean_runtime = sum(run_times) / len(run_times)
            query_batch_run_details.min_runtime = min(run_times)
            query_batch_run_details.max_runtime = max(run_times)

        return query_batch_run_details

    def run_benchmark_queries(self):
        try:
            overall_start_time = time.time()

            all_query_run_details = Munch(run_date=datetime.now().astimezone(),
                                          hostname=self._hostname,
                                          port=self._port,
                                          disable_certificate_validation=self._disable_certificate_validation,
                                          username=self._username,
                                          schema=self._schema,
                                          query_yaml_filename=self._query_yaml_filename,
                                          overall_start_datetime=datetime.now().astimezone(),
                                          overall_start_time=time.time(),
                                          overall_success_count=0,
                                          overall_failure_count=0,
                                          query_run_results=[])

            # Get the Snowflake version
            with self.execute_sql(command="SELECT VERSION()") as cursor:
                all_query_run_details.database_version = cursor.fetchone()[0]

            with Timer(name=f"Run all queries",
                       text=TIMER_TEXT,
                       initial_text=True,
                       logger=self.logger.info
                       ):
                for query in self.benchmark_queries.queries:
                    query_munch = Munch(query)
                    query_batch_run_details = self.run_query_batch(query=query_munch)
                    self.logger.info(
                        msg=f"Query: {query_batch_run_details.query.query_id} - batch run details: {query_batch_run_details}")
                    all_query_run_details.query_run_results.append(query_batch_run_details)
                    all_query_run_details.overall_success_count += query_batch_run_details.success_count
                    all_query_run_details.overall_failure_count += query_batch_run_details.failure_count

            all_query_run_details.overall_end_datetime = datetime.now().astimezone()
            all_query_run_details.overall_end_time = time.time()
            all_query_run_details.overall_run_time = all_query_run_details.overall_end_time - all_query_run_details.overall_start_time

            with open(file=self.output_file_path,
                      mode=self._output_file_mode,
                      ) as output_file_stream:
                output_file_stream.write(json.dumps(obj=all_query_run_details, default=str))

        except Exception as e:
            self.logger.exception(f"function: run_benchmark_queries - encountered Exception: {str(e)}")
            raise
        else:
            self.logger.info("All queries completed")
        finally:
            self.close_connection()


@click.command()
@click.option(
    "--hostname",
    type=str,
    default=os.getenv("FLIGHT_HOSTNAME", "localhost"),
    required=True,
    show_default=True,
    help="The Flight SQL server hostname to connect to"
)
@click.option(
    "--port",
    type=int,
    default=os.getenv("FLIGHT_PORT", 31337),
    required=True,
    show_default=True,
    help="The Flight SQL server port to connect to"
)
@click.option(
    "--certificate-validation/--no-certificate-validation",
    type=bool,
    default=(os.getenv("FLIGHT_CERTIFICATE_VALIDATION", "True").upper() == "TRUE"),
    show_default=True,
    required=True,
    help="Validate the Flight SQL Server''s TLS Certificate"
)
@click.option(
    "--username",
    type=str,
    default=os.getenv("FLIGHT_USERNAME", "flight_username"),
    required=True,
    show_default=True,
    help="The username used to connect to Flight SQL"
)
@click.option(
    "--password",
    type=str,
    default=os.getenv("FLIGHT_PASSWORD"),
    required=True,
    show_default=False,
    help="The password used to connect to Flight SQL"
)
@click.option(
    "--schema",
    type=str,
    default=os.getenv("FLIGHT_SCHEMA", "main"),
    required=True,
    show_default=True,
    help="The Flight SQL schema to use for querying data (the search path)"
)
@click.option(
    "--query-yaml-filename",
    type=str,
    default=(QUERIES_DIR / "tpc_h.yaml").as_posix(),
    required=True,
    show_default=True,
    help="The YAML file containing the list of queries to run"
)
@click.option(
    "--num-query-runs",
    type=int,
    default=3,
    required=True,
    show_default=True,
    help="The number of times to run each query"
)
@click.option(
    "--output-filename",
    type=str,
    default=BENCHMARK_OUTPUT_FILEPATH.as_posix(),
    required=True,
    show_default=True,
    help="The file to put the run time stats into"
)
@click.option(
    "--output-file-mode",
    type=click.Choice(["a", "w"], case_sensitive=True),
    default="w",
    help="The output file mode, use value: a for 'append', and value: w to overwrite..."
)
@click.option(
    "--log-level",
    type=click.Choice(["INFO", "DEBUG", "WARNING", "CRITICAL"], case_sensitive=False),
    default=os.getenv("LOG_LEVEL", "INFO"),
    required=True,
    help="The logging level to use"
)
@click.option(
    "--log-file",
    type=str,
    default=os.getenv("LOG_FILE"),
    required=False,
    help="The log file to write to.  If None, will just log to stdout"
)
@click.option(
    "--log-file-mode",
    type=click.Choice(["a", "w"], case_sensitive=True),
    default=os.getenv("LOG_FILE_MODE", "w"),
    help="The log file mode, use value: a for 'append', and value: w to overwrite..."
)
def click_run_benchmark(hostname: str,
                        port: int,
                        certificate_validation: bool,
                        username: str,
                        password: str,
                        schema: str,
                        query_yaml_filename: str,
                        num_query_runs: int,
                        output_filename: str,
                        output_file_mode: str,
                        log_level: int,
                        log_file: str,
                        log_file_mode: str):
    logger = get_logger(filename=log_file,
                        filemode=log_file_mode,
                        logger_name="flight_server",
                        log_level=getattr(logging, log_level.upper())
                        )

    redacted_locals = {key: value for key, value in locals().items() if key not in ['password']}

    logger.info(msg=f"Called with parameter values: {redacted_locals}")

    flight_sql_benchmark = FlightSQLBenchmark(hostname=hostname,
                                              port=port,
                                              disable_certificate_validation=(not certificate_validation),
                                              username=username,
                                              password=password,
                                              schema=schema,
                                              query_yaml_filename=query_yaml_filename,
                                              num_query_runs=num_query_runs,
                                              output_filename=output_filename,
                                              output_file_mode=output_file_mode,
                                              logger=logger
                                              )
    flight_sql_benchmark.run_benchmark_queries()


if __name__ == '__main__':
    click_run_benchmark()
