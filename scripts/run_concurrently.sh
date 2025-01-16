#!/bin/bash

set -e

NUM_PROCESSES=${1:?You MUST provide the number of processes to run concurrently!}

SCRIPT_DIR=$(dirname "${0}")

pushd "${SCRIPT_DIR}/../" || { echo "Failed to change directory!"; exit 1; }

mkdir -p logs

# Declare and export the function
run_benchmark_flight_sql() {
  local process_number=${1:?You MUST provide a process number!}

  local json_filename=data/benchmark-flight-sql-tpch-sf100-queries-${process_number}.json
  local xlsx_filename=data/benchmark-flight-sql-tpch-sf100-queries-${process_number}.xlsx

  benchmark-flight-sql \
     --num-query-runs=1 \
     --output-filename=${json_filename}

  # Convert the JSON output to an XLSX file
  benchmark-flight-sql-convert-output-to-excel \
    --input-filename=${json_filename} \
    --output-excel-filename=${xlsx_filename}
}

export -f run_benchmark_flight_sql

pids=()
for i in $(seq 0 $((NUM_PROCESSES - 1))); do
  log_filename=logs/benchmark-flight-sql-tpch-sf100-queries-${i}.log
  nohup bash -c "run_benchmark_flight_sql ${i}" > "${log_filename}" &
  pids+=($!)
done

for pid in "${pids[@]}"; do
  wait $pid || echo "Process $pid failed!"
done

popd || exit 1

echo -e "All done."
