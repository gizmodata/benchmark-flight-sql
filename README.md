# Flight SQL benchmark repo

This repo is intended to run benchmark queries against Flight SQL.

## Setup (to run locally)
a
### 1. Clone the repo
```shell
git clone https://github.com/gizmodata/benchmark-flight-sql

```

### 2. Setup Python
Create a new Python 3.8+ virtual environment and install the requirements with:
```shell
cd benchmark-flight-sql

# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

# Upgrade pip, setuptools, and wheel
pip install --upgrade pip setuptools wheel

# Install the benchmark-snowflake package (in editable mode)
pip install --editable .[dev]

```

### Note
For the following commands - if you running from source and using `--editable` mode (for development purposes) - you will need to set the PYTHONPATH environment variable as follows:
```shell
export PYTHONPATH=$(pwd)/src
```

### 3. Create .env file in root of repo folder
Create a .env file in the root folder of the repo - it will be git-ignored for security reasons.   

Sample contents:
```text
FLIGHT_HOSTNAME="localhost"
FLIGHT_PORT="31337"
FLIGHT_CERTIFICATE_VALIDATION="False"
FLIGHT_USERNAME="flight_username"
FLIGHT_PASSWORD="flight_password"
```

## Running the benchmarks (with default settings)

```shell
benchmark-flight-sql
```

Note: this will create a file in the [data](data) directory called: "benchmark_results.json" with the query run details.   

To see more options:
```shell
benchmark-flight-sql --help
```

## Converting the benchmark JSON output data to Excel format
```shell
benchmark-flight-sql-convert-output-to-excel
```

Note: this will create an Excel file in the [data](data) directory called: "benchmark_results.xlsx" with the query run details.
