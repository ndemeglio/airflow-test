#!/usr/bin/env bash

export REPO_ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

# DEFINE DIRECTORY STRUCTURE FOR RESOURCE DISCOVERY
export DAGS_DIR="${REPO_ROOT_DIR}/mixpanel/dags"
export SCHEMA_DIR="${REPO_ROOT_DIR}/mixpanel/schemas"
export TEMPLATE_DIR="${REPO_ROOT_DIR}/mixpanel/dags/templates"
export PLUGINS_DIR="${REPO_ROOT_DIR}/mixpanel/plugins"

# Declare test directory
export TESTS_DIR="${REPO_ROOT_DIR}/tests"


# AIRFLOW CONFIG OVERRIDES- VALUES FOR TESTING
export AIRFLOW_HOME=${AIRFLOW_HOME:=~/airflow}

export AIRFLOW__CORE__UNIT_TEST_MODE=True
export AIRFLOW__CORE__DAGS_FOLDER="${DAGS_DIR}"
export AIRFLOW__CORE__PLUGINS_FOLDER="${PLUGINS_DIR}"
export AIRFLOW__CORE__BASE_LOG_FOLDER="${REPO_ROOT_DIR}/tests/artifacts/logs"
export AIRFLOW__CORE__LOGGING_LEVEL="INFO"
# Keep scheduler clean & only load dags in DAGS_DIR
export AIRFLOW__CORE__LOAD_EXAMPLES=False

