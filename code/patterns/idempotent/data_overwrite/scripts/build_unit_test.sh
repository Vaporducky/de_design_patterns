#!/bin/bash

# Exit on any error
set -e

BASE_PATH=code/patterns
PATTERN=idempotent
JOB_NAME=data_overwrite
TEST_PATH=${BASE_PATH}/${PATTERN}/${JOB_NAME}/tests

bash code/build_common/code_executor/execute_python.sh \
    --module pytest \
    --common "${BASE_PATH}/${PATTERN}/${JOB_NAME}" \
    -- -v ${TEST_PATH}/unit
