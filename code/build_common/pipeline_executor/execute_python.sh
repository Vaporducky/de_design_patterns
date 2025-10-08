#!/bin/bash
# ============================================================================
# SCRIPT: PYTHON EXECUTOR
# AUTHOR: Enrique Fonseca
# DATE:   2025-06-03
# REV:    1.2.A (Valid are A, B, D, T and P)
# (For Alpha, Beta, Dev, Test and Production)
#
# PLATFORM: Linux
#
# PURPOSE: Clear and necessary description of the task
#
# REV LIST:
#     DATE: DATE_of_REVISION
#     BY:   AUTHOR_of_MODIFICATION
#     MODIFICATION: Describe what was modified, new features, etc--
#
#
# set -n # Uncomment to check script syntax, without execution.
#        # NOTE: Do not forget to put the comment back in or
#        # the shell script will not execute!
# set -x # Uncomment to debug this shell script
# ============================================================================

set -euo pipefail
IFS=$'\n\t'

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
_log() {
    local level="$1"; shift
    printf '[%s] [%s] %s\n' "$(date +'%F %T')" "${level}" "$*"
}

log_info()  {
    _log "INFO"  "$@";
}

log_error() {
    _log "ERROR" "$@" >&2;
}

# -----------------------------------------------------------------------------
# Cleanup and Error Trap
# -----------------------------------------------------------------------------
cleanup() {
    log_info "Running cleanup."
    # Further cleaning logic
    log_info "Cleanup complete."
}
# Error and signal handling
on_abort() {
    local exit_code=$?
    log_error "Aborted by signal or error at: ${BASH_COMMAND} - (exit code: ${exit_code})"
    exit $exit_code
}

# Register traps
trap on_abort SIGINT SIGTERM ERR
trap cleanup EXIT

# -----------------------------------------------------------------------------
# Globals (read-only)
# -----------------------------------------------------------------------------
readonly SCRIPT_NAME="$(basename "$0")"

if [[ "${SPARK_HOME:-}" == "/opt/bitnami/spark" ]]; then
    log_info "Executing inside Bitnami Spark container."
    readonly DEFAULT_PYENV_NAME="python"
    readonly DEFAULT_PYENV_ROOT="/opt/bitnami/"
    readonly DEFAULT_PYENV_BIN="${DEFAULT_PYENV_ROOT}/${DEFAULT_PYENV_NAME}/bin/python"
else
    readonly DEFAULT_PYENV_NAME="remix_code_challenge"
    readonly DEFAULT_PYENV_ROOT="${PYENV_ROOT:-$HOME/.pyenv}"
    readonly DEFAULT_PYENV_BIN="${DEFAULT_PYENV_ROOT}/versions/${DEFAULT_PYENV_NAME}/bin/python"
fi

# -----------------------------------------------------------------------------
# Usage / Help
# -----------------------------------------------------------------------------
usage() {
    cat <<EOF
Usage: ${SCRIPT_NAME} [options] [--script <path> | --module] --common <path> [ <python-args>...]
Options:
  -e, --pyenv-bin <path>    Path to python executable (default: ${DEFAULT_PYENV_BIN})
  -s, --script <path>       Python executable (required)
  -c, --common <path>       Common pipeline code directory (required)
  -h, --help                Show this help message and exit
EOF
    exit "${1:-0}"
}

# -----------------------------------------------------------------------------
# Argument Parsing
# -----------------------------------------------------------------------------
PYTHON_ENV="${DEFAULT_PYENV_BIN}"
SCRIPT_PATH=""
MODULE_NAME=""
COMMON_PIPELINE=""

PYTHON_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        -e|--pyenv-bin)
            PYTHON_ENV="${2:?Missing value for $1}"; shift 2
            ;;
        -s|--script)
            SCRIPT_PATH="${2:?Missing value for $1}"; shift 2
            ;;
        -m|--module)
            MODULE_NAME="${2:?Missing value for $1}"; shift 2
            ;;
        -c|--common)
            COMMON_PIPELINE="${2:?Missing value for $1}"; shift 2
            ;;
        -h|--help)
            usage
            ;;
        --)
        shift
        PYTHON_ARGS+=("$@")
        break
        ;;
        -*)
            # any other dash-option → Python
            PYTHON_ARGS+=("$1"); shift
            ;;
        *)
            # any positional → pass through to Python
            PYTHON_ARGS+=("$1"); shift
            ;;
    esac
done

# -----------------------------------------------------------------------------
# Validation & Canonicalization
# -----------------------------------------------------------------------------
if [[ -n "$SCRIPT_PATH" && -n "$MODULE_NAME" ]]; then
  log_error "Error: cannot use both --script and --module simultaneously."
  exit 1
fi
if [[ -z "$SCRIPT_PATH" && -z "$MODULE_NAME" ]]; then
  log_error "Error: one of --script or --module is required."
  usage 1
fi

# Ensure required
: "${COMMON_PIPELINE:?Error: --common is required}"

if [[ -n "${SCRIPT_PATH:-}" ]]; then
    SCRIPT_PATH="$(realpath "$SCRIPT_PATH")"
fi
COMMON_PIPELINE="$(realpath "$COMMON_PIPELINE")"
COMMON_CODE="$(realpath "$(pwd)/code/common")"

# -----------------------------------------------------------------------------
# Function declaration
# -----------------------------------------------------------------------------
setup_environment () {
    # Setup environment in order to run an arbitrary pipeline job

    # Check Python binary
    if [ ! -x "$PYTHON_ENV" ]; then
        log_error "The provided environment ${PYTHON_ENV} does not exist."

        exit 1
    fi
    # Check pipeline common environment
    if [ ! -d "$COMMON_PIPELINE" ]; then
        log_error "The provided common pipeline path ${COMMON_PIPELINE} does not exist."

        exit 1
    fi

    log_info "Configuration set:"
    log_info "========================================"
    log_info "[*] PYTHON_ENV:  ${PYTHON_ENV}"
    log_info "[*] COMMON_CODE: ${COMMON_CODE}"
    log_info "[*] COMMON_PIPELINE: ${COMMON_PIPELINE}"
    log_info "========================================"
}

main () {
    setup_environment

    log_info "Running Python executable target: ${SCRIPT_PATH}"

    if [[ -n "$SCRIPT_PATH" ]]; then
        PYTHONPATH="${COMMON_CODE}:${COMMON_PIPELINE}" \
        "${PYTHON_ENV}" \
        "$SCRIPT_PATH" \
        "${PYTHON_ARGS[@]}"
    else
        PYTHONPATH="${COMMON_CODE}:${COMMON_PIPELINE}" \
        "${PYTHON_ENV}" \
         -m "$MODULE_NAME" \
         "${PYTHON_ARGS[@]}"
    fi

    log_info "Successfully ran Python target. Exiting process."
    exit 0
}

main
