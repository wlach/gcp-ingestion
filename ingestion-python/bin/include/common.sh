#!/usr/bin/env bash

set -eo pipefail

cd "$(dirname "$0")"/..

LINT_ARGS=( --black --docstyle --flake8 --mypy-ignore-missing-imports )

VENV_DIR="$PWD/venv/$(uname)"

PYTHON=python3.7

# use venv unless disabled
if ${VENV:-true}; then PYTHON="$VENV_DIR/bin/$PYTHON"; fi
