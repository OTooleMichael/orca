#!/usr/bin/env bash
TARGET_DIR=${1:-"."}
GRCP_DIR="./grpc_generated"
GRCP_REGEX="/grpc_generated/"
set -e
echo "Formatting $TARGET_DIR"
python -m ruff format $TARGET_DIR
python -m ruff check --fix $TARGET_DIR --exclude=$GRCP_DIR
echo "MYPY"
mypy $TARGET_DIR --exclude="generate_grpc" --exclude="generated_grpc"
