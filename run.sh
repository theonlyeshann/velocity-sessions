#!/bin/sh
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR
export HOME="$SCRATCH_DIR" TMPDIR="$SCRATCH_DIR" PYTHONDONTWRITEBYTECODE=1
export STAGING_DIR="$SCRATCH_DIR/staging"
mkdir -p "$STAGING_DIR" "$OUTPUT_DIR"
# Native engine writes transient CSV into scratch, never into the scorer's outputs.
OUTPUT_DIR="$STAGING_DIR" /app/velocity
python3 -B /app/write_parquet.py
