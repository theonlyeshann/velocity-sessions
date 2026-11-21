#!/bin/sh
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}"
export DATA_DIR OUTPUT_DIR VELOCITY_NATIVE_PARQUET=1
exec /app/velocity
