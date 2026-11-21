#!/usr/bin/env bash
# Reproducible native-amd64 Docker side-by-side on one generated dataset.
# Keeps official V7 from immutable Git SHA and benchmarks candidate V8 separately.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD"
DATASET="${DATASET:-dev20m}"
RUNS="${RUNS:-3}"
BASE_SHA="3087f8266d3d6b340696a95160f6d88300041820"
if [[ "$DATASET" = /* ]]; then DATA_PATH="$DATASET"; else DATA_PATH="$ROOT/$DATASET"; fi
if [[ ! -f "$DATA_PATH/manifest.json" ]]; then echo "Missing dataset $DATA_PATH" >&2; exit 1; fi
if ! git cat-file -e "$BASE_SHA^{commit}"; then echo "V7 baseline commit $BASE_SHA missing. Fetch Git history first." >&2; exit 1; fi
WORK="$ROOT/bench-v8-v7"
mkdir -p "$WORK/v7-source" "$WORK/v8-source/src" "$WORK/v7-output" "$WORK/v8-output" "$WORK/scratch"
chmod 777 "$WORK/v7-output" "$WORK/v8-output" "$WORK/scratch"
# Git archive copies the exact V7 submission without altering the active branch.
git archive "$BASE_SHA" | tar -xf - -C "$WORK/v7-source"
echo 'Building V7 baseline image from pinned commit...'
docker buildx build --platform linux/amd64 --load -t velocity-sessions:v7-test "$WORK/v7-source"
echo 'Building V8 candidate image from current source...'
# Minimal build context: never upload the 20M-event dataset to Docker build.
cp src/main.cpp src/fast_parquet.hpp "$WORK/v8-source/src/"
cp Dockerfile run.sh "$WORK/v8-source/"
docker buildx build --platform linux/amd64 --load -t velocity-sessions:v8-test "$WORK/v8-source"
for ((i=1;i<=RUNS;i++)); do
 for version in v7 v8; do
  OUT="$WORK/$version-output"
  sudo find "$OUT" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  echo "===== $version run $i/$RUNS (8 vCPUs, 24 GiB, native amd64) ====="
  /usr/bin/time -f 'HOST_WALL=%e' docker run --rm --platform linux/amd64 \
    --cpus 8 --memory 24g --network none --read-only --user 1000:1000 \
    --tmpfs /tmp:rw,size=64m \
    -v "$DATA_PATH:/data:ro" -v "$OUT:/output" \
    -v "$WORK/scratch:/scratch" \
    -e VELOCITY_THREADS=8 \
    "velocity-sessions:$version-test" 2>&1 | tee "$WORK/$version-run-$i.log" | grep -E '\[PROFILE\]|native_seconds|HOST_WALL|recovered|general-purpose'
 done
done
# Same writer and shard layout: identical Parquet bytes are a stronger check
# than comparing row counts. Do not submit if this differs.
echo 'Checking exact V7/V8 Parquet outputs...'
diff -qr "$WORK/v7-output" "$WORK/v8-output"
echo 'MATCH — outputs byte-identical. Review median timings before submitting.'
