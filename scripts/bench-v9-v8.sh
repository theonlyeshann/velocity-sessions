#!/usr/bin/env bash
# Native AMD64 V8 vs V9 A/B benchmark. Run from the repository with dev20m present.
# No source mutation; each image is built separately. Output checks are byte-exact.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD"
DATASET="${DATASET:-dev20m}"
RUNS="${RUNS:-3}"
BASE_REF="${BASE_REF:-v8-identity-experiment}"
if [[ "$DATASET" = /* ]]; then DATA_PATH="$DATASET"; else DATA_PATH="$ROOT/$DATASET"; fi
if [[ ! -f "$DATA_PATH/manifest.json" ]]; then echo "Missing dataset $DATA_PATH" >&2; exit 1; fi
if ! git cat-file -e "${BASE_REF}^{commit}"; then echo "V8 baseline ref $BASE_REF missing" >&2; exit 1; fi
WORK="$ROOT/bench-v9-v8"
mkdir -p "$WORK/v8-source" "$WORK/v9-source/src" "$WORK/v8-output" "$WORK/v9-output" "$WORK/scratch"
chmod 777 "$WORK/v8-output" "$WORK/v9-output" "$WORK/scratch"
echo "Building V8 from commit $(git rev-parse --short "$BASE_REF")"
git archive "$BASE_REF" | tar -xf - -C "$WORK/v8-source"
docker buildx build --platform linux/amd64 --load -t velocity-sessions:v8-compare "$WORK/v8-source"
echo 'Building V9 from the current working tree'
cp src/main.cpp src/fast_parquet.hpp "$WORK/v9-source/src/"
cp Dockerfile run.sh "$WORK/v9-source/"
docker buildx build --platform linux/amd64 --load -t velocity-sessions:v9-compare "$WORK/v9-source"
for ((i=1;i<=RUNS;i++)); do
 for version in v8 v9; do
  OUT="$WORK/$version-output"
  # The container is UID 1000 while the VM SSH user has a different UID.
  sudo find "$OUT" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  echo "===== $version run $i/$RUNS (8 vCPU, 24 GiB, native AMD64) ====="
  /usr/bin/time -f 'HOST_WALL=%e' docker run --rm --platform linux/amd64 \
   --cpus 8 --memory 24g --memory-swap 24g --network none --read-only \
   --user 1000:1000 --tmpfs /tmp:rw,size=64m \
   -v "$DATA_PATH:/data:ro" -v "$OUT:/output" -v "$WORK/scratch:/scratch" \
   -e VELOCITY_THREADS=8 "velocity-sessions:$version-compare" 2>&1 \
   | tee "$WORK/$version-run-$i.log" \
   | grep -E '\[PROFILE\]|native_seconds|HOST_WALL|recovered|general-purpose'
 done
done
echo 'Checking V8/V9 output file equality (byte-for-byte)...'
diff -qr "$WORK/v8-output" "$WORK/v9-output"
echo 'MATCH — V8 and V9 Parquet outputs are byte-identical. Compare medians before promoting.'
