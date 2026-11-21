#!/usr/bin/env bash
# V9 vs V10 native AMD64 A/B/C comparison. Includes byte-exact correctness verification.
# Uses a pinned committed V9 branch for baseline; does not modify any repo source.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$PWD"
DATASET="${DATASET:-dev20m}"
RUNS="${RUNS:-3}"
BASE_REF="${BASE_REF:-v9-fast-csv-experiment}"
BENCH_DIR="${BENCH_DIR:-$ROOT/bench-v10-v9}"
if [[ "$DATASET" = /* ]]; then DATA_PATH="$DATASET"; else DATA_PATH="$ROOT/$DATASET"; fi
if [[ ! -f "$DATA_PATH/manifest.json" ]]; then echo "Missing dataset: $DATA_PATH" >&2; exit 1; fi
if [[ "$(uname -m)" != x86_64 ]]; then echo "Requires native x86_64 Linux, not Docker emulation" >&2; exit 1; fi
if ! git cat-file -e "${BASE_REF}^{commit}"; then echo "Missing pinned V9 commit/ref: $BASE_REF" >&2; exit 1; fi
mkdir -p "$BENCH_DIR"/{v9-source,v10-source/src,v10-person-source/src,v9-output,v10-output,v10-person-output,scratch}
chmod 777 "$BENCH_DIR"/{v9-output,v10-output,v10-person-output,scratch}

echo "Baseline ref: $(git rev-parse --short "$BASE_REF")"
echo "Native CPU: $(lscpu | grep -m1 'Model name:' || true)"
echo "Input: $(du -sh "$DATA_PATH" | cut -f1)" 
echo "Compiling 3 isolated images (V9, V10, V10 person-local sort)"
git archive "$BASE_REF" | tar -xf - -C "$BENCH_DIR/v9-source"
cp src/main.cpp src/fast_parquet.hpp "$BENCH_DIR/v10-source/src/"
cp Dockerfile run.sh "$BENCH_DIR/v10-source/"
cp experiments/v10-person-sort.cpp "$BENCH_DIR/v10-person-source/src/main.cpp"
cp src/fast_parquet.hpp "$BENCH_DIR/v10-person-source/src/"
cp Dockerfile run.sh "$BENCH_DIR/v10-person-source/"
for version in v9 v10 v10-person; do
 docker buildx build --platform linux/amd64 --load -t "velocity-sessions:$version-compare" "$BENCH_DIR/$version-source"
done

for ((i=1;i<=RUNS;i++)); do
 for version in v9 v10 v10-person; do
  OUT="$BENCH_DIR/$version-output"
  # Docker uses UID 1000, while SSH VM user may have a different UID.
  sudo find "$OUT" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
  echo "===== $version run $i/$RUNS; 8 vCPU, 24 GiB; native AMD64 ====="
  /usr/bin/time -f 'HOST_WALL=%e' docker run --rm --platform linux/amd64 \
    --cpus 8 --memory 24g --memory-swap 24g --network none --read-only \
    --user 1000:1000 --tmpfs /tmp:rw,size=64m \
    -v "$DATA_PATH:/data:ro" -v "$OUT:/output" -v "$BENCH_DIR/scratch:/scratch" \
    -e VELOCITY_THREADS=8 "velocity-sessions:$version-compare" 2>&1 \
    | tee "$BENCH_DIR/$version-run-$i.log" \
    | grep -E '\[PROFILE\]|native_seconds|HOST_WALL|recovered|general-purpose'
 done
done

echo 'Checking byte-for-byte V9/V10/V10-person Parquet equality...'
diff -qr "$BENCH_DIR/v9-output" "$BENCH_DIR/v10-output"
diff -qr "$BENCH_DIR/v9-output" "$BENCH_DIR/v10-person-output"
echo 'MATCH: all versions produced identical Parquet outputs.'
python3 - "$BENCH_DIR" "$RUNS" <<'PY'
import re,statistics,sys,pathlib
p=pathlib.Path(sys.argv[1]);runs=int(sys.argv[2])
print('\nEngine seconds:')
for version in ('v9','v10','v10-person'):
 xs=[]
 for i in range(1,runs+1):
  t=(p/f'{version}-run-{i}.log').read_text()
  values=re.findall(r'native_seconds=([\d.]+)',t)
  if values:xs.append(float(values[-1]))
 print(f'{version:10s}: runs={xs}; median={statistics.median(xs):.5f}s' if xs else f'{version:10s}: no result')
PY
