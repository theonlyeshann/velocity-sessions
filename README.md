# Velocity Sessions — Builders Buildathon / Databricks track

**Shopping Sessions challenge:** a native C++20 analytics engine with DuckDB Parquet serialization. It implements linked customer identities, event cleaning and sessionization, purchase/category aggregation, and 7-day marketing attribution and assists.

## Why not Spark?

The rules explicitly allow alternative engines. For a short, CPU-bound benchmark, avoiding Spark startup and distributed shuffles lets the processing engine use compact in-memory structures and only one ordered pass per person after sorting.

## Architecture

1. Strict CSV parser with exact quoting, field-count and whitespace semantics.
2. Union-find for transitive linked IDs and byte-wise canonical names (including IDs never present in a purchase or click).
3. Earliest-ingest-first event dedup, followed by timestamp/type/price/latency validation.
4. Event sort by `(person, epoch_millis)` and sessionization with an exact 30-minute gap.
5. Session-day purchase categories and revenue aggregations.
6. Click sort by `(person, epoch_millis, click_id)` and a sliding 7-day window. Campaign counts handle per-campaign assists and the latest-click/tie-break attribution.
7. Native engine writes temporary CSV to `/scratch/staging`, and DuckDB writes two typed Parquet files under `/output`.

## Build and submit

From this folder:

```bash
docker buildx build --platform linux/amd64 --load -t velocity-sessions:latest .
```

Run against any generated dataset (the dataset folder must contain `events/`, `clicks/`, `links/`, `products.csv`, `campaigns.csv`, `manifest.json`):

```bash
mkdir -p local-output local-scratch
docker run --rm --network none --read-only --cpus=8 --memory=24g --user=1000:1000 \
  -v "$PWD/dev:/data:ro" -v "$PWD/local-output:/output" -v "$PWD/local-scratch:/scratch" \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch \
  velocity-sessions:latest
```

**Important:** mounted host folders must be writable by uid 1000, or use Docker named volumes. The scorer will provision writable `/output` and `/scratch`.

For fixture checking with the full scorer-like Docker runner, copy `fixtures/` and `scripts/` from the provided PySpark starter into this directory, then run:

```bash
python3 -m pip install polars
make fixtures-docker
```

For a direct local validation of the C++ logic without Docker, run:

```bash
g++ -O3 -std=c++20 -o velocity src/main.cpp
python3 tests/check_csv.py
```

The direct logic test is **not** a replacement for the Parquet/scorer test.

## Dev benchmark

Use the provided Rust generator (Rust >=1.85):

```bash
cd generator
cargo build --release
./target/release/generate --seed 1 --events 1000000 --clicks 250000 --output ../dev
cd ..
```

Run the Docker command above, then compare with the supplied `dev-answer` tables using the provided `scripts/compare.py`, e.g. `python3 scripts/compare.py local-output dev-answer` (see script for expected argument order). The benchmark target is the published board time, not your laptop's time.

## Current verification

- 36 / 36 supplied fixtures **pass** when comparing native intermediate results against the reference CSVs.
- Docker execution, Parquet schemas, full-size generator datasets and leaderboard runtime **still need verification** on a machine with Docker.

## Competition rules / caveats

- The event's earliest well-formed CSV copy wins even if the timestamp or other values are invalid.
- Links are transitive, global and timeless. User ID comparison uses raw UTF-8 bytes, not trimming or case folding.
- Exactly 30 minutes is the **same** session; a 30-minute-plus-1ms gap starts a new one.
- The most recent eligible click wins; if multiple clicks share that timestamp, the byte-wise smallest click ID wins.
- A campaign assists each session at most once, and the credited campaign also counts as an assist.
- Never hardcode manifest dates or input file counts.
