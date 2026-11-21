# Velocity V8 experimental linked-ID fast path

This is a **performance experiment**, not yet an official leaderboard winner. It replaces only `src/main.cpp` and introduces `scripts/bench-v8-v7.sh`. All other project files remain V7.

## Changes
- Memory-map links and parse exact two-field CSV using `string_view`, avoiding one heap string per link field.
- Replace the ordinary user-ID `unordered_map` in the DSU with a compact open-addressed flat numeric index. The exact-string fallback retains unusual IDs, uppercase variants, shared devices, and malformed input semantics.
- Store canonical component names as non-owning views into mapped link files (mapped memory remains valid through final output).
- Preserve V7 event-ID, click-ID, session, and Parquet algorithms exactly.

## Local validation
Compared with the pinned V7 executable on 36 supplied fixtures plus the V6 and V7 adversarial probes: **38/38 cases produced byte-identical Parquet**. A 5M-event synthetic data sample also produced byte-identical output. This synthetic sample has very few links and is **not predictive** of the full 20M-generator workload.

## Test on the native Google Cloud VM

1. Do **not** overwrite your V7 `main`. Create a new branch and unzip the drop-in at repository root:

```bash
git switch main
git switch -c v8-identity-experiment
unzip -o ~/velocity-v8-identity-candidate.zip -d .
```

2. With `dev20m` still in your project root, benchmark both Docker images under 8 vCPUs/24 GiB:

```bash
DATASET=dev20m RUNS=3 bash scripts/bench-v8-v7.sh
```

The script builds V7 directly from immutable commit `3087f8266d3d6b340696a95160f6d88300041820`, builds V8 separately, records all profiles, runs restricted nonroot containers, and checks **byte-identical output**. If it fails or V8 is slower, stay on V7. No GitHub push is needed to benchmark.

**Benchmark caveat:** The first run may be affected by cold filesystem cache. Compare repeated runs and medians; only use official 20M evaluation for contest score.
