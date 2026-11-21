# Velocity V10 — Fused Event Materialization + Canonical-Key Caching

Experimental candidate based on V9 commit `8cafe9b56bd73400e22c27ce0044212d8b0256f1`.

## Why it can be faster

V9 first creates `PendingEvent` objects, then traverses *all winning events again* to resolve user identity and write compact `Event` records. V10 emits the compact `Event` directly while validating the deduplicated winner, removing the intermediate object array and one complete traversal.

During linked-ID construction V10 keeps the exact union-find graph. Once the graph is final, it rewrites the existing 16-byte open-address hash-table payload for each compact user ID to the precomputed canonical packed ID, or a root index for a noncompact name. This removes repeated link-parent chasing and a second base36 parse from normal event and click processing. Unlinked IDs still retain exact identity. Unusual or case-variant user IDs still pass through the exact-string fallback.

**V10-person** is an optional experimental sorting strategy. It first radix-groups events by canonical person then sorts timestamps only *within* each person's group, rather than running an 8-byte timestamp radix over the entire event array. Very active users get local radix sorting. This strategy is not assumed faster: the VM A/B/C script will establish the result.

## Correctness evidence and limitations

- **38/38** tests (36 official fixtures, generator-key adversarial probe, and early-invalid-duplicate probe) gave byte-identical Parquet outputs to the unchanged V9 binary for *both* candidate variants.
- A separate **2M-event, 500K-click synthetic workload** also produced identical outputs between V10 variants and V9. This is **not** the published generator and should not be treated as a leaderboard speed prediction.
- The official 20M/60M generator-based runtime and the scorer result are **not yet known**. Never submit before the native benchmark and exact-output check pass.
- Preserve the earliest-ingest duplicate winner before validation; never infer connectivity from a common numerical user index. V10 follows these requirements.

## Install on the cloud VM

From the current V9 branch:

```bash
git switch v9-fast-csv-experiment
git switch -c v10-dense-identity-experiment
unzip -o ~/velocity-v10-dense-identity-candidate.zip -d .
DATASET=dev20m RUNS=3 bash scripts/bench-v10-v9.sh
```

The benchmark compiles three separate images and validates exact Parquet equality. It requires native AMD64, Docker and sufficient disk. It does not alter the V9 commit or upload any result. The best candidate variant can be selected for the next submission after the benchmark. The default `src/main.cpp` is V10 (fused + cached canonical key, original V9 sort). The variant in `experiments/v10-person-sort.cpp` adds the person-local sort.

**Do not run `git add .`** in the VM because this directory contains multi-GB input and benchmark outputs. Commit `src/main.cpp`, the experiment file, benchmark script, and this README only after validating.
