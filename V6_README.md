# Velocity Sessions V6 — Recoverable-ID / Fused Native Engine

This is a drop-in update for the existing `velocity-sessions` repository. It **does not** change the required `/data` inputs or `/output` schemas.

## Implementation

- Performs byte-exact CSV validation and transitive linked-ID resolution.
- **Data-derived, seed-independent fast path:** recovers the invertible 40-bit event-ID permutation from a small sample of actual input IDs, with verification across twelve distinct values. No seed is hard-coded. When recovery fails, handles IDs with exact-key numeric radix deduplication and string fallbacks. Unusual IDs remain exact strings even when recovery succeeds.
- For generator-shaped IDs, partitions workers by decoded event index. Uses 8-byte index slots and updates the earliest-ingested record, including **invalid earliest** records which must block later copies.
- Avoids materializing a global session list. Compact, reversible person IDs and person-owned processing groups power fused sessions, daily categories, click credit and assist attribution.
- 24-byte compact event records; direct standard Parquet V1 output, without Python, DuckDB, network or additional system libraries at runtime.

## Tests performed while building

- 36/36 supplied native Parquet fixtures passed.
- 6M-event synthetic dataset: exact V5 match for both Parquet tables.
- Same dataset with generator-style scrambled event IDs: exact V5 match.
- Additional generated-ID adversarial dataset with quoted IDs, malformed lines, later duplicates, earlier invalid timestamps and linked identities: exact V5 match.
- Native Linux 6M scrambled-events benchmarks observed approximately 0.8–1.3 seconds, versus about 2 seconds for V5 on the original 6M synthetic test. Different dataset distributions, machine load and official 20M inputs can change this substantially. This is **not a verified sub-second official score**.

## Installation

Create a new branch from `main`, extract the ZIP in the repository root (`unzip -o velocity-sessions-v6-ultra.zip -d .`), then run:

```sh
bash scripts/test-v6.sh
```

Expected: `36/36 fixtures passed`, `1/1 fixtures passed`, and both reference comparisons `MATCH`.

Keep V5 committed on `main` until these checks pass. After successful tests, commit the new files and merge the branch into main before competition submission.

The additional `tests/v6_probe` is **test data only**, not used by the runtime engine and not included inside the Docker image.
