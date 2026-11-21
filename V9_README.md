# Velocity V9: Event CSV Fast Path (experimental)

This is a narrow **V8-based experiment**, not a claimed winning build. Do not replace or merge the V8 checkpoint until native benchmarks and correctness checks pass.

## What changed

1. `csv_split7` uses a single quote check for normal unquoted event rows, then scans only six commas. Quoted rows and unusual CSV quoting still run through the unchanged V8 parser. This preserves the original exact parser semantics.
2. The direct-indexed event winner loop now validates the already-decoded CSV fields rather than decoding the same winner's seven fields again. Dedup precedence remains unchanged: the earliest ingest wins even if invalid.

No changes to linked identity logic, session rules, attribution, click paths, or Parquet writer.

## Validation performed

Compared the unchanged V8 executable with V9 on all **36 official fixtures** plus **V6 generator-key** and **V7 early-invalid-duplicate** adversarial fixtures (**38/38 matched**). The generated Parquet files were byte-identical for every comparison. These are correctness checks, not a substitute for the full 20M generator benchmark or 60M final-scale testing.

## Install and A/B benchmark on native `velocity-bench`

From the project root with the committed `v8-identity-experiment` branch and `dev20m` already present:

```bash
git switch v8-identity-experiment
git switch -c v9-fast-csv-experiment
unzip -o ~/velocity-v9-fast-csv-candidate.zip -d .
DATASET=dev20m RUNS=3 bash scripts/bench-v9-v8.sh
```

The script creates separate Docker images for committed V8 and candidate V9, executes three 8-thread runs each with 8 CPU/24 GiB limits, and checks byte-identical Parquet output. It uses `sudo` only to delete old UID-1000-owned benchmark output before each run. The existing V8 branch and its source are not modified.

**Decision rule:** Promote V9 only if the 20M native median is consistently faster than V8, output matches exactly, and the full correctness/robustness suite also passes under the final container constraints. If not, discard this experiment and move to a more significant dedup + identity optimization.
