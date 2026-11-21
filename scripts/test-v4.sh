#!/bin/sh
set -eu
printf '\n[1/5] Build linux/amd64 Docker image\n'
docker buildx build --platform linux/amd64 --load -t velocity-sessions:latest .
printf '\n[2/5] Verify all 36 correctness fixtures\n'
make fixtures-docker
if [ ! -f dev/manifest.json ]; then
  echo 'dev/manifest.json is missing. Generate the original dev dataset before running this test.' >&2
  exit 1
fi
if [ ! -d dev-answer ]; then
  echo 'dev-answer/ not found. Extract dev-answer.zip into the project root.' >&2
  exit 1
fi
printf '\n[3/5] Run 1-million-event benchmark in Docker\n'
mkdir -p benchmark-output
docker run --rm --platform linux/amd64 \
  --cpus 8 --memory 24g \
  -v "$(pwd)/dev:/data:ro" \
  -v "$(pwd)/benchmark-output:/output" \
  velocity-sessions:latest
printf '\n[4/5] Compare Parquet output with the reference\n'
python3 scripts/compare.py benchmark-output dev-answer
printf '\n[5/5] Validate restricted read-only non-root environment\n'
mkdir -p local-output local-scratch
docker run --rm --platform linux/amd64 --user 1000:1000 --read-only \
  --cpus 8 --memory 24g --tmpfs /tmp:rw,size=64m \
  -v "$(pwd)/dev:/data:ro" \
  -v "$(pwd)/local-output:/output" \
  -v "$(pwd)/local-scratch:/scratch" \
  velocity-sessions:latest
python3 scripts/compare.py local-output dev-answer
printf '\nV4 TESTS COMPLETE. Confirm both comparison outputs say MATCH.\n'