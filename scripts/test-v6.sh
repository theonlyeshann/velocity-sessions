#!/bin/sh
set -eu
printf '\n[1/6] Build V6 linux/amd64 Docker image\n'
docker buildx build --platform linux/amd64 --load -t velocity-sessions:latest .
printf '\n[2/6] Verify all 36 official correctness fixtures\n'
make fixtures-docker
printf '\n[3/6] Test generator-aware dedup + cross-file malformed duplicates\n'
python3 scripts/run_fixtures.py tests/v6_probe -- python3 tests/docker_fixture.py
if [ ! -f dev/manifest.json ] || [ ! -d dev-answer ]; then
  echo 'Missing dev/manifest.json or dev-answer/; these are required for the 1M reference test.' >&2
  exit 1
fi
printf '\n[4/6] Verify 1-million-event reference results\n'
mkdir -p benchmark-output
docker run --rm --platform linux/amd64 --cpus 8 --memory 24g \
  -v "$(pwd)/dev:/data:ro" -v "$(pwd)/benchmark-output:/output" \
  velocity-sessions:latest
python3 scripts/compare.py benchmark-output dev-answer
printf '\n[5/6] Verify UID1000, no network, and read-only root\n'
mkdir -p local-output local-scratch
chmod 777 local-output local-scratch
docker run --rm --platform linux/amd64 --network none --read-only \
  --user 1000:1000 --cpus 8 --memory 24g \
  -v "$(pwd)/dev:/data:ro" -v "$(pwd)/local-output:/output" \
  -v "$(pwd)/local-scratch:/scratch" \
  velocity-sessions:latest
python3 scripts/compare.py local-output dev-answer
printf '\n[6/6] V6 VALIDATED: 36 official fixtures, 1 adversarial probe, 2 reference MATCH checks\n'
