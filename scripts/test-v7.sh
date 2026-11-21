#!/bin/sh
set -eu
printf '\n[1/6] Build V7 Atomic-Radix linux/amd64 Docker image\n'
docker buildx build --platform linux/amd64 --load -t velocity-sessions:latest .
printf '\n[2/6] Run 36 official correctness fixtures\n'
make fixtures-docker
printf '\n[3/6] Verify V6 generator-key adversarial probe\n'
python3 scripts/run_fixtures.py tests/v6_probe -- python3 tests/docker_fixture.py
printf '\n[4/6] Verify V7 early-invalid-duplicate adversarial probe\n'
python3 scripts/run_fixtures.py tests/v7_overflow -- python3 tests/docker_fixture.py
if [ ! -f dev/manifest.json ] || [ ! -d dev-answer ]; then
  echo 'The 1M reference requires dev/manifest.json and dev-answer/ in the project root.' >&2
  exit 1
fi
printf '\n[5/6] Verify 1-million-event reference output against expected tables\n'
mkdir -p benchmark-output
rm -f benchmark-output/user_daily/*.parquet benchmark-output/campaign_daily/*.parquet
# Output folders may not exist yet; avoid failing on missing glob paths.
docker run --rm --platform linux/amd64 --cpus 8 --memory 24g \
  -v "$(pwd)/dev:/data:ro" -v "$(pwd)/benchmark-output:/output" \
  velocity-sessions:latest
python3 scripts/compare.py benchmark-output dev-answer
printf '\n[6/6] Verify UID1000, no network, and read-only root\n'
mkdir -p local-output local-scratch
rm -f local-output/user_daily/*.parquet local-output/campaign_daily/*.parquet
chmod 777 local-output local-scratch
docker run --rm --platform linux/amd64 --network none --read-only \
  --user 1000:1000 --cpus 8 --memory 24g \
  -v "$(pwd)/dev:/data:ro" -v "$(pwd)/local-output:/output" \
  -v "$(pwd)/local-scratch:/scratch" \
  velocity-sessions:latest
python3 scripts/compare.py local-output dev-answer
printf '\nV7 VALIDATED — 36/36 official, 2 adversarial probes, 2 reference MATCH checks\n'
