#!/bin/sh
set -eu
printf '\n[1/5] Build native-Parquet linux/amd64 image\n'
docker buildx build --platform linux/amd64 --load -t velocity-sessions:latest .
printf '\n[2/5] Run all 36 Docker fixtures\n'
make fixtures-docker
if [ ! -f dev/manifest.json ] || [ ! -d dev-answer ]; then
  echo 'Missing dev/manifest.json or dev-answer/. Generate the million-event dev dataset and extract dev-answer.zip first.' >&2
  exit 1
fi
printf '\n[3/5] Check 1M-event results\n'
mkdir -p benchmark-output
docker run --rm --platform linux/amd64 \
  -v "$(pwd)/dev:/data:ro" -v "$(pwd)/benchmark-output:/output" \
  velocity-sessions:latest
python3 scripts/compare.py benchmark-output dev-answer
printf '\n[4/5] Test UID1000 and read-only filesystem\n'
mkdir -p local-output local-scratch
chmod 777 local-output local-scratch
docker run --rm --platform linux/amd64 --network none --read-only \
  --user 1000:1000 --cpus 8 --memory 24g \
  -v "$(pwd)/dev:/data:ro" -v "$(pwd)/local-output:/output" \
  -v "$(pwd)/local-scratch:/scratch" velocity-sessions:latest
python3 scripts/compare.py local-output dev-answer
printf '\n[5/5] SUCCESS — verify 36/36 and both MATCH lines before Git push\n'
