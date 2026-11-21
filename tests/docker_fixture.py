"""Adapter for the official fixture runner: runs one case under scorer-like Docker limits."""
import os
import subprocess
import sys
import tempfile
from pathlib import Path

input_dir, output_dir = map(lambda x: Path(x).resolve(), sys.argv[-2:])
# The official checker creates private output dirs; uid 1000 needs write access.
os.chmod(output_dir, 0o777)
with tempfile.TemporaryDirectory() as temp:
    scratch = Path(temp)
    os.chmod(scratch, 0o777)
    cmd = [
        'docker', 'run', '--rm', '--network', 'none', '--read-only',
        '--cpus', '8', '--memory', '24g', '--user', '1000:1000',
        '-v', f'{input_dir}:/data:ro',
        '-v', f'{output_dir}:/output',
        '-v', f'{scratch}:/scratch',
        '-e', 'DATA_DIR=/data', '-e', 'OUTPUT_DIR=/output',
        '-e', 'SCRATCH_DIR=/scratch', 'velocity-sessions:latest'
    ]
    sys.exit(subprocess.call(cmd))
