import os
import sys
import subprocess

def _run(cmd: list, **kwargs):
    p = subprocess.run(cmd, **kwargs)
    p.check_returncode()
    return p

bin_root = os.path.abspath(os.path.dirname(sys.executable))
htsfile_path = os.path.join(bin_root, "htsfile")
bcftools_path = os.path.join(bin_root, "bcftools")
bcftools_available = os.path.isfile(bcftools_path)

try:
    p = _run([htsfile_path, "--version"], capture_output=True)
    htsfile_available = True
except (FileNotFoundError, subprocess.CalledProcessError):
    htsfile_available = False

try:
    p = _run([bcftools_path, "--version"], capture_output=True)
    bcftools_available = True
except (FileNotFoundError, subprocess.CalledProcessError):
    bcftools_available = False
