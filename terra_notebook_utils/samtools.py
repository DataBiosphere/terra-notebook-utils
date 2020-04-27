import os
import sys
import subprocess

paths = dict(htsfile=None, bcftools=None)

def _run(cmd: list, **kwargs):
    p = subprocess.run(cmd, **kwargs)
    p.check_returncode()
    return p

def _samtools_binary_path(name):
    roots = [
        os.path.abspath(os.path.dirname(sys.executable)),  # typical bin path in a virtual env
        "/home/jupyter-user/.local/bin"  # terra installation path
    ]
    for root in roots:
        path = os.path.join(root, name)
        try:
            _run([path, "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return path
        except (FileNotFoundError, subprocess.CalledProcessError):
            pass
    return None

for name in paths:
    paths[name] = _samtools_binary_path(name)
    if paths[name] is None:
        print(f"WARNING: {name} unavailable: htslib or bcftools build failed during installation. "
              "          try `pip install -v terra-notebook-utils` to diagnose the problem")

available = {name: paths[name] is not None for name in paths}
