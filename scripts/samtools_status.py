#!/usr/bin/env python
import io
import os
import sys
import argparse

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from terra_notebook_utils import samtools

for key, val in samtools.available.items():
    print(f"{key} is available:", val)
