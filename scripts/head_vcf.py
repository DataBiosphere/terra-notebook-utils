#!/usr/bin/env python
import io
import os
import sys
import argparse

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from terra_notebook_utils import vcf

parser = argparse.ArgumentParser()
parser.add_argument("cloudpath", help="location of file. Can be local or GCP bucket path (e.g. gs://key)")
args = parser.parse_args()

if args.cloudpath.startswith("gs://"):
    cloudpath = args.cloudpath.split("gs://", 1)[1] 
    bucket_name, key = cloudpath.split("/", 1)
    info = vcf.VCFInfo.with_bucket_object(key, bucket_name)
else:
    info = vcf.VCFInfo.with_file(args.cloudpath)

info.print_header()
