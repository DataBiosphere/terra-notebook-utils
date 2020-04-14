#!/usr/bin/env python
"""
Merge VCFs stored in google buckets pointed to by `input_keys`.
Output to `output_key` in the same bucket.

merge_vcfs.py "fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10" "combined.vcf.gz" "consent1/HVH_phs000993_TOPMed_WGS_freeze.8.chr7.hg38.vcf.gz,phg001280.v1.TOPMed_WGS_Amish_v4.genotype-calls-vcf.WGS_markerset_grc38.c2.HMB-IRB-MDS/Amish_phs000956_TOPMed_WGS_freeze.8.chr7.hg38.vcf.gz"
"""
import argparse

from terra_notebook_utils import vcf

parser = argparse.ArgumentParser()
parser.add_argument("bucket", type=str, help="Name of GS bucket")
parser.add_argument("output_key", type=str, help="GS bucket location of output file.")
parser.add_argument("input_keys", help="Comma delimited list of GS bucket locations of input files.")
args = parser.parse_args()

keys = args.input_keys.split(",")
vcf.combine(args.bucket, keys, args.bucket, args.output_key)

