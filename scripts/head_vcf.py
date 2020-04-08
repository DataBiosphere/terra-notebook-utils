#!/usr/bin/env python
import io
import argparse
from contextlib import closing

import bgzip

parser = argparse.ArgumentParser()
parser.add_argument("cloudpath", help="location of file. Can be local or GCP bucket path (e.g. gs://key)")
args = parser.parse_args()

if args.cloudpath.startswith("gs://"):
    from google.cloud.storage import Client
    import gs_chunked_io as gscio
    cloudpath = args.cloudpath.split("gs://", 1)[1] 
    bucket_name, key = cloudpath.split("/", 1)
    blob = Client().bucket(bucket_name).get_blob(key)
    raw = gscio.Reader(blob)
else:
    raw = open(args.cloudpath, "rb")

with closing(raw):
    with bgzip.BGZipReader(raw) as bgreader:
        with io.BufferedReader(bgreader) as reader:
            for line in reader:
                if line.startswith(b"#"):
                    print(line.decode("ascii").strip())
                else:
                    break
