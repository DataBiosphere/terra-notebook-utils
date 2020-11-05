"""
Grab the first N lines of a VCF and gzip or bgzip the result
"""
import io
import os
import sys
import gzip
from contextlib import closing
from functools import lru_cache
from typing import Iterable

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import bgzip
from bgzip.bgzip_utils import BGZIPMalformedHeaderException
import gs_chunked_io as gscio

from terra_notebook_utils import gs, drs, WORKSPACE_GOOGLE_PROJECT


def partialize_vcf(uri: str, number_of_lines: int, zip_format: str="bgzip") -> bytes:
    with io.BytesIO() as raw:
        if "bgzip" == zip_format:
            zip_writer = bgzip.BGZipWriter(raw)
        elif "gzip" == zip_format:
            zip_writer = gzip.GzipFile(fileobj=raw, mode="w")
        else:
            raise ValueError("Supported values for `zip_format` are 'bgzip' and 'gzip'")
        with zip_writer as writer:
            for line in _vcf_lines(uri, number_of_lines):
                writer.write(line)
        out = raw.getvalue()
    return out

@lru_cache()
def _vcf_lines(uri: str, number_of_lines: int) -> Iterable[bytes]:
    with closing(_get_fileobj(uri)) as fh:
        if uri.endswith(".gz"):
            with bgzip.BGZipReader(fh) as gzip_reader:
                return [line for _, line in zip(range(number_of_lines), io.BufferedReader(gzip_reader))]
        else:
            return [line for _, line in zip(range(number_of_lines), io.BufferedReader(fh))]

def _get_fileobj(uri: str):
    if uri.startswith("gs://"):
        bucket_name, key = uri[5:].split("/", 1)
        blob = gs.get_client().bucket(bucket_name, user_project=WORKSPACE_GOOGLE_PROJECT).get_blob(key)
        fh = gscio.Reader(blob, chunk_size=1024 ** 2)
    elif uri.startswith("drs://"):
        gs_client, drs_info = drs.resolve_drs_for_gs_storage(uri)
        bucket = gs_client.bucket(drs_info.bucket_name, user_project=WORKSPACE_GOOGLE_PROJECT)
        fh = gscio.Reader(bucket.get_blob(drs_info.key), chunk_size=1024 ** 2)
    else:
        fh = open(uri, "rb")
    return fh
