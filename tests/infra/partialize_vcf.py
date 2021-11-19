"""Grab the first N lines of a VCF and gzip or bgzip the result."""
import io
import os
import sys
import gzip
from functools import lru_cache
from typing import Iterable

import bgzip

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config  # initialize the test environment

from terra_notebook_utils import drs


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
    blob = drs.blob_for_url(uri)
    with blob.open() as fh:
        if uri.endswith(".gz"):
            with bgzip.BGZipReader(fh) as gzip_reader:
                return [line for _, line in zip(range(number_of_lines), io.BufferedReader(gzip_reader))]
        else:
            return [line for _, line in zip(range(number_of_lines), io.BufferedReader(fh))]
