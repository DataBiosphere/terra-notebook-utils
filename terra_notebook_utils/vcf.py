"""
VCF file utilities
"""
import io
import gzip
from multiprocessing import cpu_count
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import bgzip
import gs_chunked_io as gscio
from gs_chunked_io.async_collections import AsyncQueue

from terra_notebook_utils import gs, drs, IO_CONCURRENCY, WORKSPACE_GOOGLE_PROJECT


cores_available = cpu_count()

class VCFInfo:
    columns = ["chrom", "pos", "id", "ref", "alt", "qual", "filter", "info", "format"]
    chromosomes = [f"chr{i}" for i in range(1, 23)] + ["chrX", "chrY"]

    def __init__(self, fileobj):
        self.header = list()
        for line in fileobj:
            line = line.decode("utf-8").strip()
            if line.startswith("##"):
                self.header.append(line)
            elif line.startswith("#"):
                self.header.append(line)
                self.samples = line.split("\t")[len(self.columns):]
            elif not line:
                continue
            else:
                first_data_line = line
                break
        parts = first_data_line.split("\t", len(self.columns))
        for key, val in zip(self.columns + ["data"], parts):
            setattr(self, key, val)

    @property
    def length(self):
        for line in self.header:
            if "length" in line and f"{self.chrom}," in line:
                length = line.rsplit("=", 1)[-1].replace(">", "")
                return int(length)
        return None

    def print_header(self):
        for line in self.header:
            print(line)

    @classmethod
    def with_bgzip_fileobj(cls, fileobj, read_buf: Optional[memoryview], chunk_size=1024 * 1024):
        if read_buf is None:
            read_buf = memoryview(bytearray(1024 * 1024 * 50))
        with bgzip.BGZipAsyncReaderPreAllocated(fileobj,
                                                read_buf,
                                                num_threads=cores_available,
                                                raw_read_chunk_size=chunk_size) as bgzip_reader:
            with io.BufferedReader(bgzip_reader) as reader:
                return cls(reader)

    @classmethod
    def with_gzip_fileobj(cls, fileobj):
        gzip_reader = gzip.GzipFile(fileobj=fileobj)
        return cls(gzip_reader)

    @classmethod
    def with_blob(cls, blob, read_buf: Optional[memoryview]=None):
        chunk_size = 1024 * 1024
        with ThreadPoolExecutor(max_workers=IO_CONCURRENCY) as e:
            async_queue = AsyncQueue(e, IO_CONCURRENCY)
            try:
                with gscio.Reader(blob, chunk_size, async_queue) as raw:
                    return cls.with_bgzip_fileobj(raw, read_buf, chunk_size)
            except bgzip.BGZIPException:
                with gscio.Reader(blob, chunk_size, async_queue) as raw:
                    return cls.with_gzip_fileobj(raw)

    @classmethod
    def with_file(cls, filepath, read_buf: memoryview=None):
        with open(filepath, "rb") as raw:
            try:
                return cls.with_bgzip_fileobj(raw, read_buf)
            except bgzip.BGZIPException:
                raw.seek(0)
                return cls.with_gzip_fileobj(raw)

def vcf_info(uri: str,
             workspace_namespace: Optional[str]=WORKSPACE_GOOGLE_PROJECT) -> VCFInfo:
    if uri.startswith("drs://"):
        client, drs_info = drs.resolve_drs_for_gs_storage(uri)
        blob = client.bucket(drs_info.bucket_name, user_project=workspace_namespace).get_blob(drs_info.key)
        return VCFInfo.with_blob(blob)
    elif uri.startswith("gs://"):
        bucket, key = uri[5:].split("/", 1)
        blob = gs.get_client().bucket(bucket, user_project=workspace_namespace).get_blob(key)
        return VCFInfo.with_blob(blob)
    elif uri.startswith("s3://"):
        raise ValueError("S3 URIs not supported")
    else:
        return VCFInfo.with_file(uri)

def _headers_equal(a, b):
    for line_a, line_b in zip(a, b):
        if line_a.startswith("##bcftools_viewCommand"):
            # TODO: Include information about which files were combined
            pass
        elif line_a.startswith("##"):
            if line_a != line_b:
                return False
    return True
