"""VCF file utilities."""
import io
import gzip
from multiprocessing import cpu_count
from typing import Optional

import bgzip

from terra_notebook_utils.blobstore import Blob


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
                                                num_threads=cpu_count(),
                                                raw_read_chunk_size=chunk_size) as bgzip_reader:
            with io.BufferedReader(bgzip_reader) as reader:
                return cls(reader)

    @classmethod
    def with_gzip_fileobj(cls, fileobj):
        gzip_reader = gzip.GzipFile(fileobj=fileobj)
        return cls(gzip_reader)

    @classmethod
    def with_blob(cls, blob: Blob, read_buf: Optional[memoryview]=None):
        try:
            with blob.open() as raw:
                chunk_size = 1024 * 1024
                with io.BufferedReader(raw) as fh:
                    return cls.with_bgzip_fileobj(fh, read_buf, chunk_size)
        except bgzip.BGZIPException:
            with blob.open() as raw:
                with io.BufferedReader(raw) as fh:
                    return cls.with_gzip_fileobj(fh)

def _headers_equal(a, b):
    for line_a, line_b in zip(a, b):
        if line_a.startswith("##bcftools_viewCommand"):
            # TODO: Include information about which files were combined
            pass
        elif line_a.startswith("##"):
            if line_a != line_b:
                return False
    return True
