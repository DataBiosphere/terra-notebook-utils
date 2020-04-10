import io
from multiprocessing import cpu_count

import bgzip
import gs_chunked_io as gscio


cores_available = cpu_count()


class VCFInfo:
    columns = ["chrom", "pos", "id", "ref", "alt", "qual", "filter", "info", "format"]
    chromosomes = [f"chr{i}" for i in range(1, 23)] + ["chrX"]

    def __init__(self, fileobj):
        self.header = list()
        for line in fileobj:
            line = line.decode("utf-8").strip()
            if line.startswith("##"):
                self.header.append(line)
            elif not line or line.startswith("#"):
                continue
            else:
                first_data_line = line
                break
        parts = first_data_line.split("\t", len(self.columns))
        for key, val in zip(self.columns + ["data"], parts):
            setattr(self, key, val)

    @classmethod
    def with_blob(cls, blob, read_buf: memoryview):
        chunk_size = 1024 * 1024
        with gscio.AsyncReader(blob, chunks_to_buffer=1, chunk_size=chunk_size) as raw:
            with bgzip.BGZipAsyncReaderPreAllocated(raw,
                                                    read_buf,
                                                    num_threads=cores_available,
                                                    raw_read_chunk_size=chunk_size) as bgzip_reader:
                with io.BufferedReader(bgzip_reader) as reader:
                    return cls(reader)
