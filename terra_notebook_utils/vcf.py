import io
import os
import time
from uuid import uuid4
from contextlib import AbstractContextManager
from multiprocessing import Process, cpu_count

import bgzip
import gs_chunked_io as gscio

from terra_notebook_utils import gs, xprofile


cores_available = cpu_count()


class BlobReaderProcess(AbstractContextManager):
    def __init__(self, bucket_name, key):
        self.filepath = f"/tmp/{uuid4()}.vcf.bgz"
        os.mkfifo(self.filepath)
        self.proc = Process(target=self.run, args=(bucket_name, key, self.filepath))
        self.proc.start()
        self._closed = False

    def run(self, bucket_name, key, filepath):
        fd = os.open(filepath, os.O_WRONLY)
        blob = gs.get_client().bucket(bucket_name).get_blob(key)
        blob_reader = gscio.AsyncReader(blob, chunks_to_buffer=1)
        while True:
            data = bytearray(blob_reader.read(blob_reader.chunk_size))
            if not data:
                break
            while data:
                try:
                    k = os.write(fd, data)
                    data = data[k:]
                except BrokenPipeError:
                    time.sleep(1)

    def close(self):
        if not self._closed:
            self._closed = True
            self.proc.join(1)
            os.unlink(self.filepath)
            if not self.proc.exitcode:
                self.proc.terminate()

    def __exit__(self, *args, **kwargs):
        self.close()


class BlobWriterProcess(AbstractContextManager):
    def __init__(self, bucket_name, key):
        self.filepath = f"/tmp/{uuid4()}.vcf.bgz"
        os.mkfifo(self.filepath)
        self.proc = Process(target=self.run, args=(bucket_name, key, self.filepath))
        self.proc.start()
        self._closed = False

    def run(self, bucket_name, key, filepath):
        bucket = gs.get_client().bucket(bucket_name)
        with gscio.Writer(key, bucket) as blob_writer:
            with open(filepath, "rb") as fh:
                while True:
                    data = fh.read(blob_writer.chunk_size)
                    if not data:
                        break
                    blob_writer.write(data)

    def close(self):
        if not self._closed:
            self._closed = True
            self.proc.join(300)
            os.unlink(self.filepath)
            if not self.proc.exitcode:
                self.proc.terminate()

    def __exit__(self, *args, **kwargs):
        self.close()


class VCFInfo:
    columns = ["chrom", "pos", "id", "ref", "alt", "qual", "filter", "info", "format"]
    chromosomes = [f"chr{i}" for i in range(1, 23)] + ["chrX", "chrY"]

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


def _headers_equal(a, b):
    for line_a, line_b in zip(a, b):
        if line_a.startswith("##bcftools_viewCommand"):
            # TODO: Include information about which files were combined
            pass
        else:
            if line_a != line_b:
                return False
    return True


@xprofile.profile()
def prepare_merge_workflow_input(table_name, prefixes, output_pfx):
    from terra_notebook_utils import table, WORKSPACE_BUCKET

    bucket = gs.get_client().bucket(WORKSPACE_BUCKET)
    vcfs_by_chrom = {chrom: list() for chrom in VCFInfo.chromosomes}
    names_by_chrom = {chrom: list() for chrom in VCFInfo.chromosomes}
    read_buf = memoryview(bytearray(1024 * 1024 * 50))
    for pfx in prefixes:
        for item in bucket.list_blobs(prefix=pfx):
            print("Inspecting", item.name)
            vcf = VCFInfo.with_blob(item, read_buf=read_buf)
            chrom = vcf.chrom
            if vcf not in vcfs_by_chrom[chrom]:
                if len(vcfs_by_chrom[chrom]):
                    assert _headers_equal(vcf.header, vcfs_by_chrom[chrom][0].header)
                vcfs_by_chrom[chrom].append(vcf)
                names_by_chrom[chrom].append(item.name)
            else:
                raise Exception("Two chromosomes in the same vcf?")

    tsv = "\t".join([f"entity:{table_name}_id", "bucket", "output_key", "input_keys"])
    for chrom, names in names_by_chrom.items():
        if names:
            output_key = f"{output_pfx}/{chrom}.vcf.bgz"
            input_keys = ",".join(names)
            tsv += "\r" + "\t".join([f"{uuid4()}", WORKSPACE_BUCKET, output_key, input_keys])

    table.delete_table(f"{table_name}_set")  # This table is produced by workflows and must be deleted first
    table.delete_table(f"{table_name}")

    table.upload_entities(tsv)
