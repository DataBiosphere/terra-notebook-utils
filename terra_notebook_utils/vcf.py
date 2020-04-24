import io
from uuid import uuid4
from multiprocessing import cpu_count

import bgzip
import gs_chunked_io as gscio

from terra_notebook_utils import gs, xprofile, WORKSPACE_BUCKET


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

    def print_header(self):
        for line in self.header:
            print(line)

    @classmethod
    def with_bgzip_fileobj(cls, fileobj, read_buf: memoryview, chunk_size=1024 * 1024):
        if read_buf is None:
            read_buf = memoryview(bytearray(1024 * 1024 * 50))
        with bgzip.BGZipAsyncReaderPreAllocated(fileobj,
                                                read_buf,
                                                num_threads=cores_available,
                                                raw_read_chunk_size=chunk_size) as bgzip_reader:
            with io.BufferedReader(bgzip_reader) as reader:
                return cls(reader)

    @classmethod
    def with_gzip_fileobj(cls, fileobj, read_buf: memoryview, chunk_size=1024 * 1024):
        import gzip
        gzip_reader = gzip.GzipFile(fileobj=fileobj)
        return cls(gzip_reader)

    @classmethod
    def with_blob(cls, blob, read_buf: memoryview=None):
        chunk_size = 1024 * 1024
        try:
            with gscio.AsyncReader(blob, chunks_to_buffer=1, chunk_size=chunk_size) as raw:
                return cls.with_bgzip_fileobj(raw, read_buf, chunk_size)
        except bgzip.BGZIPException:
            with gscio.AsyncReader(blob, chunks_to_buffer=1, chunk_size=chunk_size) as raw:
                return cls.with_gzip_fileobj(raw, read_buf, chunk_size)

    @classmethod
    def with_file(cls, filepath, read_buf: memoryview=None):
        with open(filepath, "rb") as raw:
            try:
                return cls.with_bgzip_fileobj(raw, read_buf)
            except bgzip.BGZIPException:
                raw.seek(0)
                return cls.with_gzip_fileobj(raw, read_buf)

    @classmethod
    def with_bucket_object(cls, key, bucket_name=WORKSPACE_BUCKET, read_buf: memoryview=None):
        blob = gs.get_client().bucket(bucket_name).get_blob(key)
        return cls.with_blob(blob)


def _headers_equal(a, b):
    for line_a, line_b in zip(a, b):
        if line_a.startswith("##bcftools_viewCommand"):
            # TODO: Include information about which files were combined
            pass
        elif line_a.startswith("##"):
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
