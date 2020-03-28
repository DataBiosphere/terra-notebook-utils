import io
import os
from collections import namedtuple
import pandas as pd
from uuid import uuid4

import gs_chunked_io as gscio
import bgzip

from terra_notebook_utils import gs, table, progress, WORKSPACE_BUCKET


number_of_reader_threads = 4
number_of_writer_threads = 8
writer_batch_size = 20000


tab = "\t".encode("ascii")
linesep = os.linesep.encode("ascii")


class VCFRow(namedtuple("VCFRow", "chrom pos vid ref alt qual filt info fmt samples")):
    __slots__ = ()

    def parse_info(self, name, cast: type=None):
        val = self.info.split(name + b"=", 1)[1].split(b";", 1)[0]
        if cast:
            return cast(val)
        else:
            return val

    def replace_info(self, name, newval: bytes):
        first_part, last_part = self.info.split(name + b"=", 1)
        last_part = last_part.split(b";", 1)[1]
        new_info = first_part + name + b"=" + newval + b";" + last_part
        return type(self)(self.chrom,
                          self.pos,
                          self.vid,
                          self.ref,
                          self.alt,
                          self.qual,
                          self.filt,
                          new_info,
                          self.fmt,
                          self.samples)

    @classmethod
    def with_line(cls, line):
        return cls(*line.strip().split(tab, 9))


class VCFFile:
    columns = [b"#CHROM", b"POS", b"ID", b"REF", b"ALT", b"QUAL", b"FILTER", b"INFO", b"FORMAT"]
    chromosomes = [b"chr1", b"chr2", b"chr3", b"chr4", b"chr5", b"chr6", b"chr7", b"chr8", b"chr9", b"chr10", b"chr11",
                   b"chr12", b"chr13", b"chr14", b"chr15", b"chr16", b"chr17", b"chr18", b"chr19", b"chr20", b"chr21",
                   b"chr22", b"chrX"]

    def __init__(self, fileobj):
        self._closed = False
        self._fileobj = fileobj
        self._parse_header()
        self._first_data_line = next(self._fileobj)
        self.first_row = VCFRow.with_line(next(self._fileobj))
        self._did_yield_first_row = False

    def _parse_header(self):
        self.header = list()
        for line in self._fileobj:
            line = line.strip()
            if not line:
                continue
            elif line.startswith(b"##"):
                self.header.append(line)
            else:
                column_headers_line = line
                break
        self.column_names = column_headers_line.split(tab)
        assert self.column_names[:9] == self.columns
        self.sample_names = self.column_names[9:]
        self.number_of_samples = len(self.sample_names)

    def __iter__(self):
        if not self._did_yield_first_row:
            self._did_yield_first_row = True
            yield self.first_row
        for line in self._fileobj:
            yield VCFRow.with_line(line)

    def get_rows(self, number_of_rows):
        rows = [None] * number_of_rows
        row_iter = iter(self)
        for i in range(number_of_rows):
            try:
                rows[i] = next(row_iter)
            except StopIteration:
                del rows[i:]
                break
        return rows

    def close(self):
        if not self._closed:
            self._closed = True
            self._fileobj.close()
            self._fileobj = None


class _Combiner:
    def __init__(self, vcfs, out_fileobj):
        self.vcfs = vcfs
        self.out_fileobj = out_fileobj
        self._assemble_header()
        sample_names = list()
        for vcf in vcfs:
            sample_names.extend(vcf.sample_names)
        self.number_of_samples = len(sample_names)
        self.header_lines.append(tab.join(VCFFile.columns + sample_names))
        self._an_strings = [f"AN={2 * vcf.number_of_samples}".encode("ascii") for vcf in self.vcfs]
        self._combined_an_string = f"AN={2 * self.number_of_samples}".encode("ascii")

        self._an_info_in_rows = True
        self._ac_info_in_rows = True

    def _assemble_header(self):
        self.header_lines = list()
        for lines in zip(*[vcf.header for vcf in self.vcfs]):
            if lines[0].startswith(b"##bcftools_viewCommand"):
                # TODO: Include information about which files were combined
                self.header_lines.append(b"##terra-notebook-utils vcf combination")
            else:
                for line in lines[1:]:
                    if line != lines[0]:
                        raise ValueError("Headers not equal")
                self.header_lines.append(lines[0])

    def combine_rows(self, rows):
        new_row = rows[0]
        if self._ac_info_in_rows:
            try:
                number_of_allels = sum([r.parse_info(b"AC", int) for r in rows])
                new_row = new_row.replace_info(b"AC", f"{number_of_allels}".encode("ascii"))
            except IndexError:
                self._ac_info_in_rows = False
        if self._an_info_in_rows:
            try:
                new_row = new_row.replace_info(b"AN", f"{2 * self.number_of_samples}".encode("ascii"))
            except ValueError:
                self._an_info_in_rows = False
        return tab.join(new_row + tuple(row.samples for row in rows[1:]))

    def start(self):
        count = 0
        with progress.ProgressReporter() as pr:
            with bgzip.AsyncBGZipWriter(self.out_fileobj, writer_batch_size, number_of_reader_threads) as writer:
                writer.write(linesep.join(self.header_lines))
                for rows in zip(*self.vcfs):
                    combined_line = linesep + self.combine_rows(rows)
                    writer.write(combined_line)
                    count += 1
                    if not count % 1000:
                        pr.checkpoint(1000)
        pr.checkpoint(0)


def vcf_for_blob(blob,
                 bgizp_read_buf=memoryview(bytearray(1024 * 1024 * 1000)),
                 gs_read_chunk_size=1024 * 1024 * 2,
                 bgzip_raw_read_chunk_size=1024 * 1024,
                 buffered_reader_chunk_size=1024 * 1024):
    raw = gscio.AsyncReader(blob, chunks_to_buffer=1, chunk_size=gs_read_chunk_size)
    gzip_reader = bgzip.BGZipAsyncReaderPreAllocated(raw,
                                                     bgizp_read_buf,
                                                     num_threads=number_of_reader_threads,
                                                     raw_read_chunk_size=1024 * 1024)
    return VCFFile(io.BufferedReader(gzip_reader, 1024 * 1024))


def vcf_files_for_blobs(blobs):
    vcfs = list()
    for blob in blobs:
        raw = gscio.AsyncReader(blob, chunks_to_buffer=1, chunk_size=2 * 1024 * 1024)
        buf = memoryview(bytearray(1024 * 1024 * 1000))
        gzip_reader = bgzip.BGZipAsyncReaderPreAllocated(raw,
                                                         buf,
                                                         num_threads=number_of_reader_threads,
                                                         raw_read_chunk_size=1024 * 1024)
        vcfs.append(VCFFile(io.BufferedReader(gzip_reader, 1024 * 1024)))
    return vcfs


def combine(blobs, dst_bucket_name, dst_key):
    vcfs = vcf_files_for_blobs(blobs)
    client = gs.get_client()
    bucket = client.bucket(dst_bucket_name)
    with gscio.AsyncWriter(dst_key, bucket, concurrent_uploads=2) as fh:
        c = _Combiner(vcfs, fh)
        c.start()


def combine_local(blobs, filepath):
    vcfs = vcf_files_for_blobs(blobs)
    with open(filepath, "wb") as fh:
        c = _Combiner(vcfs, fh)
        c.start()


def _headers_equal(a, b):
    for line_a, line_b in zip(a, b):
        if line_a.startswith(b"##bcftools_viewCommand"):
            # TODO: Include information about which files were combined
            pass
        else:
            if line_a != line_b:
                return False
    return True

from terra_notebook_utils import xprofile

@xprofile.profile()
def prepare_merge_workflow_input(prefixes, output_pfx="merged"):
    bucket = gs.get_client().bucket(WORKSPACE_BUCKET)
    bgzip_read_buffer = memoryview(bytearray(1024 * 1024 * 50))
    vcfs_by_chrom = {chrom: list() for chrom in VCFFile.chromosomes}
    names_by_chrom = {chrom: list() for chrom in VCFFile.chromosomes}
    for pfx in prefixes:
        for item in bucket.list_blobs(prefix=pfx):
            print("Inspecting", item.name)
            vcf = vcf_for_blob(item,
                               bgizp_read_buf=bgzip_read_buffer,
                               gs_read_chunk_size=1024 * 1024,
                               bgzip_raw_read_chunk_size=1024 * 5,
                               buffered_reader_chunk_size=1024 * 5)
            vcf.close()
            chrom = vcf.first_row.chrom
            if vcf not in vcfs_by_chrom[chrom]:
                if len(vcfs_by_chrom[chrom]):
                    assert _headers_equal(vcf.header, vcfs_by_chrom[chrom][0].header)
                vcfs_by_chrom[chrom].append(vcf)
                names_by_chrom[chrom].append(item.name)
            else:
                raise Exception("Two chromosomes in the same vcf? No bueno")

    data = list()
    for chrom in names_by_chrom:
        output_key = output_pfx + chrom.decode("ascii") + ".vcf.gz"
        data.append([str(uuid4()), WORKSPACE_BUCKET, output_key, ",".join(names_by_chrom[chrom])])
    df = pd.DataFrame(data, columns=["entity:merge_vcf_id", "bucket", "output_key", "input_keys"])

    ents = [dict(entityType=e['entityType'], entityName=e['name'])
            for e in table.list_entities("merge_vcf")]
    table.delete_entities(ents)

    out_data = df.to_csv(sep="\t", index=False)
    with open("out.csv", "w") as fh:
        fh.write(out_data)
    table.upload_entities(out_data)
