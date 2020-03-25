import io
import os
import time
import signal
import queue
from functools import wraps
from multiprocessing import Process, Pipe, Queue, Array
from ctypes import c_char_p
import numpy as np
from concurrent.futures import ProcessPoolExecutor, as_completed

import bgzip

from terra_notebook_utils import vcf


strlen = 10000
arr_sz = 400000
chunk_size = 8000
arr_base = None


tab = "\t".encode("ascii")
linesep = os.linesep.encode("ascii")


def create_base(number_of_columns):
    global arr_base
    arr_base = Array("c", strlen * arr_sz * number_of_columns)


def get_string_vector(number_of_columns):
    return np.ctypeslib.as_array(arr_base.get_obj()).view(f"S{strlen}").reshape(arr_sz, number_of_columns)


class SignalTimeout(Exception):
    pass


def timeout(seconds_remaining=1):
    def _on_alarm(signum, frame):
        raise SignalTimeout("Time's up!")

    def will_wrap(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            signal.signal(signal.SIGALRM, _on_alarm)
            signal.alarm(seconds_remaining)
            try:
                res = func(*args, **kwargs)
            except SignalTimeout:
                raise
            finally:
                signal.signal(signal.SIGALRM, signal.SIG_DFL)
                signal.alarm(0)
            return res
        return wrapped
    return will_wrap


class MPReaderServer:
    columns = [b"#CHROM", b"POS", b"ID", b"REF", b"ALT", b"QUAL", b"FILTER", b"INFO", b"FORMAT"]

    def __init__(self, filename, queue, num_vcfs, vcf_index):
        self.queue = queue
        self.raw = open(filename, "rb")
        self.gzip_reader = bgzip.BGZipReaderPreAllocated(self.raw,
                                                         memoryview(bytearray(1024 * 1024 * 1024)),
                                                         num_threads=4,
                                                         raw_read_chunk_size=5 * 1024 * 1024)
        self.reader = io.BufferedReader(self.gzip_reader)
        self.vcf_index = vcf_index
        self.lines = get_string_vector(num_vcfs + 1)
        self.line_index = 0
        self._parse_header()
        self._chunk_refs = list()

    def _parse_header(self):
        self.header = list()
        for line in self.reader:
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

    @timeout(1)
    def get_line(self):
        return self.reader.readline()

    def run(self):
        while True:
            start_line_index = self.line_index
            for count in range(1, 1 + chunk_size):
                try:
                    l = self.get_line()
                except SignalTimeout:
                    self.queue.put(None)
                    return
                self.lines[self.line_index % arr_sz, self.vcf_index] = l
                self.line_index += 1
            chunk_ref = start_line_index, count
            self.queue.put(chunk_ref)

    @classmethod
    def start(cls, *args, **kwargs):
        self = cls(*args, **kwargs)
        self.run()
        
    def stop(self):
        self.gzip_reader.close()
        self.raw.close()


class MPReaderClient(io.IOBase):
    def __init__(self, filename: str, num_vcfs, vcf_index):
        q = Queue(5)
        self.queue = q
        self.proc = Process(target=MPReaderServer.start, args=(filename, q, num_vcfs, vcf_index))
        self.proc.start()

    def get_next_chunk_ref(self):
        try:
            return self.queue.get(True, 1)
        except queue.Empty:
            return None

    def close(self, *args, **kwargs):
        if not self.proc.join(5):
            self.proc.terminate()

    def readinto(self, buff):
        d = self.read(len(buff))
        buff[:len(d)] = d
        return len(d)


def process_lines(chunk_refs):
    start, count = chunk_refs[0]
    for cr in chunk_refs[1:]:
        assert start == cr[0]
        assert count == cr[1]
    lines = get_string_vector(len(chunk_refs) + 1)
    for line_index in range(start, start + count):
        i = line_index % arr_sz
        rows = [vcf.VCFRow(*lines[i, c].strip().split(tab, 9)) for c in range(len(chunk_refs))]
        new_row = rows[0]
        new_line = tab.join(new_row + tuple(row.samples for row in rows[1:]))
        lines[i, len(chunk_refs)] = new_line
    return True


def write_lines(q, num_vcfs):
    lines = get_string_vector(num_vcfs + 1)
    with open("out.vcf.gz", "wb") as fh_out:
        writer_batch_size = 20000
        number_of_writer_threads = 8
        with bgzip.AsyncBGZipWriter(fh_out, writer_batch_size, number_of_writer_threads) as writer: 
            while True:
                try:
                    chunk_ref = q.get(True, 1)
                except queue.Empty:
                    break
                if chunk_ref is None:
                    break
                start, count = chunk_ref
                for line_index in range(start, start + count):
                    i = line_index % arr_sz
                    writer.write(lines[i, num_vcfs])
        

# filenames = ["HVH_phs000993_TOPMed_WGS_freeze.8.chr7.hg38.vcf.gz"]
filenames = ["HVH_phs000993_TOPMed_WGS_freeze.8.chr7.hg38.vcf.gz", "Amish_phs000956_TOPMed_WGS_freeze.8.chr7.hg38.vcf.gz"]
num_vcfs = len(filenames)
create_base(num_vcfs + 1)
readers = [MPReaderClient(filenames[i], num_vcfs, i) for i in range(num_vcfs)]
futures = list()

writer_queue = Queue()
writer_proc = Process(target=write_lines, args=(writer_queue, num_vcfs))
writer_proc.start()


start_time = time.time()
with ProcessPoolExecutor(max_workers=1) as e:
    finished = False
    while not finished:
        while len(futures) < 2:
            chunk_refs = [r.get_next_chunk_ref() for r in readers]
            if None in chunk_refs:
                finished = True
                break
            else:
                f = e.submit(process_lines, chunk_refs)
                futures.append((f, chunk_refs[0]))
        while futures and futures[0][0].done():
            f, chunk_ref = futures.pop(0)
            writer_queue.put(chunk_ref)
            foo = f.result()
    for r in readers:
        r.close()


writer_queue.close()
writer_proc.join()


print("DOOM", time.time() - start_time)


# doom = Array("c", 10 * 5 * 2)
# foo = np.ctypeslib.as_array(doom.get_obj())
# bar = foo.view("S10").reshape(5, 2)
# bar[0,0] = "frank"
# bar[0,1] = "bobby"
# print(bar)
# print(foo)
