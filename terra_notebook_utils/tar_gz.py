import os
import gzip
import typing
import tarfile
from math import ceil
from pathlib import Path
from contextlib import closing
from typing import Optional

import gs_chunked_io as gscio
from google.cloud.storage.bucket import Bucket

from terra_notebook_utils import gs
from terra_notebook_utils.progress import ProgressBar

_chunk_size = 1024 * 1024 * 32

# Set this for unit tests only
_extract_single_chunk: bool = False

def extract(src_fh, dst_bucket: typing.Optional[Bucket]=None, root: typing.Optional[str]=None):
    """
    Extract a tar.gz archive into the local filesystem, or a GS bucket if `dst_bucket` is provided.
    """
    gzip_reader = gzip.GzipFile(fileobj=src_fh)
    tf = tarfile.TarFile(fileobj=gzip_reader)  # type: ignore
    for tarinfo in tf:
        if tarinfo.isfile():
            if dst_bucket:
                dst_fh = _prepare_gs(tarinfo, dst_bucket, root or "")
            else:
                dst_fh = _prepare_local(tarinfo, root or "")
            print(f"Inflating {tarinfo.name}")
            progress_bar = ProgressBar(ceil(tarinfo.size / _chunk_size) + 1,
                                       size=ceil(tarinfo.size / 1024 ** 2),
                                       units="MB")
            with closing(progress_bar):
                with closing(dst_fh):
                    _transfer_data(tf.extractfile(tarinfo), dst_fh, progress_bar)
                progress_bar.update()

            # Hack for speedy unit tests
            if _extract_single_chunk:
                return

def _transfer_data(from_fh, to_fh, progress_bar: ProgressBar=None):
    while True:
        data = from_fh.read(_chunk_size)
        if data:
            to_fh.write(data)
        else:
            break
        if progress_bar:
            progress_bar.update()

        # Hack for speedy unit tests
        if _extract_single_chunk:
            return

def _prepare_local(tarinfo: tarfile.TarInfo, root: str):
    filepath = os.path.abspath(os.path.join(root, tarinfo.name))
    Path(os.path.dirname(filepath)).mkdir(parents=True, exist_ok=True)
    return open(filepath, "wb")

def _prepare_gs(tarinfo: tarfile.TarInfo, bucket: Bucket, root: typing.Optional[str]):
    if root is not None:
        key = f"{root}/{tarinfo.name}"
    else:
        key = tarinfo.name
    return gscio.Writer(key, bucket, chunk_size=_chunk_size)
