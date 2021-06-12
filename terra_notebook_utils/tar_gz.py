import os
import gzip
import tarfile
from typing import Optional

from getm.progress import ProgressBar

from terra_notebook_utils.blobstore import copy_client


_chunk_size = 1024 * 1024 * 32

# Set this for unit tests only
_extract_single_chunk: bool = False

def extract(src_fh, root: Optional[str]=None):
    """Extract a tar.gz archive into the local filesystem, or a GS bucket if `dst_bucket` is provided."""
    root = root or os.getcwd()
    gzip_reader = gzip.GzipFile(fileobj=src_fh)
    tf = tarfile.TarFile(fileobj=gzip_reader)  # type: ignore
    for tarinfo in tf:
        if tarinfo.isfile():
            blob = copy_client.blob_for_url(f"{root}/{tarinfo.name}")
            with ProgressBar(tarinfo.name, tarinfo.size, 40) as progress:
                src_fh = tf.extractfile(tarinfo)
                with blob.part_writer() as writer:
                    while True:
                        data = src_fh.read(_chunk_size)
                        if data:
                            writer.put_part(data)
                            progress.add(len(data))
                        else:
                            break

                        # Hack for speedy unit tests
                        if _extract_single_chunk:
                            return
