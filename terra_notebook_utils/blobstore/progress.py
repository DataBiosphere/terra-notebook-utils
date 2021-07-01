import enum
from math import ceil
from typing import Union

from getm import default_chunk_size
from getm.progress import ProgressBar, ProgressLogger

from terra_notebook_utils.logger import logger


class Indicator(enum.Enum):
    log = ProgressLogger
    bar = ProgressBar

    @classmethod
    def get(cls, prog_type: Union[str, "Indicator"], name: str, size: int):
        if isinstance(prog_type, str):
            pt = cls[prog_type]
        elif isinstance(prog_type, cls):
            pt = prog_type
        else:
            raise TypeError(f"unsupported progress indicator type '{type(prog_type)}'")
        return cls._instantiate(pt, name, size)

    @classmethod
    def _instantiate(cls, tp: "Indicator", name: str, size: int):
        if tp in (cls.bar,):
            increments = 20
        elif tp == cls.log:
            increments = ceil(size / default_chunk_size / 2)
        logger.debug(f"Using progress indicator classs '{tp.name}'")
        return tp.value(name, size, increments)
