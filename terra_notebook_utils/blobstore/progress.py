import time
import enum
from math import ceil
from typing import Union

from getm import default_chunk_size
from getm.progress import ProgressBar, ProgressLogger, sizeof_fmt

from terra_notebook_utils.logger import logger


class NotebookProgressBar:
    def __init__(self, name: str, size: int, increments: int=0):
        print(name, flush=True)
        from ipywidgets import HBox, IntProgress, Label
        from IPython.display import display, clear_output
        self.size = size
        self._percent = Label("")
        self._bar = IntProgress(min=0, max=size)
        self._rate = Label("")
        self._duration = Label("")
        ui = HBox([self._percent, self._bar, Label(sizeof_fmt(size)), self._rate, self._duration])
        display(ui)
        logger.debug(f"'{type(self)}' displayed IPython widget '{ui}'.")

    def add(self, sz: int):
        logger.debug(f"'{type(self)}' added progress '{sz}'.")
        duration = time.time() - self._start
        progress = self._bar.value + sz
        self._bar.value = progress
        self._percent.value = "{:3d}%".format((100 * progress) // self.size)
        self._rate.value = f"{sizeof_fmt(progress / duration)}/s"
        self._duration.value = "{:.2f}s".format(duration)

    def __enter__(self):
        logger.debug(f"'{type(self)}' entered context.")
        self._start = time.time()
        self.add(0)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.add(0)
        logger.debug(f"'{type(self)}' exited context.")

class Indicator(enum.Enum):
    log = ProgressLogger
    bar = ProgressBar
    notebook_bar = NotebookProgressBar

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
        if tp in (cls.bar, cls.notebook_bar):
            increments = 20
        elif tp == cls.log:
            increments = ceil(size / default_chunk_size / 2)
        logger.debug(f"Using progress indicator classs '{tp.name}'")
        return tp.value(name, size, increments)
