import sys
import time
import threading
from math import floor, ceil
from contextlib import AbstractContextManager

class ProgressBar(AbstractContextManager):
    def __init__(self, number_of_steps, prefix="", units="", size: int=None):
        self.number_of_steps = number_of_steps
        self.steps_completed = 0
        self.prefix = prefix
        self.units = units
        self.size = size
        self._length = 80
        self._lock = threading.Lock()
        self._start_time = time.time()

    def update(self, number_of_steps=1):
        self.steps_completed += number_of_steps
        slots_filled = int(self._length * self.steps_completed / self.number_of_steps)
        duration = ceil(time.time() - self._start_time)
        portion_complete = self.steps_completed / self.number_of_steps
        bar = "#" * slots_filled
        bar += " " * (self._length - slots_filled)
        bar += f" {floor(100 * portion_complete)}%"
        if self.size is not None:
            bar += f" of {self.size}{self.units}"
            bar += f" %.2f{self.units}/s" % (self.size * portion_complete / duration)
        with self._lock:
            print(self.prefix, bar, f"({duration} seconds)", end="\r")
            sys.stdout.flush()

    def close(self, message=None):
        with self._lock:
            print()
            if message is not None:
                print(message)

    def __exit__(self, *args, **kwargs):
        self.close()
