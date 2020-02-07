import sys
import time
import threading
from math import floor, ceil
from contextlib import AbstractContextManager

class ProgressBar(AbstractContextManager):
    def __init__(self, number_of_steps, prefix="", suffix=""):
        self.number_of_steps = number_of_steps
        self.steps_completed = 0
        self.prefix = prefix
        self.suffix = suffix
        self._length = 80
        self._lock = threading.Lock()
        self._start_time = time.time()

    def update(self, number_of_steps=1):
        self.steps_completed += number_of_steps
        slots_filled = int(self._length * self.steps_completed / self.number_of_steps)
        duration = ceil(time.time() - self._start_time)
        bar = "#" * slots_filled
        bar += " " * (self._length - slots_filled)
        bar += f" {floor(100 * self.steps_completed / self.number_of_steps)}%"
        with self._lock:
            print(self.prefix, bar, self.suffix, f"({duration} seconds)", end="\r")
            sys.stdout.flush()

    def close(self, message=None):
        with self._lock:
            print()
            if message is not None:
                print(message)

    def __exit__(self, *args, **kwargs):
        self.close()
