import sys
import threading
from contextlib import AbstractContextManager

class ProgressBar(AbstractContextManager):
    def __init__(self, number_of_steps, prefix="", suffix=""):
        self.number_of_steps = number_of_steps
        self.steps_completed = 0
        self.prefix = prefix
        self.suffix = suffix
        self._length = 80
        self._lock = threading.Lock()

    def update(self, number_of_steps=1):
        self.steps_completed += number_of_steps
        slots_filled = int(self._length * self.steps_completed / self.number_of_steps)
        bar = "#" * slots_filled
        bar += " " * (self._length - slots_filled)
        bar += f" {self.steps_completed} of {self.number_of_steps}"
        with self._lock:
            print(self.prefix, bar, self.suffix, end="\r")
            sys.stdout.flush()

    def finish(self, message=None):
        with self._lock:
            print()
            if message is not None:
                print(message)

    def __exit__(self, *args, **kwargs):
        self.finish()
