import sys
import time
import threading
import sched
from math import floor, ceil
from contextlib import AbstractContextManager
from concurrent.futures import ThreadPoolExecutor, as_completed


class RateLimited:
    """
    Decorator to rate limit method calls. Raises `exception` if method is called more quickly than
    `rate` times per second, or does nothing if exception is None.
    """
    def __init__(self, rate: float, exception: Exception=None):
        self.period = 1 / rate
        self.exception = exception
        self.reset()

    def reset(self):
        self._last_call = time.time() - 10 * self.period

    def __call__(self, func):
        def wrapper(*args, **kwargs):
            now = time.time()
            if now - self._last_call >= self.period:
                self._last_call = now
                return func(*args, **kwargs)
            elif self.exception is not None:
                raise self.exception("Too soon")

        wrapper.reset = self.reset
        return wrapper


class ProgressBar(AbstractContextManager):
    def __init__(self, number_of_steps, prefix="", units="", size: int=None):
        self.number_of_steps = number_of_steps
        self.steps_completed = 0
        self.prefix = prefix
        self.units = units
        self.size = size
        self._length = 40  # This seems like a reasonable length for notebook output
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
        self._bar = bar
        self._duration = duration
        self._print()

    @RateLimited(2.0)
    def _print(self):
        with self._lock:
            print("\r", f"{self.prefix} {self._bar} ({self._duration} seconds)", end="")

    def close(self, message=None):
        self._print.reset()
        self._print()
        with self._lock:
            print()
            if message is not None:
                print(message)

    def __exit__(self, *args, **kwargs):
        self.close()


class ProgressReporter(AbstractContextManager):
    def __init__(self, units: str="lines"):
        self.start_time = time.time()
        self._checkpoint_time = [self.start_time, self.start_time]
        self._units_processed = [0, 0]
        self._lock = threading.Lock()
        self.units = units

    @property
    def duration(self):
        return self._checkpoint_time[-1] - self.start_time

    @property
    def average_rate(self):
        return self._units_processed[-1] / self.duration

    @property
    def instantaneous_rate(self):
        _units_processed = [self._units_processed[-1] - self._units_processed[-2]]
        duration = self._checkpoint_time[-1] - self._checkpoint_time[-2]
        return _units_processed / duration

    @property
    def units_processed(self):
        return self._units_processed[-1]

    def checkpoint(self, _units_processed: int):
        self._units_processed = [self._units_processed[-1], self._units_processed[-1] + _units_processed]
        self._checkpoint_time = [self._checkpoint_time[-1], time.time()]
        self._print()

    @RateLimited(2.0)
    def _print(self):
        with self._lock:
            print("\r",
                  f"%9i {self.units} processed," % self._units_processed[-1],
                  f"%6.0f {self.units}/s" % self.average_rate,
                  "%7.2f s" % self.duration,
                  end="")

    def close(self):
        self._print.reset()
        self._print()
        with self._lock:
            print()

    def __exit__(self, *args, **kwargs):
        self.close()
