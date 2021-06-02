import time
from contextlib import AbstractContextManager

class profile(AbstractContextManager):
    """Profile methods or code blocks with decorators or contexts, respectively.

    @profile("profiling my method")
    def my_method(*args, **kwargs):
       ...

    with profile("profiling my block"):
       ...
    """
    def __init__(self, name="default"):
        self.name = name

    def __enter__(self, *args, **kwargs):
        self.start = time.time()

    def __exit__(self, *args, **kwargs):
        print(self.name, time.time() - self.start)

    def __call__(self, meth):
        def wrapper(*args, **kwargs):
            start = time.time()
            res = meth(*args, **kwargs)
            dur = time.time() - start
            print(f"{self.name} took {dur} seconds")
            return res
        return wrapper
