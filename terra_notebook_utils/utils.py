from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from typing import Any, Callable, Iterable, Set


class _AsyncContextManager:
    """
    Context manager for asynchronous execution. Wait on exit for all jobs to complete.
    """
    def submit(self, func: Callable[..., Any], *args, **kwargs):
        f = self._executor.submit(func, *args, **kwargs)
        self._futures.add(f)
        self.prune_futures()

    def prune_futures(self):
        for f in self._futures.copy():
            if f.done():
                f.result()
                self._futures.remove(f)

    def _prepare_for_exit(self):
        raise NotImplementedError()

    def __enter__(self):
        self._executor = ThreadPoolExecutor(max_workers=10)
        self._futures: Set[Future] = set()
        return self

    def __exit__(self, *args, **kwargs):
        self._prepare_for_exit()
        while self._futures:
            for f in as_completed(self._futures):
                pass
            self.prune_futures()
        self._executor.shutdown()

def concurrent_recursion(process_item: Callable[[Any], Iterable[Any]], items: Iterable[Any], concurrency: int=8):
    """
    Call `process_item` on each item in `items`, and on each item returned by `process_item`, concurrently.
    """
    with ThreadPoolExecutor(max_workers=concurrency) as e:
        futures = {e.submit(process_item, item) for item in items}
        while futures:
            for f in as_completed(futures):
                futures.remove(f)
                futures.update({e.submit(process_item, item) for item in f.result()})
