import json
import threading
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from typing import Any, Callable, Dict, Optional, Iterable, Set

import jmespath


class _AsyncContextManager:
    """
    Context manager for asynchronous execution. Wait on exit for all jobs to complete.
    """
    def submit(self, func: Callable[..., Any], *args, **kwargs):
        f = self._executor.submit(func, *args, **kwargs)
        self._futures.add(f)
        if threading.current_thread() is threading.main_thread():
            self.prune_futures()

    def prune_futures(self):
        assert threading.current_thread() is threading.main_thread()
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

def concurrent_recursion(recurse: Callable[[Any], Iterable[Any]], initial_data: Iterable[Any], concurrency: int=8):
    """
    Call `recurse` on each item in `initial_data`, and on each item returned by `recurse`, concurrently.
    """
    with ThreadPoolExecutor(max_workers=concurrency) as e:
        futures = {e.submit(recurse, item) for item in initial_data}
        while futures:
            for f in as_completed(futures):
                futures.remove(f)
                futures.update({e.submit(recurse, item) for item in f.result()})

def js_get(path: str, data: Dict[str, Any], default: Optional[Any]=None) -> Any:
    res = jmespath.search(path, data)
    if res is not None:
        return res
    elif default is not None:
        return default
    else:
        raise KeyError(f"'{path}' not found in {json.dumps(data, indent=2)}")
