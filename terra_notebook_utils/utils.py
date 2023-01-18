import json
import os
import threading
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from typing import Any, Callable, Dict, Optional, Iterable, Set

import jmespath

from terra_notebook_utils import ExecutionEnvironment, ExecutionPlatform, ExecutionContext


class _AsyncContextManager:
    """Context manager for asynchronous execution. Wait on exit for all jobs to complete."""
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
    """Call `recurse` on each item in `initial_data`, and on each item returned by `recurse`, concurrently."""
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

@lru_cache()
def is_notebook() -> bool:
    """Detect if runtime is a Jupyter notebook."""
    try:
        return "ZMQInteractiveShell" == get_ipython().__class__.__name__  # type: ignore
    except NameError:
        return False


@lru_cache()
def get_execution_context() -> ExecutionContext:
    """
    Identify information about the context in which terra-notebook-utils is executing.
    TODO Improve the information available and algorithm to identify these values accurately!
    """
    # Workaround current insufficient information by assuming
    # the execution environment is Terra, as that is the most
    # common and important case.
    # execution_environment = ExecutionEnvironment.OTHER
    execution_environment = ExecutionEnvironment.TERRA_WORKSPACE
    execution_platform = ExecutionPlatform.UNKNOWN
    workspace_bucket = os.environ.get('WORKSPACE_BUCKET', None)
    if workspace_bucket and workspace_bucket.startswith("gs://"):
        execution_platform = ExecutionPlatform.GOOGLE
    else:
        # Workaround current insufficient information by assuming
        # the execution platform is not Google then it is Azure.
        execution_platform = ExecutionPlatform.AZURE
    return ExecutionContext(execution_environment, execution_platform)
