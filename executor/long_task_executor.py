import concurrent
from typing import Sequence, Any, Callable

from alive_progress import alive_bar


class LongAsyncTaskExecutor(object):
    def __init__(self, max_workers: int = 5):
        self.max_workers = max_workers

    def execute(self, items: Sequence[Any], item_func: Callable, item_func_args: Sequence[Any],
                callback: Callable[[Any], Any]):
        with alive_bar(len(items)) as bar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [executor.submit(item_func, *item_func_args, item) for item in items]

                for future in concurrent.futures.as_completed(futures):
                    callback(future.result())
                    bar()
