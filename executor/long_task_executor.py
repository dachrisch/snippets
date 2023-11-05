import concurrent
from concurrent import futures
from typing import Sequence, Any, Callable, TypeVar, Generic

from alive_progress import alive_bar

T = TypeVar('T')
V = TypeVar('V')


class LongAsyncTaskExecutor(Generic[T]):
    def __init__(self, max_workers: int = 5):
        self.max_workers = max_workers

    def execute(self,
                items: Sequence[T],
                item_func: Callable[[*Any, T], V],
                item_func_args: Sequence[Any],
                callback: Callable[[V], Any]) -> None:
        with alive_bar(len(items)) as bar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                _futures = [executor.submit(item_func, *item_func_args, item) for item in items]

                for future in concurrent.futures.as_completed(_futures):
                    callback(future.result())
                    bar()
