import itertools
import os
from concurrent.futures.thread import ThreadPoolExecutor

from task_lib.context import Context
from task_lib.tasks.groups.group import Group


class Parallel(Group):
    def __call__(self, ctx: Context):
        process_count = os.cpu_count()
        n = len(self.tasks)
        indexes = list(range(n))
        with ThreadPoolExecutor(max_workers=process_count) as pool:
            pool.map(self._processor, itertools.product(indexes, [ctx]))

    def _processor(self, args):
        i, ctx = args
        self.tasks[i](ctx)
