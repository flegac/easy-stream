from functools import total_ordering
from typing import Callable, Any, Union


@total_ordering
class Task:
    sequence = 0

    def __init__(self, task: Callable[[], Any] = None):
        if task is not None and not isinstance(task, Callable):
            raise ValueError
        self.tid = Task.sequence
        Task.sequence += 1
        self.runner = task
        self.task_size = 1

    @property
    def description(self):
        return f'{self.tid:02}'

    def __repr__(self):
        if type(self) is not Task:
            return f'{self.__class__.__name__}'
        return f'{self.__class__.__name__}_{self.tid:02}'

    def __str__(self):
        return repr(self)

    def __hash__(self):
        return hash(self.tid)

    def __eq__(self, other):
        if not isinstance(other, Task):
            return NotImplemented
        return self.tid == other.tid

    def __lt__(self, other):
        if not isinstance(other, Task):
            return NotImplemented
        return self.task_size > other.task_size


TASK = Union[Task, Callable[[], Any]]


def taskify(task: TASK):
    if isinstance(task, Task):
        return task
    return Task(task)
