from typing import Callable, Iterable

from easy_stream.concurrent_lib.task.group import Group
from easy_stream.concurrent_lib.task.parallel import Parallel
from easy_stream.concurrent_lib.task.serial import Serial
from easy_stream.concurrent_lib.task.task import Task


def _build(factory: type[Group], *tasks: Task | Iterable | Callable):
    if len(tasks) == 0:
        raise ValueError
    if len(tasks) == 1:
        item = tasks[0]
        if isinstance(item, Iterable):
            return factory(*item)
        if isinstance(item, Task):
            return item
        return Task(item)

    return factory.from_iter(tasks)


def serial(*tasks: Task | Iterable | Callable):
    return _build(Serial, *tasks)


def parallel(*tasks: Task | Iterable | Callable):
    return _build(Parallel, *tasks)


def task():
    pass


if __name__ == '__main__':
    zzz = serial(_ for _ in [task, task, task])

    xxx = serial(
        task,
        task,
        serial(
            task,
            task,
            parallel(
                task,
                task
            ),
            task
        ),
        parallel(
            serial(
                _
                for _ in [task, task, task]
            ),
            task
        ),

        task,
        task
    )

    yyy = serial([task, task])

    print()
