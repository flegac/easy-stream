import itertools
from typing import Iterable

from concurrent_lib.task.task import Task, TASK, taskify


class Group(Task):
    @classmethod
    def from_iter(cls, tasks: Iterable[TASK]):
        return cls(*tasks)

    def __init__(self, *tasks: TASK):
        super().__init__()
        self.tasks = []
        for tt in tasks:
            self.tasks.extend(self.taskify(tt))
        self.tasks = tuple(self.tasks)
        self.task_size = sum([_.task_size for _ in self.tasks])

    @classmethod
    def taskify(cls, task: TASK) -> Iterable[Task]:
        if not isinstance(task, cls):
            return [taskify(task)]
        return list(itertools.chain.from_iterable(
            map(cls.taskify, task.tasks)
        ))

    @property
    def description(self):
        return f'{self}[{', '.join(_.description for _ in self.tasks)}]'
