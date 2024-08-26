from task_lib.context import Task
from task_lib.tasks.base_task import BaseTask


class Group(BaseTask):
    @staticmethod
    def sequential(*tasks: Task):
        from task_lib.tasks.groups.sequential import Sequential
        return Sequential(*tasks)

    @staticmethod
    def parallel(*tasks: Task):
        from task_lib.tasks.groups.parallel import Parallel
        return Parallel(*tasks)

    def __init__(self, *tasks: Task):
        self.tasks = list(filter(None, tasks))
