from task_lib.context import Task, Context
from task_lib.tasks.base_task import BaseTask


class Name(BaseTask):
    def __init__(self, name: str, task: Task):
        assert isinstance(name, str)
        self.name = name
        self.task = task

    def __call__(self, ctx: Context):
        self.task(ctx.child(self.name))
