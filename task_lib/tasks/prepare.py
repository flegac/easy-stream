from task_lib.context import Task, Context
from task_lib.tasks.base_task import BaseTask


class Prepare(BaseTask):
    def __init__(self, task: Task):
        self.task = task
        self.is_active = True

    def __call__(self, ctx: Context):
        if self.is_active:
            ctx.prepare()
        self.task(ctx)
