from easy_stream.task_lib.context import Task, Context
from easy_stream.task_lib.tasks.base_task import BaseTask


class Clean(BaseTask):
    def __init__(self, task: Task):
        self.task = task
        self.is_active = False

    def __call__(self, ctx: Context):
        if self.is_active:
            ctx.clean()
        self.task(ctx)
