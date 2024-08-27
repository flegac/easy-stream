from easy_stream.task_lib.context import Task, Context
from easy_stream.task_lib.tasks.base_task import BaseTask


class PlaceHolder(BaseTask):
    def __init__(self, task: Task = None):
        self.task = task

    def __call__(self, ctx: Context):
        if self.task is not None:
            self.task(ctx)
