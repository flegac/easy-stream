import time

from easy_stream.task_lib.context import Task, Context
from easy_stream.task_lib.task_infos import TaskStatus, TaskInfos
from easy_stream.task_lib.tasks.base_task import BaseTask


class Status(BaseTask):
    def __init__(self, task: Task):
        self.task = task

    def __call__(self, ctx: Context):
        ctx.status = TaskStatus.Running
        _set_status(ctx, TaskStatus.Running)
        start = time.time()
        try:
            self.task(ctx)
            _set_status(ctx, TaskStatus.Done)
        except Exception as e:
            _set_status(ctx, TaskStatus.Error)
            raise e
        finally:
            delta = time.time() - start
            infos = TaskInfos.from_context(ctx)
            infos.duration += delta
            infos.save(ctx)


def _set_status(ctx: Context, status: TaskStatus):
    infos = TaskInfos.from_context(ctx)

    ctx.debug(f'[{infos.status.name}] -> [{status.name}]')
    infos.status = status
    infos.save(ctx)
