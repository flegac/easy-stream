from task_lib.context import Task, Context
from task_lib.task_infos import TaskInfos, TaskStatus
from task_lib.tasks.base_task import BaseTask


class Skip(BaseTask):
    def __init__(self, task: Task):
        self.task = task
        self.is_active = True

    def __call__(self, ctx: Context):
        infos = TaskInfos.from_context(ctx)
        if infos.status is TaskStatus.Done:
            if self.is_active:
                ctx.debug('SKIPPED')
                return
            else:
                infos.duration = 0
                infos.save(ctx)

        self.task(ctx)
