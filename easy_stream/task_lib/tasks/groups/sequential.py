from easy_stream.task_lib.context import Context
from easy_stream.task_lib.tasks.groups.group import Group


class Sequential(Group):
    def __call__(self, ctx: Context):
        for task in self.tasks:
            task(ctx)
