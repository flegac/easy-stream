import shutil

from task_lib.context import Context
from task_lib.locators.locator import Locator
from task_lib.tasks.base_task import BaseTask


class Copy(BaseTask):
    def __init__(self, target: Locator):
        self.target = target

    def __call__(self, ctx: Context):
        for src in self.target.iter(ctx):
            dst = ctx.path_to(src.name)
            ctx.debug(f'copy {src} into {dst}')
            if src.is_dir():
                shutil.copytree(src, dst, dirs_exist_ok=True)
            else:
                shutil.copy(src, ctx.output)
