from pathlib import Path

from task_lib.context import Context
from task_lib.locators.locator import Locator


class Posix(Locator):
    def __init__(self, *items: Path):
        super().__init__()
        self.items = [Path(_) for _ in items]

    def _iter_ctx(self, ctx: Context = None):
        res = []

        for glob in self.items:
            res.extend(glob.parent.glob(glob.name))
        return res
