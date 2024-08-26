from pathlib import Path

from task_lib.context import Context
from task_lib.locators.locator import Locator


class Step(Locator):
    def __init__(self, *items: str, is_sibling: bool = False):
        super().__init__()
        self.items = [_ for _ in items]
        self.is_sibling = is_sibling

    def _iter_ctx(self, ctx: Context = None):
        ref = ctx.output if self.is_sibling else ctx.root
        for item in self.items:
            glob = Path(item)
            name = glob.name
            root_path = ref / glob.parent
            for path in root_path.glob(name):
                yield path
