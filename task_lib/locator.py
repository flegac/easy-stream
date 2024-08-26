from pathlib import Path
from typing import Union, List

from task_lib.locators.locator import Locator
from task_lib.locators.posix import Posix
from task_lib.locators.step import Step


class Loc:
    @staticmethod
    def posix(*globs: Union[str, Path]) -> Locator:
        return Posix(*globs)

    @staticmethod
    def step(*globs: str) -> Locator:
        return Step(*globs)

    @staticmethod
    def sibling(*globs: str) -> Locator:
        return Step(*globs, is_sibling=True)

    @staticmethod
    def with_dest(paths: List[Path], root: Path):
        return list(map(lambda p: root / p.name, paths))

    @staticmethod
    def with_suffix(paths: List[Path], suffix: str):
        return list(map(lambda p: p.with_suffix(suffix), paths))
