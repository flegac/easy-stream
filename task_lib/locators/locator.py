from abc import abstractmethod
from pathlib import Path
from typing import List

from task_lib.context import Context


class Skip:
    def __init__(
            self,
            first: int = None,
            limit: int = None,
            files=False,
            directories=False,
            hidden=True
    ):
        self.first = first
        self.limit = limit
        self.files = files
        self.directories = directories
        self.hidden = hidden

    def apply(self, paths: List[Path]):
        if self.files:
            paths = filter(lambda p: not p.is_file(), paths)
        if self.directories:
            paths = filter(lambda p: not p.is_dir(), paths)
        if self.hidden:
            paths = filter(lambda p: not p.name.startswith('.'), paths)
        paths = list(paths)

        start = 0
        stop = len(paths)
        if self.first is not None:
            start = self.first
        if self.limit is not None:
            stop = start + self.limit
        return paths[slice(start, stop)]

    def __repr__(self):
        return f'{self.__class__.__name__}(f={self.files}, d={self.directories}, h={self.hidden})'

    def update(self, files: bool = None, directories: bool = None, hidden: bool = None):
        if files is not None:
            self.files = files
        if directories is not None:
            self.directories = directories
        if hidden is not None:
            self.hidden = hidden


class Locator:
    def __init__(self):
        self._skip = Skip()

    def skip(self, files: bool = None, directories: bool = None, hidden: bool = None):
        self._skip.update(files, directories, hidden)
        return self

    def limit(self, n: int):
        self._skip.limit = n
        return self

    def iter(self, ctx: Context):
        paths = self._iter_ctx(ctx)
        return self._skip.apply(paths)

    def single(self, ctx: Context):
        paths = self.iter(ctx)
        return paths[0]

    @abstractmethod
    def _iter_ctx(self, ctx: Context) -> List[Path]:
        ...

    def __repr__(self):
        return f'{self.__class__.__name__}({self.__dict__})'
