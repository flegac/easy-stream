import copy
import shutil
import tempfile
from pathlib import Path
from typing import Callable, Any, Union

from easy_stream.task_lib.task_logger import TaskLogger


class Context:
    @staticmethod
    def temporary():
        return Context(tempfile.mkdtemp())

    def __init__(self, output: Union[str, Path], root: Path = None, logger: TaskLogger = None):
        self.output = Path(output)
        self.root = Path(root) if root is not None else self.output
        self.logger = logger

    @property
    def name(self):
        return self.output.name

    @property
    def step_id(self):
        return self.output.relative_to(self.root).as_posix()

    def clean(self):
        shutil.rmtree(self.output, ignore_errors=True)
        return self

    def prepare(self):
        self.output.mkdir(parents=True, exist_ok=True)
        return self

    # relative context
    def copy(self):
        return copy.copy(self)

    def root_context(self):
        ctx = self.copy()
        ctx.output = ctx.root
        return ctx

    def child(self, name: str):
        ctx = self.copy()
        ctx.output = ctx.output / name
        return ctx

    def sibling(self, name: str):
        ctx = self.copy()
        ctx.output = self.output.parent / name
        return ctx

    def path_to(self, name: str):
        return self.output / name

    # logging
    def debug(self, msg: Any):
        if self.logger:
            self.logger.debug(f'{self.step_id} : {msg}')
        return self

    def info(self, msg: Any):
        if self.logger:
            self.logger.info(f'{self.step_id} : {msg}')
        return self

    def warning(self, msg: Any):
        if self.logger:
            self.logger.warning(f'{self.step_id} : {msg}')
        return self

    def error(self, msg: Any):
        if self.logger:
            self.logger.error(f'{self.step_id} : {msg}')
        return self


Task = Callable[[Context], Any]
TaskFactory = Callable[[Any], Task]
