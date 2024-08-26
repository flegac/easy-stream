from typing import Optional

from task_lib.context import Task, Context
from task_lib.tasks.clean import Clean
from task_lib.tasks.groups.parallel import Parallel
from task_lib.tasks.groups.sequential import Sequential
from task_lib.tasks.name import Name
from task_lib.tasks.place_holder import PlaceHolder
from task_lib.tasks.prepare import Prepare
from task_lib.tasks.skip import Skip
from task_lib.tasks.status import Status


class Step(Task):
    def __init__(self, name: str = '', task: Task = None):
        self._place_holder = PlaceHolder(task)
        self._status = Status(self._place_holder)
        self._skip = Skip(self._status)
        self._prepare = Prepare(self._skip)
        self._clean = Clean(self._prepare)
        self._name = Name(name, self._clean)
        self.delegate = self._name

    def __call__(self, ctx: Context):
        self.delegate(ctx)

    def skip(self, is_active: bool = True):
        """
        Auto skip this step if infos.status == Done
        :param is_active:
        :return:
        """
        self._skip.is_active = is_active
        return self

    def clean(self, is_active: bool = True):
        """
        Auto clean this step ctx.workspace before execution if is_active is True
        :param is_active:
        :return:
        """
        self._clean.is_active = is_active
        return self

    def sequential(self, *tasks: Optional[Task]):
        return self._configure(Sequential(*tasks)).skip(False)

    def parallel(self, *tasks: Optional[Task]):
        return self._configure(Parallel(*tasks)).skip(False)

    def _configure(self, task: Task):
        if self._place_holder.task is not None:
            raise ValueError(
                f'Redefining {self._place_holder}: '
                f'you should call once, either {self.__class__.__name__}.parallel'
                f' or  {self.__class__.__name__}.sequential.'
            )
        self._place_holder.task = task
        return self
