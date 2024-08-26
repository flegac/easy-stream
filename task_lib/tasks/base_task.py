from task_lib.context import Task


class BaseTask(Task):
    def __repr__(self):
        return f'{self.__class__.__name__}({self.__dict__})'
