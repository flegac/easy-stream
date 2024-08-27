from abc import ABC, abstractmethod
from typing import Mapping, Type, Callable, Any

from easy_stream.concurrent_lib.task.parallel import Parallel
from easy_stream.concurrent_lib.task.serial import Serial
from easy_stream.concurrent_lib.task.task import Task


class Runner(ABC):

    @staticmethod
    def thread(worker_count: int = None):
        from easy_stream.concurrent_lib.thread_runner import ThreadRunner
        return ThreadRunner(worker_count=worker_count)

    @staticmethod
    def process(worker_count: int = None):
        from easy_stream.concurrent_lib.process_runner import ProcessRunner
        return ProcessRunner(worker_count=worker_count)

    @staticmethod
    def simple():
        from easy_stream.concurrent_lib.simple_runner import SimpleRunner
        return SimpleRunner()

    @abstractmethod
    def completed_tasks(self) -> int:
        ...

    @abstractmethod
    def run(self, task: Task) -> int:
        ...

    def run_any(self, task: Task):
        runners: Mapping[Type[Task], Callable[[Task], Any]] = {
            Parallel: self.parallel,
            Serial: self.serial,
            Task: self.task,
        }
        runners[type(task)](task)

    @abstractmethod
    def parallel(self, task: Parallel):
        ...

    @abstractmethod
    def serial(self, task: Serial):
        ...

    @abstractmethod
    def task(self, task: Task):
        ...

    def __repr__(self):
        params = {
            k: v
            for k, v in self.__dict__.items()
            if not k.startswith('_')
        }
        params = ', '.join(f'{k}={v}' for k, v in params.items())
        return f'{self.__class__.__name__}({params})'
