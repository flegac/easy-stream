from easy_stream.concurrent_lib.runner import Runner
from easy_stream.concurrent_lib.task.parallel import Parallel
from easy_stream.concurrent_lib.task.serial import Serial
from easy_stream.concurrent_lib.task.task import Task
from easy_kit.timing import time_func


class SimpleRunner(Runner):
    def __init__(self):
        self._completed = []
        self._hierarchy = []

    def completed_tasks(self) -> int:
        return len(self._completed) - len(self._hierarchy)

    @time_func
    def run(self, task: Task):
        self.run_any(task)

    def parallel(self, task: Parallel):
        for t in task.tasks:
            self.run_any(t)
        self._hierarchy.append(task)
        self._completed.append(task)

    def serial(self, task: Serial):
        for t in task.tasks:
            self.run_any(t)
        self._hierarchy.append(task)
        self._completed.append(task)

    def task(self, task: Task):
        task.runner()
        self._completed.append(task)
