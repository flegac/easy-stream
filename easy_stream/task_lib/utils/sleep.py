import random

import time

from easy_kit.timing import time_func
from easy_stream.task_lib.context import Task, Context


class Sleep(Task):
    @staticmethod
    @time_func
    def random(max_duration: float):
        return Sleep(random.random() * max_duration)

    def __init__(self, duration: float):
        self.duration = duration

    def __call__(self, ctx: Context):
        time.sleep(self.duration)
