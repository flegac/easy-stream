from typing import Any


class TaskLogger:
    def __init__(self, verbose=True):
        self.verbose = verbose
        self.print = print

    def debug(self, msg: Any):
        if self.verbose:
            self.print(msg)

    def info(self, msg: Any):
        if self.verbose:
            self.print(msg)

    def warning(self, msg: Any):
        if self.verbose:
            self.print(msg)

    def error(self, msg: Any):
        if self.verbose:
            self.print(msg)
