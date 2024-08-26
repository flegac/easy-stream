from typing import Any
from unittest.case import TestCase

from task_lib.context import Context
from task_lib.task_logger import TaskLogger


class TestContextLogger(TestCase):

    def test_logger(self):
        ctx = Context.temporary().child('x/y/z')
        ctx.logger = SaveLogger()

        ctx.debug('debug2')
        ctx.info('info1')
        ctx.warning('warning2')
        ctx.info('info2')
        ctx.error('error1')
        ctx.warning('warning1')
        ctx.error('error2')
        ctx.debug('debug1')

        actual = ctx.logger.msgs
        expected = [('x/y/z : debug2', 'debug'),
                    ('x/y/z : info1', 'info'),
                    ('x/y/z : warning2', 'warning'),
                    ('x/y/z : info2', 'info'),
                    ('x/y/z : error1', 'error'),
                    ('x/y/z : warning1', 'warning'),
                    ('x/y/z : error2', 'error'),
                    ('x/y/z : debug1', 'debug')]

        self.assertEqual(expected, actual)


class SaveLogger(TaskLogger):
    def __init__(self):
        super().__init__()
        self.msgs = []

    def debug(self, msg: Any):
        self._save((msg, 'debug'))

    def info(self, msg: Any):
        self._save((msg, 'info'))

    def warning(self, msg: Any):
        self._save((msg, 'warning'))

    def error(self, msg: Any):
        self._save((msg, 'error'))

    def _save(self, data):
        self.msgs.append(data)