import os
import time
import unittest

from easy_kit.timing import time_func, TimingTestCase

from easy_stream.concurrent_lib import parallel, serial, Task
from easy_stream.concurrent_lib.runner import Runner

SLOW_TESTS = bool(os.getenv('SLOW_TESTS', False))


@time_func
def cpu_task():
    return sum([1 for _ in range(100_000)])


@time_func
def sleeper_task():
    time.sleep(.02)


def task_provider(i):
    if i % 2 == 0:
        # if random.random() > .5:
        return cpu_task
    return sleeper_task


def long_serial_task(tasks: int):
    return serial(
        task_provider(_)
        for _ in range(tasks)
    )


def dummy_task():
    pass


task = Task(dummy_task)


class TestTaskRunner(TimingTestCase):

    def test_parallel_shrink(self):
        actual = parallel(
            task,
            parallel(parallel(parallel(
                task, task, task
            ))),
            task
        )
        expected = 'Parallel[00, 00, 00, 00, 00]'

        self.assertEqual(expected, actual.description)

    def test_sequential_shrink(self):
        actual = serial(
            task,
            serial(serial(serial(
                task, task, task
            ))),
            task
        )
        expected = 'Serial[00, 00, 00, 00, 00]'

        self.assertEqual(expected, actual.description)

    def test_complex_pipeline(self):
        n = 2
        actual = parallel(
            serial(parallel(serial(parallel(serial(dummy_task))))),
            parallel(
                dummy_task for _ in range(2)
            ),
            parallel(
                serial(
                    dummy_task for _ in range(3)
                )
                for _ in range(2)
            ),
            serial(
                parallel(
                    serial(
                        dummy_task for _ in range(2)
                    ),
                    dummy_task,
                )
                for _ in range(2)
            ),

        )

        expected = 'Parallel[01, 03, 04, Serial[06, 07, 08], Serial[10, 11, 12], Serial[Parallel[Serial[15, 16], 18], Parallel[Serial[20, 21], 23]]]'

        self.assertEqual(expected, actual.description)

    @unittest.skipUnless(SLOW_TESTS or True, 'slow')
    def test_execution_speed(self):
        pipe = parallel(
            parallel(
                task_provider(_) for _ in range(100)
            ),
            serial(
                parallel(
                    cpu_task,
                    cpu_task,
                    sleeper_task,
                )
                for _ in range(10)
            ),
        )
        for factory in [Runner.simple, Runner.thread, Runner.process]:
            with self.subTest(factory):
                tasker = factory()
                tasker.run(pipe)
                print(f'{tasker}: {tasker.completed_tasks()} tasks processed')
