from pathlib import Path
from unittest import TestCase

from easy_kit.timing import timing, TimingTestCase
from task_lib.context import Context
from task_lib.step import Step
from task_lib.tasks.groups.group import Group
from task_lib.utils.sleep import Sleep


def task_factory(label: str):
    def task(ctx: Context):
        ctx.debug(f'creating {label}')
        with ctx.path_to(f'{label}.txt').open('w') as _:
            _.write(f'{label}')

    return task


class TestStep(TestCase):
    # TODO : more unit tests

    def test_None_task(self):
        task_list = [
            Step(task=None),
            Step(),
            Step().sequential(),
            Step().parallel(),

            Step('xx', task=task_factory(f'xx')),
            Step('yy', task=task_factory(f'yy')),
        ]

        pipe = Step().sequential(
            None,
            task_factory(f'sub_1'),
            Step(name='single_step_None', task=None),
            Step(name='single_step', task=task_factory(f'single_step')),
            Step('using_add').sequential(
                *task_list
            )
        )
        ctx = Context(Path.cwd() / 'output')
        pipe(ctx)
        ctx.clean()

    def test_pipe(self):
        pipe = Step().sequential(
            task_factory('0'),
            Step('sub_1').sequential(*[
                task_factory(f'sub_1_{i}')
                for i in range(5)
            ]),
            Step('sub_2', task_factory('20')),
            task_factory('25'),
            Step('sub_3').sequential(
                task_factory('30'),
                task_factory('31'),
                task_factory('32'),
            ),
            task_factory('40'),
        ).skip(False).clean()

        ctx = Context(Path.cwd() / 'output2')
        pipe(ctx)
        ctx.clean()


class TestTaskGroup(TimingTestCase):
    def test_group(self):
        parallel = Group.parallel(*[
            Group.parallel(*[
                Sleep.random(.01),
                Sleep.random(.01),
                Sleep.random(.01),
            ])
            for _ in range(50)
        ])

        g1 = Group.sequential(
            parallel,
            parallel
        )

        g2 = Group.sequential(
            g1,
            g1,
            g1
        )

        pipe = g2

        with timing('pipeline'):
            pipe(None)
