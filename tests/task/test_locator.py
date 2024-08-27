from unittest import TestCase

from easy_stream.task_lib import Context
from easy_stream.task_lib import Loc
from easy_stream.task_lib import Step


def my_task(ctx: Context):
    with ctx.path_to(f'{ctx.name}.txt').open('w') as _:
        _.write('toto')


class TestLocator(TestCase):
    def setUp(self):
        pipe = Step('toto').sequential(
            Step('sub0').sequential(
                Step('sub1', my_task),
                Step('sub2', my_task),
                Step('sub3', my_task),
            )
        ).clean()

        self.ctx = Context.temporary()
        pipe(self.ctx)

    def tearDown(self):
        self.ctx.clean()

    def test_single(self):
        locator = Loc.step('toto/sub0/sub3/*')
        actual = locator.single(self.ctx).name
        expected = 'sub3.txt'
        self.assertEqual(expected, actual)

    def test_iter_posix(self):
        ctx = self.ctx
        locator = Loc.posix(self.ctx.output / 'toto/**')
        actual = set(map(lambda x: x.name, locator.iter(ctx)))
        expected = {'toto', 'sub3', 'sub2', 'sub1', 'sub0'}
        self.assertEqual(expected, actual)

    def test_iter_step(self):
        ctx = self.ctx
        locator = Loc.step('toto/sub0/*', 'toto/sub0/sub2/*', 'toto/*')
        actual = set(map(lambda x: x.name, locator.iter(ctx)))
        expected = {'sub2', 'sub0', 'sub1', 'sub3', 'sub2.txt'}
        self.assertEqual(expected, actual)

    def test_iter_sibling(self):
        ctx = self.ctx.child('toto')
        locator = Loc.sibling('sub0/*', 'sub0/sub2/*', '*')
        actual = set(map(lambda x: x.name, locator.iter(ctx)))
        expected = {'sub2', 'sub0', 'sub1', 'sub3', 'sub2.txt'}
        self.assertEqual(expected, actual)

    def test_iter_with_dest(self):
        ctx = self.ctx.child('toto')
        locator = Loc.sibling('sub0/sub2/*')
        src = locator.iter(ctx)
        dst = Loc.with_dest(src, ctx.output)
        actual = set(map(lambda x: f'{x.parent.name}/{x.name}', dst))

        expected = {'toto/sub2.txt'}
        self.assertEqual(expected, actual)

    def test_iter_with_suffix(self):
        ctx = self.ctx.child('toto')
        locator = Loc.sibling('sub0/sub2/*')
        src = locator.iter(ctx)
        dst = Loc.with_suffix(src, '.xx')
        actual = set(map(lambda x: f'{x.parent.name}/{x.name}', dst))

        expected = {'sub2/sub2.xx'}
        self.assertEqual(expected, actual)

    def test_limit(self):
        locator = Loc.step('toto/sub0/sub1/*', 'toto/**').limit(4)
        actual = set(map(lambda x: x.name, locator.iter(self.ctx)))
        expected = {'sub0', 'sub1.txt', 'sub1', 'toto'}
        self.assertEqual(expected, actual)
