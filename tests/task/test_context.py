from pathlib import Path
from unittest import TestCase

from easy_stream.task_lib import Context


class TestContext(TestCase):
    def test_child(self):
        ctx = Context(Path.cwd())

        child = ctx.child('x/y/z')

        self.assertEqual(ctx.root, child.root)
        self.assertEqual(ctx.logger, child.logger)
        self.assertEqual(child.output, ctx.output / 'x/y/z')

    def test_root_context(self):
        ctx = Context(Path.cwd())
        root = ctx.child('x/y/z').root_context()

        self.assertEqual(ctx.root, root.root)
        self.assertEqual(ctx.logger, root.logger)
        self.assertEqual(root.output, ctx.output)

    def test_sibling(self):
        ctx = Context(Path.cwd())
        ctx = ctx.child('x/y/z')

        sibling = ctx.sibling('a')

        self.assertEqual(ctx.root, sibling.root)
        self.assertEqual(ctx.output.parent, sibling.output.parent)
        self.assertEqual(sibling.output, ctx.output.parent / 'a')

    def test_name(self):
        ctx = Context(Path.cwd()).child('x/y/z')
        self.assertEqual(ctx.name, 'z')

    def test_step_id(self):
        ctx = Context(Path.cwd()).child('x/y/z')
        self.assertEqual(ctx.step_id, 'x/y/z')

    def test_prepare(self):
        ctx = Context.temporary().child('x/y/z')
        self.assertFalse(Path(ctx.output).exists())
        ctx.prepare()
        self.assertTrue(Path(ctx.output).exists())

    def test_clean(self):
        ctx = Context.temporary().child('x/y/z').prepare()
        self.assertTrue(Path(ctx.output).exists())
        ctx.clean()
        self.assertFalse(Path(ctx.output).exists())
        self.assertTrue(Path(ctx.root).exists())


