import tempfile
from pathlib import Path
from unittest.case import TestCase

from easy_stream.task_lib import Loc


class TestLocatorSkip(TestCase):
    def setUp(self):
        self.expected_path = Path(tempfile.mkdtemp())
        self.expected_file = Path(tempfile.mktemp(suffix='.file'))
        self.expected_hidden_file = Path(tempfile.mktemp(prefix='.hidden_', suffix='.file'))

        with self.expected_file.open('w'), self.expected_hidden_file.open('w'):
            pass

        self.locator = Loc.posix(
            self.expected_path,
            self.expected_file,
            self.expected_hidden_file,
        )

    def test_no_skip(self):
        locator = self.locator
        actual = set(locator.iter(None))
        expected = {self.expected_path, self.expected_file}
        self.assertEqual(expected, actual)

    def test_skip_hidden_True(self):
        locator = self.locator.skip(hidden=True)
        actual = set(locator.iter(None))
        expected = {self.expected_path, self.expected_file}
        self.assertEqual(expected, actual)

    def test_skip_hidden_False(self):
        locator = self.locator.skip(hidden=False)
        actual = set(locator.iter(None))
        expected = {self.expected_path, self.expected_file, self.expected_hidden_file}
        self.assertEqual(expected, actual)

    def test_skip_files_True(self):
        locator = self.locator.skip(files=True)
        actual = set(locator.iter(None))
        expected = {self.expected_path}
        self.assertEqual(expected, actual)

    def test_skip_files_False(self):
        locator = self.locator.skip(files=False)
        actual = set(locator.iter(None))
        expected = {self.expected_path, self.expected_file}
        self.assertEqual(expected, actual)

    def test_skip_directories_True(self):
        locator = self.locator.skip(directories=True)
        actual = set(locator.iter(None))
        expected = {self.expected_file}
        self.assertEqual(expected, actual)

    def test_skip_directories_False(self):
        locator = self.locator.skip(directories=False)
        actual = set(locator.iter(None))
        expected = {self.expected_path, self.expected_file}
        self.assertEqual(expected, actual)
