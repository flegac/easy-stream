from pathlib import Path
from unittest.case import TestCase

from easy_stream.stream_lib.stream import Stream


def int_stream(limit: int):
    def generator():
        for i in range(limit):
            yield i

    return generator


def str_to_int(path: Path):
    return len(str(path))


def iter_range(limit: int):
    return Stream(int_stream(limit))


class TestStream(TestCase):

    def test_map(self):
        stream = Stream(int_stream(5)).map(lambda x: f'toto_{x}')
        actual = stream.to_list()
        expected = ['toto_0', 'toto_1', 'toto_2', 'toto_3', 'toto_4']
        self.assertEqual(actual, expected)

    def test_flatmap(self):
        stream = Stream(int_stream(4)).flatmap(iter_range)
        actual = stream.to_list()
        expected = [0, 0, 1, 0, 1, 2]
        self.assertEqual(actual, expected)

    def test_shuffle(self):
        stream = Stream(int_stream(20)).shuffle()
        actual_set = stream.to_set()
        expected_set = set(range(20))
        actual_list = stream.to_list()
        expected_list = list(range(20))

        self.assertEqual(actual_set, expected_set)
        self.assertNotEqual(actual_list, expected_list)

    def test_filter(self):
        stream = Stream(int_stream(10)).filter(lambda x: x % 2 == 0)
        actual = stream.to_list()
        expected = [0, 2, 4, 6, 8]
        self.assertEqual(actual, expected)

    def test_slice(self):
        stream = Stream(int_stream(100)).slice(2, 20, 3)
        actual = stream.to_list()
        expected = [2, 5, 8, 11, 14, 17]
        self.assertEqual(actual, expected)


