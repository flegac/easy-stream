from unittest.case import TestCase

from stream_lib.stream import Stream
from stream_lib.stream_op import Identity


def int_stream(limit: int):
    def generator():
        for i in range(limit):
            yield i

    return generator


def iter_range(limit: int):
    return Stream(int_stream(limit))


class TestStreamOperator(TestCase):
    def test_map(self):
        operator = Identity(int).map(lambda x: f'toto_{x}')

        s1 = Stream(int_stream(4))
        actual = list(operator(s1))

        expected = ['toto_0', 'toto_1', 'toto_2', 'toto_3']
        self.assertEqual(actual, expected)

    def test_flatmap(self):
        operator = Identity(int).flatmap(iter_range)
        s1 = Stream(int_stream(4))
        actual = list(operator(s1))

        expected = [0, 0, 1, 0, 1, 2]
        self.assertEqual(actual, expected)

    def test_shuffle(self):
        operator = Identity(int).shuffle()
        s1 = Stream(int_stream(4))

        actual_set = set(operator(s1))
        expected_set = set(range(4))
        actual_list = list(operator(s1))
        expected_list = list(range(4))

        self.assertEqual(actual_set, expected_set)
        self.assertNotEqual(actual_list, expected_list)

    def test_filter(self):
        operator = Identity(int).filter(lambda x: x % 2 == 1)
        s1 = Stream(int_stream(4))

        actual = list(operator(s1))
        expected = [1, 3]
        self.assertEqual(actual, expected)

    def test_slice(self):
        operator = Identity(int).slice(1, 20, 2)
        s1 = Stream(int_stream(4))

        actual = list(operator(s1))
        expected = [1, 3]
        self.assertEqual(actual, expected)
