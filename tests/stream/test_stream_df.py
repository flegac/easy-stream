from unittest.case import TestCase

import pandas as pd

from easy_stream.stream_lib.df_stream import DFStream


class TestDfStream(TestCase):
    def setUp(self) -> None:
        n = 10
        self.stream = DFStream(lambda: pd.DataFrame({
            'x': list(range(n)),
            'y': list(range(n, 2 * n))
        }))

    def test_map(self):
        def operator(row):
            return {
                'diff': row['x'] - row['y'],
                'sum': row['x'] + row['y']
            }

        print(self.stream.map(operator).to_df())

    def test_flatmap(self):
        def operator(row):
            for i in range(3):
                yield {
                    'x': row['x'],
                    'x+ay': row['x'] + row['y'] * i
                }

        print(self.stream.flatmap(operator).to_df())

    def test_shuffle(self):
        print(self.stream.shuffle().to_df())

    def test_filter(self):
        stream_filter = self.stream.filter(lambda r: r['x'] % 2 == 0)
        print(stream_filter.to_df())

    def test_slice(self):
        print(self.stream.slice(2, 7, 3).to_df())

    def test_column(self):
        x = self.stream.column('x', int).to_list()
        y = self.stream.column('y', int).to_list()
        print('x', x)
        print('y', y)
