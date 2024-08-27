import itertools
from typing import Callable, Type, Any

import pandas as pd

from easy_stream.stream_lib.stream import Stream
from easy_stream.stream_lib.stream_op import Map, Flatmap, Filter, Identity, Operators, T


class DfMap(Map):
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        data = df._stream().apply(self.op, axis='columns')
        return pd.DataFrame(data.to_list())


class DfFlatmap(Flatmap):
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        serie = df._stream().apply(self.op, axis='columns')
        data = serie.to_list()
        data = list(itertools.chain(*data))
        return pd.DataFrame(data)


class DfFilter(Filter):
    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        df2 = df._stream()
        return df2.loc[self.selector(df2)].reset_index(drop=True)


class DfIdentity(Identity[pd.DataFrame]):
    operators = Operators(
        DfMap,
        DfFlatmap,
        DfFilter
    )


DFProvider = Callable[[], pd.DataFrame]


class DFStream(Stream[T]):
    operators = DfIdentity.operators

    def __init__(self, df: DFProvider):
        super().__init__(df)

    def __iter__(self):
        df = self._stream()
        for idx, row in df.iterrows():
            yield row

    def to_df(self):
        return self._stream()

    def column(self, name: str, cls: Type[T] = Any) -> Stream[T]:
        return Stream(lambda: self._stream()[name])

    def compute(self):
        df = self._stream()

        def provider():
            return df

        self._stream = provider
        return self
