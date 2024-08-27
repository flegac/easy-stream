from typing import TypeVar, Callable, Iterable, Generic, Type, Union

import pandas as pd

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')


class Op(Generic[T, U], Callable[[T], U]):
    @classmethod
    def identity(cls, t: Type[T]) -> 'Op[T, T]':
        return cls(lambda x: x)

    def __init__(self, op: Callable[[T], U]):
        self.op = op

    def map(self, op: Callable[[U], V]) -> 'Op[T,V]':
        def new_op(t: T):
            return op(self.op(t))

        return self.__class__(new_op)

    def __call__(self, data: T) -> U:
        return self.op(data)


class Stream(Iterable[T]):
    def __init__(self, stream: Iterable[T]):
        self.stream = stream

    def __iter__(self):
        return iter(self.stream)

    def map(self, op: Union[Op[T, U], Callable[[T], U]]) -> 'Stream[U]':
        return Stream(map(op, self.stream))

    def to_list(self):
        return list(self)


class DfStream(Iterable[tuple]):
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def __iter__(self):
        return iter(self.df)

    def map(self, op: Callable[[T], U]) -> 'DfStream[U]':
        data = self.df.apply(op, axis='columns')
        return DfStream(pd.DataFrame(data.data1()))

    def to_list(self):
        return [tuple(_) for _ in self.df.to_numpy()]


def pow2(x: int) -> int:
    return x ** 2


def mul3(x: int) -> int:
    return x * 3


def build_operator():
    identity = Op.identity(int)
    power = identity.map(pow2)
    literal = power.map(str)
    length = literal.map(len)
    return length


def stream_builder(base: Stream[int]):
    power = base.map(pow2)
    literal = power.map(str)
    length = literal.map(len)
    return length


if __name__ == '__main__':
    base: Stream[int] = Stream(range(10))
    big_op = build_operator()

    complex1 = stream_builder(base)
    complex2 = base.map(big_op).map(mul3)

    data1 = complex1.to_list()
    data2 = complex2.to_list()

    print(data1)
    print(data2)

    df = pd.DataFrame({
        'x': data1,
        'y': data2
    })
    types = tuple(df.dtypes)

    df_stream = DfStream(df)

    print(df_stream.to_list())
