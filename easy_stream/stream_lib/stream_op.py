import itertools
import random
from abc import ABC, abstractmethod
from typing import Generic, Iterable, Callable, Tuple, Type, TypeVar

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')

Mapping = Callable[[T], U]
FlatMapping = Callable[[T], Iterable[U]]


class StreamOp(Generic[T, U], ABC):
    operators: 'Operators' = None

    @abstractmethod
    def __call__(self, stream: Iterable[T]) -> Iterable[U]:
        ...

    def map(self, op: Callable[[U], V]) -> 'StreamOp[T,V]':
        return self.operators.chain(self, self.operators.map(op))

    def flatmap(self, op: Callable[[U], Iterable[V]]) -> 'StreamOp[T,V]':
        return self.operators.chain(self, self.operators.flatmap(op))

    def filter(self, selector: Callable[[U], bool]) -> 'StreamOp[T,U]':
        return self.operators.chain(self, self.operators.filter(selector))

    def slice(self, start: int, stop: int, step: int) -> 'StreamOp[T,U]':
        return self.operators.chain(self, self.operators.slice(start, stop, step))

    def shuffle(self) -> 'StreamOp[T,U]':
        return self.operators.chain(self, self.operators.shuffle())


class Identity(StreamOp[T, T]):
    def __init__(self, cls: Type[T] = object):
        self.cls = cls

    def __call__(self, stream: Iterable[T]):
        return stream


class Map(StreamOp[T, U]):
    def __init__(self, op: Mapping):
        self.op = op

    def __call__(self, stream: Iterable[T]) -> Iterable[U]:
        for item in stream:
            yield self.op(item)


class Flatmap(StreamOp[T, U]):
    def __init__(self, op: FlatMapping):
        self.op = op

    def __call__(self, stream: Iterable[T]) -> Iterable[U]:
        for item in stream:
            for _ in self.op(item):
                yield _


class Shuffle(StreamOp[T, T]):

    def __call__(self, stream: Iterable[T]) -> Iterable[T]:
        data = list(stream)
        random.shuffle(data)
        for item in data:
            yield item


class Enumerate(StreamOp[T, Tuple[int, T]]):
    def __call__(self, stream: Iterable[T]) -> Iterable[Tuple[int, T]]:
        for idx, item in enumerate(stream):
            yield idx, item


class Filter(StreamOp[T, T]):
    def __init__(self, selector: Callable[[T], bool]):
        self.selector = selector

    def __call__(self, stream: Iterable[T]) -> Iterable[T]:
        for item in stream:
            if self.selector(item):
                yield item


class Slice(StreamOp[T, T]):
    def __init__(self, start: int, stop: int, step: int = 1):
        self.start = start
        self.stop = stop
        self.step = step

    def __call__(self, stream: Iterable[T]) -> Iterable[T]:
        for item in itertools.islice(stream, self.start, self.stop, self.step):
            yield item


class Chain(StreamOp[T, V]):
    def __init__(self, op1: StreamOp[T, U], op2: StreamOp[U, V]):
        self.op1 = op1
        self.op2 = op2

    def __call__(self, stream: Iterable[T]) -> Iterable[V]:
        return self.op2(self.op1(stream))


class Operators(ABC):
    def __init__(
            self,
            map_: Type[Map] = Map,
            flatmap: Type[Flatmap] = Flatmap,
            filter_: Type[Filter] = Filter,
            slice_: Type[Slice] = Slice,
            shuffle: Type[Shuffle] = Shuffle,
            chain: Type[Chain] = Chain
    ):
        self.map = map_
        self.flatmap = flatmap
        self.filter = filter_
        self.slice = slice_
        self.shuffle = shuffle
        self.chain = chain


StreamOp.operators = Operators()
