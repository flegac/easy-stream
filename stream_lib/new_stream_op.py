from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Generic, TypeVar, Callable, Iterable, Type, Tuple

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')

Mapping = Callable[[T], U]
FlatMapping = Callable[[T], Iterable[U]]


class Ops(Enum):
    Map = auto()
    Flatmap = auto()
    Filter = auto()
    Slice = auto()
    Shuffle = auto()


class Op(Generic[T, U], ABC):
    def __init__(self, cls: Type[T]) -> 'Op[T,T]':
        self.ops = []

    def map(self, op: Callable[[U], V]) -> 'Op[T,V]':
        self.ops.append((Ops.Map, (op,)))
        return self

    def flatmap(self, op: Callable[[U], Iterable[V]]) -> 'Op[T,V]':
        self.ops.append((Ops.Map, (op,)))
        return self

    def filter(self, selector: Callable[[U], bool]) -> 'Op[T,U]':
        self.ops.append((Ops.Filter, (selector,)))
        return self

    def slice(self, start: int, stop: int, step: int) -> 'Op[T,U]':
        self.ops.append((Ops.Slice, (start, stop, step)))
        return self

    def shuffle(self) -> 'Op[T,U]':
        self.ops.append((Ops.Shuffle, ()))
        return self


class OpGroup(Generic[T], ABC):
    @abstractmethod
    def apply(self, op: Op, args: Tuple, data: T) -> T:
        ...


class StreamOpGroup(OpGroup):

    def apply2(self, full_op: Op, data: T) -> T:
        for op, args in full_op.ops:
            data = self.apply(op, args, data)
        return data

    def apply(self, op: Ops, args: Tuple, data: T) -> T:
        mapping = {
            Ops.Map: self.map,
            Ops.Flatmap: self.flatmap,
            Ops.Filter: self.filter,
            Ops.Slice: self.slice,
            Ops.Shuffle: self.shuffle,
        }
        return mapping[op](args, data)

    def map(self, args: Tuple, data: T) -> T:
        op: Mapping = args[0]
        return list(map(op, data))

    def flatmap(self, args: Tuple, data: T) -> T:
        op: FlatMapping = args[0]
        raise NotImplementedError

    def filter(self, args: Tuple, data: T) -> T:
        selector = args[0]
        return list(filter(selector, data))

    def slice(self, args: Tuple, data: T) -> T:
        start, stop, step = args
        return data[start:stop:step]

    def shuffle(self, args: Tuple, data: T) -> T:
        from random import shuffle
        shuffle(data)
        return data


if __name__ == '__main__':
    u = Op(str)
    v = Op(int)
    xx = (Op(int)
          .filter(lambda x: x % 2 == 0)
          .map(str)
          # .flatmap(lambda x: ['a', 'b'])
          .slice(0, 10, 2)
          .shuffle())

    y = [1, 2, 3, 4]

    group = StreamOpGroup()

    y = group.apply2(xx, y)
    print(y)
