from typing import TypeVar, List, Iterable, Set, Callable, Union

from easy_stream.stream_lib.stream_op import Operators, Mapping, StreamOp, FlatMapping

T = TypeVar('T')
U = TypeVar('U')
V = TypeVar('V')

StreamProvider = Callable[[], Iterable[T]]


class Stream(Iterable[T]):
    operators = Operators()

    def __init__(self, stream: StreamProvider):
        self._stream = stream

    def __iter__(self):
        for item in self._stream():
            yield item

    def map(self, op: Union[Mapping, StreamOp[T, U]]) -> 'Stream[U]':
        return self.__class__(lambda: self.operators.map(op)(self))

    def flatmap(self, op: FlatMapping) -> 'Stream[U]':
        return self.__class__(lambda: self.operators.flatmap(op)(self))

    def filter(self, selector: Callable[[T], bool]) -> 'Stream[T]':
        return self.__class__(lambda: self.operators.filter(selector)(self))

    def shuffle(self, ) -> 'Stream[T]':
        return self.__class__(lambda: self.operators.shuffle()(self))

    def slice(self, start: int, stop: int, step: int) -> 'Stream[T]':
        return self.__class__(lambda: self.operators.slice(start, stop, step)(self))

    # terminal operations
    def to_list(self) -> List[T]:
        return [_ for _ in iter(self)]

    def to_set(self) -> Set[T]:
        return {_ for _ in iter(self)}
