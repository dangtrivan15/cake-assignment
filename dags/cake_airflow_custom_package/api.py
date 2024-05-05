from typing import Iterable, Any, Optional
from datetime import datetime


class State:
    id: str
    cursor: datetime

    def __init__(self, id: str, cursor: datetime):
        self.id = id
        self.cursor = cursor


class DataPoint:
    """
    This represents data in a "State-atomic" manner.
    This could be a container for a chunk of data (for batching) or an individual record itself, based on implementation
    """
    data: Any
    state: State


class Source:
    def health_check(self):
        raise NotImplementedError

    def read(self, from_state: State) -> Iterable[DataPoint]:
        """
        :param from_state: state to filter out already synced data
        :return: an iterable of DataPoint to be piped, this must be sorted in each DataPoint's by (cursor key, id)
        """
        raise NotImplementedError

    def tear_down(self):
        raise NotImplementedError


class Destination:
    def health_check(self):
        raise NotImplementedError

    def write(self, data: DataPoint):
        """
        :param data: the data to write, this is atomic in aspect of the source
        :return: None. However, this operation must be revertible for this data in case of Exception
        """
        raise NotImplementedError

    def tear_down(self):
        raise NotImplementedError


class StateMachine:
    def health_check(self):
        raise NotImplementedError

    def update_state(self, state: State):
        raise NotImplementedError

    def get_latest_state(self) -> Optional[State]:
        raise NotImplementedError

    def tear_down(self):
        raise NotImplementedError


class Transformer:
    def transform(self, data_point: DataPoint) -> Iterable[DataPoint]:
        raise NotImplementedError


class NoTransform(Transformer):
    def transform(self, data_point: DataPoint) -> Iterable[DataPoint]:
        yield data_point


class Pipeline:
    def __init__(
            self,
            source: Source,
            destination: Destination,
            state_machine: StateMachine,
            transformer: Transformer = None
    ):
        self.source = source
        self.destination = destination
        self.state_machine = state_machine
        if transformer is None:
            transformer = NoTransform()
        self.transformer = transformer

    def health_check(self):
        self.source.health_check()
        self.destination.health_check()
        self.state_machine.health_check()

    def tear_down(self):
        self.source.tear_down()
        self.destination.tear_down()
        self.state_machine.tear_down()

    def sync(self):
        state = self.state_machine.get_latest_state()
        try:
            for data_point in self.source.read(state):
                for transformed_data_point in self.transformer.transform(data_point):
                    self.destination.write(transformed_data_point)
                    self.state_machine.update_state(transformed_data_point.state)
        finally:
            self.tear_down()

