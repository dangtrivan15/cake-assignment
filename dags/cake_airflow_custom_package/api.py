from typing import Iterable, Any
from datetime import datetime


class DataPoint:
    data: Any
    cursor: datetime


class Source:
    def health_check(self):
        raise NotImplementedError

    def read(self, from_cursor: datetime) -> Iterable[DataPoint]:
        """
        :param from_cursor: a timestamp to filter out older data
        :return: an iterable of DataPoint to be piped, this must be sorted in each DataPoint's cursor key
        """
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


class StateMachine:
    def health_check(self):
        raise NotImplementedError

    def update_state(self, value: datetime):
        raise NotImplementedError

    def get_latest_cursor(self) -> datetime:
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

    def sync(self):
        for data_point in self.source.read(
                self.state_machine.get_latest_cursor()
        ):
            for transformed_data_point in self.transformer.transform(data_point):
                self.destination.write(transformed_data_point)
                self.state_machine.update_state(transformed_data_point.cursor)

