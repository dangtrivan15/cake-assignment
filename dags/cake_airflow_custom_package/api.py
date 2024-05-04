from typing import Iterable


class DataPoint:
    @property
    def data(self):
        raise NotImplementedError

    @property
    def cursor(self):
        raise NotImplementedError


class Source:
    def health_check(self):
        raise NotImplementedError

    def has_new_data(self, from_cursor):
        raise NotImplementedError

    def read(self, from_cursor) -> Iterable[DataPoint]:
        raise NotImplementedError


class Destination:
    def health_check(self):
        raise NotImplementedError

    def write(self, data: DataPoint):
        raise NotImplementedError


class StateMachine:
    def health_check(self):
        raise NotImplementedError

    def update_state(self, state: dict):
        raise NotImplementedError

    def get_latest_state(self) -> dict:
        raise NotImplementedError


class Pipeline:
    def __init__(
            self,
            source: Source,
            destination: Destination,
            state_machine: StateMachine
    ):
        self.source = source
        self.destination = destination
        self.state_machine = state_machine

    def health_check(self):
        self.source.health_check()
        self.destination.health_check()
        self.state_machine.health_check()

    def has_new_data(self) -> bool:
        latest_state = self.state_machine.get_latest_state()
        if latest_state is not None:
            cursor = latest_state['cursor']
        else:
            cursor = None
        return self.source.has_new_data(cursor)

    def sync(self):
        latest_state = self.state_machine.get_latest_state()
        if latest_state is not None:
            cursor = latest_state['cursor']
        else:
            cursor = None
        for data_point in self.source.read(cursor):
            self.destination.write(data_point)
            self.state_machine.update_state(
                {"cursor": data_point.cursor}
            )

