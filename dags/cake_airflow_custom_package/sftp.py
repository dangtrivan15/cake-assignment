from cake_airflow_custom_package.api import DataPoint, Source, Destination
from airflow.providers.sftp.hooks.sftp import SFTPHook
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional
from airflow.models import Connection
from os.path import basename


class File:
    def __init__(self, path: Path, last_modified_time: datetime):
        self.path: Path = path
        self.last_modified_time: datetime = last_modified_time


class FileContent(DataPoint):
    def __init__(self, data: Iterable[bytes], cursor: datetime, file_name: str):
        self._data = data
        self._cursor = cursor
        self._file_name = file_name

    @property
    def file_name(self):
        return self._file_name

    @property
    def data(self) -> Iterable[bytes]:
        return self._data

    @property
    def cursor(self) -> datetime:
        return self._cursor


class SFTPSource(Source):
    def __init__(
            self,
            connection_id: str,
            dir_path: Path,
    ):
        self._connection_id = connection_id
        self._hook = SFTPHook(ftp_conn_id=self._connection_id)
        self.dir_path = dir_path

    @property
    def connection_id(self):
        return self._connection_id

    def health_check(self):
        is_ok, _ = self._hook.test_connection()
        if not is_ok:
            raise Exception(f"Connection id {self._connection_id} tested fail")

    def list_new_files(self, start_from: Optional[datetime]) -> list[File]:
        result = []
        for file_name in self._hook.list_directory(self.dir_path.name):
            file_path = str((self.dir_path / file_name).absolute())
            print(file_path)
            modified_time_str = self._hook.get_mod_time(file_path)
            modified_time = datetime.strptime(modified_time_str, "%Y%m%d%H%M%S")
            # This is considered risky as it imposes implicit dependency on provider's time string format implementation
            if start_from is None or modified_time > start_from:
                result.append(
                    File(
                        path=Path(file_path),
                        last_modified_time=modified_time
                    )
                )
        return result

    def read_file(self, file: File) -> Iterable[bytes]:
        with self._hook.conn.open(file.path.name, "rb") as f:
            while True:
                data = f.read(1024)
                if not data:
                    break
                yield data

    def tear_down(self):
        self._hook.close_conn()

    def has_new_data(self, from_cursor):
        return len(self.list_new_files(start_from=from_cursor))

    def read(self, from_cursor) -> Iterable[DataPoint]:
        for file in self.list_new_files(start_from=from_cursor):
            yield FileContent(
                data=self.read_file(file),
                cursor=file.last_modified_time,
                file_name=basename(file.path)
            )


class SFTPDest(Destination):
    def __init__(
            self,
            connection_id: str,
            dir_path: Path,
    ):
        self._connection_id = connection_id
        self._hook = SFTPHook(ftp_conn_id=self._connection_id)
        self.dir_path = dir_path

    def health_check(self):
        is_ok, _ = self._hook.test_connection()
        if not is_ok:
            raise Exception(f"Connection id {self._connection_id} tested fail")

    def write(self, data: FileContent):
        # TODO: handle write failure, even for updated file case
        with self._hook.conn.open((self.dir_path / data.file_name).name, "wb") as f:
            for chunk in data.data:
                f.write(chunk)


