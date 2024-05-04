from cake_airflow_custom_package.api import DataPoint, Source, Destination, Transformer
from airflow.providers.sftp.hooks.sftp import SFTPHook
from datetime import datetime
from typing import Iterable, Optional
from os.path import basename, join


class File:
    def __init__(self, path: str, last_modified_time: datetime):
        self.path: str = path
        self.last_modified_time: datetime = last_modified_time


class FileContent(DataPoint):
    def __init__(self, data: Iterable[str], cursor: datetime, file_name: str):
        self.data = data
        self.cursor = cursor
        self.file_name = file_name


class SFTPSource(Source):
    def __init__(
            self,
            connection_id: str,
            dir_path: str,
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
        for file_name in self._hook.list_directory(self.dir_path):
            file_path = join(self.dir_path, file_name)
            modified_time_str = self._hook.get_mod_time(file_path)
            modified_time = datetime.strptime(modified_time_str, "%Y%m%d%H%M%S")
            # This is considered risky as it imposes implicit dependency on provider's time string format implementation
            print(f"{file_name} - {modified_time} - {start_from}")
            if start_from is None or modified_time > start_from:
                result.append(
                    File(
                        path=file_path,
                        last_modified_time=modified_time
                    )
                )
        return result

    def read_file(self, file: File) -> Iterable[str]:
        with self._hook.get_conn().open(file.path, "r") as f:
            while True:
                data = f.read(1024)
                if not data:
                    break
                yield data

    def tear_down(self):
        self._hook.close_conn()

    def has_new_data(self, from_cursor: Optional[datetime]):
        return len(self.list_new_files(start_from=from_cursor))

    def read(self, from_cursor: Optional[datetime]) -> Iterable[DataPoint]:
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
            dir_path: str,
    ):
        self._connection_id = connection_id
        self._hook = SFTPHook(ftp_conn_id=self._connection_id)
        self.dir_path = dir_path

    def health_check(self):
        is_ok, _ = self._hook.test_connection()
        if not is_ok:
            raise Exception(f"Connection id {self._connection_id} tested fail")

    def write(self, data: FileContent):
        dest_file_path = join(self.dir_path, data.file_name)
        previously_exists = self._hook.path_exists(dest_file_path)
        bak_file_path = f"{dest_file_path}.bak"
        if previously_exists:
            self._hook.get_conn().rename(dest_file_path, bak_file_path)
        try:
            with self._hook.get_conn().open(dest_file_path, "w") as f:
                for chunk in data.data:
                    f.write(chunk)
        except:
            self._hook.delete_file(dest_file_path)
            if previously_exists:
                self._hook.get_conn().rename(bak_file_path, dest_file_path)
            raise
        else:
            self._hook.delete_file(bak_file_path)


class ReplacingWithATransformer(Transformer):
    def transform(self, data_point: FileContent) -> Iterable[FileContent]:
        def replace_str_with_a(s: str):
            return "a" * len(s)
        data_point.data = map(replace_str_with_a, data_point.data)
        yield data_point
