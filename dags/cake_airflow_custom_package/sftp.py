from cake_airflow_custom_package.api import DataPoint, Source, Destination, Transformer, State
from airflow.providers.sftp.hooks.sftp import SFTPHook
from datetime import datetime
from typing import Iterable, Optional
from os.path import basename, join


class File:
    def __init__(self, path: str, last_modified_time: datetime):
        self.path = path
        self.last_modified_time = last_modified_time


class FileContent(DataPoint):
    def __init__(self, data: Iterable[bytes], file_path: str, state: State):
        self.data = data
        self.file_path = file_path
        self.state = state


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

    def list_new_files(self, from_time: datetime, from_name: str) -> list[File]:
        # The arguments here are vague and does not reflect the below implementation
        result: list[File] = []
        for file_name in self._hook.list_directory(self.dir_path):
            file_path = join(self.dir_path, file_name)
            modified_time_str = self._hook.get_mod_time(file_path)
            modified_time = datetime.strptime(modified_time_str, "%Y%m%d%H%M%S")
            # This is considered risky as it imposes implicit dependency on provider's time string format implementation
            file = File(path=file_path, last_modified_time=modified_time)
            if (from_time, from_name) < (file.last_modified_time, file.path):
                result.append(file)
        return result

    def read_file(self, file: File) -> Iterable[bytes]:
        with self._hook.get_conn().open(file.path, "rb") as f:
            while True:
                data = f.read(1024)
                if not data:
                    break
                yield data

    def tear_down(self):
        self._hook.close_conn()

    def read(self, from_state: Optional[State]) -> Iterable[DataPoint]:
        if from_state is None:
            from_time = datetime(1970, 1, 1, 0, 0, 0, 0)
            from_name = ''
        else:
            from_time, from_name = from_state.cursor, from_state.id

        for file in sorted(
                self.list_new_files(from_time=from_time, from_name=from_name),
                key=lambda f: (f.last_modified_time, f.path)
        ):
            yield FileContent(
                data=self.read_file(file),
                state=State(
                    cursor=file.last_modified_time,
                    id=file.path
                ),
                file_path=file.path,
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
        dest_file_path = join(self.dir_path, basename(data.file_path))
        previously_exists = self._hook.path_exists(dest_file_path)
        bak_file_path = f"{dest_file_path}.bak"
        if previously_exists:
            self._hook.get_conn().rename(dest_file_path, bak_file_path)
        try:
            with self._hook.get_conn().open(dest_file_path, "wb") as f:
                for chunk in data.data:
                    f.write(chunk)
        except:
            self._hook.delete_file(dest_file_path)
            if previously_exists:
                self._hook.get_conn().rename(bak_file_path, dest_file_path)
            raise
        else:
            if previously_exists:
                self._hook.delete_file(bak_file_path)

    def tear_down(self):
        self._hook.close_conn()


class ReplacingSpaceWithUnderScoreTransformer(Transformer):

    def transform(self, data_point: FileContent) -> Iterable[FileContent]:
        def replace_space_with_under_score(s: bytes):
            return s.replace(b" ", b"_")
        data_point.data = map(replace_space_with_under_score, data_point.data)
        yield data_point
