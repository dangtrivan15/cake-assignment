from cake_airflow_custom_package.api import StateMachine
from psycopg2 import connect
from urllib.parse import quote_plus
import json
from typing import Optional
from datetime import datetime


class PostgresStateMachine(StateMachine):
    def __init__(self, connection_id, uri):
        self.connection_id = connection_id
        self.conn = connect(uri)
        self.init_schema()

    def init_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS _cake_state_machine ("
                "   connection_id VARCHAR PRIMARY KEY,"
                "   cursor timestamp"
                ")"
            )
        self.conn.commit()

    def health_check(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1")

    def update_state(self, value: datetime):
        with self.conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO _cake_state_machine (connection_id, cursor) "
                f"VALUES (%(connection_id)s, %(cursor)s) "
                f"ON CONFLICT (connection_id) DO UPDATE SET cursor = EXCLUDED.cursor",
                vars={
                    'connection_id': self.connection_id,
                    'cursor': value
                }
            )
        self.conn.commit()

    def get_latest_cursor(self) -> Optional[datetime]:
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT cursor FROM _cake_state_machine WHERE connection_id = %(connection_id)s",
                vars={
                    'connection_id': self.connection_id,
                }
            )
            result = cur.fetchone()
            if result is not None:
                result = result[0]
            return result


def _make_db_uri(user, password, host, port, database, protocol):
    return f'{protocol}://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}'
