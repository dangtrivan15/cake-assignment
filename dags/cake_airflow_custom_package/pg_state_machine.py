from cake_airflow_custom_package.api import StateMachine
from psycopg2 import connect
from urllib.parse import quote_plus
import json
from typing import Optional
from datetime import datetime


class PostgresStateMachine(StateMachine):
    def __init__(self, id, uri):
        self.id = id
        self.conn = connect(uri)
        self.init_schema()

    def init_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS _cake_state_machine ("
                "   id VARCHAR PRIMARY KEY,"
                "   cursor timestamp"
                ")"
            )
        self.conn.commit()

    def health_check(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1")

    def update_cursor(self, value: datetime):
        with self.conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO _cake_state_machine (id, cursor) "
                f"VALUES (%(id)s, %(cursor)s) "
                f"ON CONFLICT (id) DO UPDATE SET cursor = EXCLUDED.cursor",
                vars={
                    'id': self.id,
                    'cursor': value
                }
            )
        self.conn.commit()

    def get_latest_cursor(self) -> Optional[datetime]:
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT cursor FROM _cake_state_machine WHERE id = %(id)s",
                vars={
                    'id': self.id,
                }
            )
            result = cur.fetchone()
            if result is not None:
                result = result[0]
            return result

    def tear_down(self):
        self.conn.close()


def _make_db_uri(user, password, host, port, database, protocol):
    return f'{protocol}://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}'
