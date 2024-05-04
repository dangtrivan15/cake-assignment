from cake_airflow_custom_package.api import StateMachine
from psycopg2 import connect
from urllib.parse import quote_plus
import json
from typing import Optional


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
                "   state_obj jsonb"
                ")"
            )
        self.conn.commit()

    def health_check(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1")

    def update_state(self, state: dict):
        with self.conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO _cake_state_machine (connection_id, state_obj) "
                f"VALUES (%(connection_id)s, %(state_obj)s::jsonb) "
                f"ON CONFLICT (connection_id) DO UPDATE SET state_obj = EXCLUDED.state_obj",
                vars={
                    'connection_id': self.connection_id,
                    'state_obj': json.dumps(state)
                }
            )
        self.conn.commit()

    def get_latest_state(self) -> Optional[dict]:
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT state_obj FROM _cake_state_machine WHERE connection_id = %(connection_id)s",
                vars={
                    'connection_id': self.connection_id,
                }
            )
            return cur.fetchone()


def _make_db_uri(user, password, host, port, database, protocol):
    return f'{protocol}://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}'
