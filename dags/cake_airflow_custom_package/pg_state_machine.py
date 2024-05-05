from cake_airflow_custom_package.api import StateMachine, State
from psycopg2 import connect
from typing import Optional


class PostgresStateMachine(StateMachine):
    def __init__(self, pipeline_id, uri):
        self.pipeline_id = pipeline_id
        self.conn = connect(uri)
        self.init_schema()

    def init_schema(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS _cake_state_machine ("
                "   pipeline_id VARCHAR PRIMARY KEY,"
                "   cursor timestamp,"
                "   id VARCHAR"
                ")"
            )
        self.conn.commit()

    def health_check(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1")

    def update_state(self, state: State):
        with self.conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO _cake_state_machine (pipeline_id, cursor, id) "
                f"VALUES (%(pipeline_id)s, %(cursor)s, %(id)s) "
                f"ON CONFLICT (pipeline_id) DO UPDATE SET cursor = EXCLUDED.cursor, id = EXCLUDED.id",
                vars={
                    'pipeline_id': self.pipeline_id,
                    'cursor': state.cursor,
                    'id': state.id,
                }
            )
        self.conn.commit()

    def get_latest_state(self) -> Optional[State]:
        with self.conn.cursor() as cur:
            cur.execute(
                f"SELECT cursor, id FROM _cake_state_machine WHERE pipeline_id = %(pipeline_id)s",
                vars={
                    'pipeline_id': self.pipeline_id,
                }
            )
            result = cur.fetchone()
            if result is not None:
                cursor, id = result
                result = State(cursor=cursor, id=id)
            return result

    def tear_down(self):
        self.conn.close()
