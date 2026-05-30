from datetime import datetime
from lib.pg import PgConnect


class StgRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def order_events_insert(self,
                            object_id: int,
                            object_type: str,
                            sent_dttm: datetime,
                            payload: str
                            ) -> None:

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO stg.order_events(
                        object_id,
                        object_type,
                        sent_dttm,
                        payload
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (object_id) DO UPDATE SET
                        object_type = excluded.object_type,
                        sent_dttm = excluded.sent_dttm,
                        payload = excluded.payload
                    """,
                    (object_id, object_type, sent_dttm, payload)
                )
