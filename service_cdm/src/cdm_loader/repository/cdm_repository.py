from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_tables(self, table_name: str, data: dict) -> None:
        columns = list(data.keys())
        values = list(data.values())

        placeholders = ', '.join(['%s'] * len(values))
        columns_sql = ', '.join(columns)

        # Создаем список обновляемых колонок и их значений.
        set_clause = ', '.join([
            f'{col} = EXCLUDED.{col}'
            for col in columns
            if col != columns[0] and col != columns[1]  # Исключаем колонку ключа и времени.
        ])

        query = f"""
            INSERT INTO cdm.{table_name} ({columns_sql})
            VALUES ({placeholders})
            ON CONFLICT ({columns[0]}, {columns[1]}) DO UPDATE SET {set_clause};
        """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, values)
