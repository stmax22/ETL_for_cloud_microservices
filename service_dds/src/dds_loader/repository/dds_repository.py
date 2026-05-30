from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_tables(self, table_name: str, data: dict) -> None:
        columns = list(data.keys())
        values = list(data.values())

        placeholders = ', '.join(['%s'] * len(values))
        columns_sql = ', '.join(columns)

        query = f"""
            INSERT INTO dds.{table_name} ({columns_sql})
            VALUES ({placeholders})
            ON CONFLICT ({columns[0]}) DO NOTHING;
        """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, values)

    def insert_satellite_tables(self, table_name: str, data: dict) -> None:
        columns = list(data.keys())
        values = list(data.values())

        placeholders = ', '.join(['%s'] * len(values))
        columns_sql = ', '.join(columns)

        # Создаем список обновляемых колонок и их значений.
        set_clause = ', '.join([
            f"{col} = EXCLUDED.{col}"
            for col in columns
            if col != columns[0] and col != columns[-3]  # Исключаем колонку ключа и времени.
        ])

        query = f"""
            INSERT INTO dds.{table_name} ({columns_sql})
            VALUES ({placeholders})
            ON CONFLICT ({columns[0]}, {columns[-3]}) DO UPDATE SET {set_clause};
        """

        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, values)

    def get_user_product_stats(self, user_uuid: str) -> list:
        query = """
            SELECT
                p.h_product_pk as product_id,
                pn.name as product_name,
                COUNT(DISTINCT o.h_order_pk) as order_cnt
            FROM dds.l_order_user ou
            JOIN dds.h_order o ON ou.h_order_pk = o.h_order_pk
            JOIN dds.l_order_product op ON o.h_order_pk = op.h_order_pk
            JOIN dds.h_product p ON op.h_product_pk = p.h_product_pk
            JOIN dds.s_product_names pn ON p.h_product_pk = pn.h_product_pk
            WHERE ou.h_user_pk = %s
            GROUP BY p.h_product_pk, pn.name
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (user_uuid,))
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]

    def get_user_category_stats(self, user_uuid: str) -> list:
        query = """
            SELECT
                c.h_category_pk as category_id,
                c.category_name,
                COUNT(DISTINCT o.h_order_pk) as order_cnt
            FROM dds.l_order_user ou
            JOIN dds.h_order o ON ou.h_order_pk = o.h_order_pk
            JOIN dds.l_order_product op ON o.h_order_pk = op.h_order_pk
            JOIN dds.l_product_category pc ON op.h_product_pk = pc.h_product_pk
            JOIN dds.h_category c ON pc.h_category_pk = c.h_category_pk
            WHERE ou.h_user_pk = %s
            GROUP BY c.h_category_pk, c.category_name
        """
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (user_uuid,))
                columns = [desc[0] for desc in cur.description]
                return [dict(zip(columns, row)) for row in cur.fetchall()]
