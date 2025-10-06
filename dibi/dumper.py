import psycopg2


def dump_db(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cursor:
        cmd = """
          select schemaname,
                 tablename
          from pg_catalog.pg_tables
          where schemaname not in ('information_schema', 'pg_catalog')
          order by schemaname, tablename"""
        cursor.execute(cmd)
        return_values = []
        for schema_name, table_name in cursor.fetchall():
            cmd = f"""
              select count(*)
              from {schema_name}.{table_name}"""
            cursor.execute(cmd)
            results = cursor.fetchone()
            if results:
                print(f"{table_name} -> {results[0]}")
                return_values.append((table_name, results[0]))
    return return_values
