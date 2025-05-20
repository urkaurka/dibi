def dump_db(conn):
    with conn.cursor() as cursor:
        cmd = """
          select schemaname,
                 tablename
          from pg_catalog.pg_tables
          where schemaname not in ('information_schema', 'pg_catalog')
          order by schemaname, tablename"""
        cursor.execute(cmd)
        for schema_name, table_name in cursor.fetchall():
            cmd = f"""
              select count(*)
              from {schema_name}.{table_name}"""
            cursor.execute(cmd)
            print(f"{table_name} -> {cursor.fetchone()[0]}")
