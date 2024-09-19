class Table:
    def __init__(self,
                 conn,
                 table_name: str,
                 create_query: str,
                 insert_query: str):
        self.conn = conn
        self.cursor = conn.cursor()
        self.table_name = table_name
        self.create_query = create_query
        self.insert_query = insert_query

    def create(self):
        self.cursor.execute(self.create_query)
        self.conn.commit()
        return self

    def insertmany(self, records: list[tuple]):
        self.cursor.executemany(
            self.insert_query,
            records)
        self.conn.commit()
        return self

    def drop(self):
        self.cursor.execute(f'drop table if exists {self.table_name} cascade')
        self.conn.commit()

    def reset(self):
        self.drop()
        self.create()
        return self

    def exists(self) -> bool:
        schema, table = self.table_name.split('.')
        cmd = f"""
          select count(*)
          from pg_tables
          where schemaname = '{schema}'
            and tablename  = '{table}'"""
        self.cursor.execute(cmd)
        return 0 < self.cursor.fetchone()[0]

    def how_many_rows(self):
        self.cursor.execute(f"""
          select count(*) from {self.table_name}""")
        return self.cursor.fetchone()[0]

    def create_indexes(self):
        pass

    def max_id(self, label='id'):
        cmd = f"""
          select max({label})
          from {self.table_name}"""
        self.cursor.execute(cmd)
        ret = self.cursor.fetchone()[0]
        if not ret:
            return 0
        return ret
