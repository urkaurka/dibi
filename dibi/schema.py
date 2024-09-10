from dibi.img_table import Table


class Schema:
    def __init__(self, conn, name: str):
        self.tables = {
            # 'tokens': {'table': TokensImgTable, 'buffer': []
        }
        self.name = name
        self.conn = conn
        self.cursor = conn.cursor()

    def reset_schema(self):
        self.cursor.execute(f"drop schema if exists {self.name} cascade")
        self.cursor.execute(f"create schema {self.name}")
        self.reset_all_tables()
        self.conn.commit()

    def reset_all_tables(self):
        for _, tab in self.tables.items():
            tab['table'].reset()

    def insert_tables(self, step=0):
        for _, tab in self.tables.items():
            if len(tab['buffer']) > step:
                tab['table'].insertmany(tab['buffer'])
                tab['buffer'].clear()

    def table(self, name):
        return self.tables[name]['table']

    def buffer(self, name):
        return self.tables[name]['buffer']

    def dump_sizes(self):
        print(f"schema {self.name}.*")
        for _, tab in self.tables.items():
            print(f"{tab['table'].table_name} --> {tab['table'].how_many_rows()}")
        print()

    def create_indexes(self):
        for _, tab in self.tables.items():
            tab["table"].create_indexes()
        self.conn.commit()


def make_from_db(conn, schema_name: str) -> Schema:
    with conn.cursor() as cursor:
        cursor.execute("""select schemaname, tablename
                          from pg_catalog.pg_tables
                          where schemaname not in (
                            'information_schema', 'pg_catalog')
                          order by schemaname, tablename""")
        sche = Schema(conn, schema_name)
        sche.tables = {
            table_name: {
                'table': Table(conn=conn,
                               table_name=f"{schema_name}.{table_name}",
                               create_query="",
                               insert_query="")}
            for schema_name, table_name in cursor.fetchall()}
    return sche
