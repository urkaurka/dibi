from typing import Union
import logging
from pathlib import Path
import csv

import psycopg2  # type: ignore
from jinja2 import Template

logger = logging.getLogger(__name__)
csv.register_dialect('pipes', delimiter='|')


def get_connection(db: dict = None):
    return psycopg2.connect(
        host=db['HOST'],
        port=db['PORT'],
        database=db['NAME'],
        user=db['USER'],
        password=db['PASSWORD']
    )


class QueryWrapper:
    def __init__(self, cursor):
        self.cursor = cursor

    def scalar_query(self, query: str) -> Union[int, str]:
        self.cursor.execute(query)
        res = self.cursor.fetchone()
        if not res:
            return None
        return res[0]

    def query(self, query: str):
        self.cursor.execute(query)
        return self.cursor.fetchall()


def drop_any_tables_except(conn, schema_in: str, tables_to_save: list[str] = []) -> None:
    with conn.cursor() as cursor:
        qw = QueryWrapper(cursor)
        sel_query = """
          select table_schema,
                 table_name
          from information_schema.tables
          where table_type = 'BASE TABLE'
            and table_schema not in ('pg_catalog', 'information_schema')"""
        for schema, table_name in qw.query(sel_query):
            if schema == schema_in and table_name not in tables_to_save:
                drop_query = f"drop table if exists {schema}.{table_name} cascade"
                logger.info(drop_query)
                cursor.execute(drop_query)
                conn.commit()


def empty_any_tables_except(conn, schema_in: str, tables_to_save: list[str] = []) -> None:
    with conn.cursor() as cursor:
        qw = QueryWrapper(cursor)
        sel_query = """
          select table_schema,
                 table_name
          from information_schema.tables
          where table_type = 'BASE TABLE'
            and table_schema not in ('pg_catalog', 'information_schema')"""
        for schema, table_name in qw.query(sel_query):
            if schema == schema_in and table_name not in tables_to_save:
                drop_query = f"delete from {schema}.{table_name};"
                logger.info(drop_query)
                cursor.execute(drop_query)
                conn.commit()


def execute_template_script(conn, path: Path, context: dict, fetch=False):
    with conn.cursor() as cursor:
        logger.info(f'start script {path.name}')
        template = Template(path.open().read())
        cursor.execute(template.render(**context))
        conn.commit()
        logger.info(f'end script {path.name}')
        if fetch:
            return cursor.fetchall()


def execute_script(conn, path: Path, schema='en', fetch=False):
    logger.info(f'start script {path.name}')
    with conn.cursor() as cursor:
        real_script = path.open().read()
        if schema:
            logger.info(f'start script {path.name}')
            real_script = f"set search_path to {schema};\n" + real_script
        cursor.execute(real_script)
        conn.commit()
        logger.info(f'end script {path.name}')
        if fetch:
            return cursor.fetchall()


def create_schema(conn, schema: str) -> None:
    with conn.cursor() as cursor:
        cmd = f"create schema if not exists {schema};"
        cursor.execute(cmd)
    conn.commit()


def sql_to_file(so, headers: tuple[str], rows) -> int:
    writer = csv.writer(so, dialect="pipes")
    writer.writerow(headers)
    nr_rows = len(rows)
    writer.writerows(rows)
    return nr_rows
