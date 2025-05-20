from datetime import datetime


def type_from_val(val):
    if isinstance(val, int):
        return 'integer'
    elif isinstance(val, float):
        return 'float'
    elif isinstance(val, datetime):
        return 'TIMESTAMPTZ'
    return 'varchar'


def create_query_from_plain_dict(table_name: str, data: dict):
    body_lines = [
        f"{key} {type_from_val(val)},"
        for key, val in data.items()
    ]
    body = '\n'.join(body_lines)[:-1]

    return f"""
      create table {table_name} (
        id serial,
        {body}
      )"""


def insert_query_from_plain_dict(table_name: str, data: dict):
    field_list = list(data.keys())
    return f"""
      insert into {table_name} ({', '.join(field_list)})
      values ({', '.join(["%s" for _ in range(len(field_list))])})"""
