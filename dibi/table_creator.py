def type_from_val(val):
    if val is int:
        return 'integer'
    elif val is float:
        return 'float'
    return 'varchar'


def create_query_from_plain_dict(table_name: str, data: dict):
    body_lines = [
        f"{key} {type_from_val(val)},"
        for key, val in data.items()
    ]
    body = '\n'.join(body_lines)[:-1]

    return f"""
      create table {table_name} (
        {body}
      )"""


def insert_query_from_plain_dict(table_name: str, data: dict):
    field_list = data.keys()
    return f"""
      insert into {table_name} ({', '.join(field_list)})
      values ({', '.join(["%s" for _ in range(field_list)])}"""
