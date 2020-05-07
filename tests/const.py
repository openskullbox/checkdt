CONNECT_ARGS = {
    "name": "connection_name",
    "type": "redshift",
    "host": "xxx.us-east-1.redshift.amazonaws.com",
    "username": "username",
    "password": "password",
    "database": "dbname",
    "port": 5439,
}

FILTER_ARGS = {
    "name": "max_id",
    "filter_type": "feedback",
    "operation": "max",
    "field": "id",
    "table": "public.table",
    "conn_id": None
}
FILTER_VAL = 6

SOURCE_ARGS = {
    "name": "table_name",
    "conn_id": None,
    "tablename": "public.table",
    "field_list": [
        {"fieldname": "id", "type": "integer"},
        {"fieldname": "code", "type": "string", "length": 20},
        {"fieldname": "type", "type": "string", "length": 3},
    ],
}
