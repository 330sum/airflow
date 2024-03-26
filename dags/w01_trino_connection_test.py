from airflow.hooks.base import BaseHook

from sqlalchemy import create_engine
from trino.dbapi import connect
from trino.auth import BasicAuthentication


def get_trino_connector(source_catalog, source_schema):
    trino_conn = BaseHook.get_connection(conn_id="trino_test")

    trino_conn_info = {
        "host": trino_conn.host,
        "port": trino_conn.port,
        "user": trino_conn.login,
        "password": trino_conn.password,
    }

    conn = connect(
        host=trino_conn_info["host"],
        port=trino_conn_info["port"],
        user=trino_conn_info["user"],
        catalog=f'{source_catalog}',
        schema=f'{source_schema}',
        auth=BasicAuthentication(trino_conn_info["user"], trino_conn_info["password"]),
        http_scheme="https",
    )
    cur = conn.cursor()

    trino_engine = f'trino://{trino_conn.login}@{trino_conn.host}:{trino_conn.port}/{source_catalog}/{source_schema}'
    engine = create_engine(trino_engine,
                           connect_args={
                               "auth": BasicAuthentication(f"{trino_conn.login}", f"{trino_conn.password}"),
                               "http_scheme": "https",
                           })

    return conn, cur, engine
