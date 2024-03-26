from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from pendulum.tz.timezone import Timezone
from sqlalchemy import create_engine
from trino.auth import BasicAuthentication
from trino.dbapi import connect

kst = Timezone("Asia/Seoul")

source_catalog = 'mariadb'
source_schema = 'ms01'
source_table = 'mt01'

target_catalog = 'iceberg'
target_schema = 'is01'
target_table = 'it01'

partition_column = 'id'

date_now = datetime.now(tz=kst)
start_date = date_now - timedelta(days=1)


@dag(
    dag_id="get_trino",
    schedule=None,
    start_date=start_date,
    description='트리노 연결 연습 중',
    catchup=False,
    tags=["trino"],
    params={
        "source_catalog": source_catalog,
        "source_schema": source_schema,
        "partition_column": partition_column,
    }
)
@task
def get_trino_connector(source_catalog, source_schema):
    trino_conn = BaseHook.get_connection(conn_id="trinotrino")

    trino_conn_info = {
        "host": trino_conn.host,
        "port": trino_conn.port,
        "user": trino_conn.login,
        "password": trino_conn.password,
    }

    print("trino_conn.host=====================>", trino_conn.host)
    print("trino_conn.port=====================>", trino_conn.port)
    print("trino_conn.login=====================>", trino_conn.login)
    print("trino_conn.password=====================>", trino_conn.password)

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

    print("conn=================> ", cur)
    print("cur=================> ", cur)

    trino_engine = f'trino://{trino_conn.login}@{trino_conn.host}:{trino_conn.port}/{source_catalog}/{source_schema}'
    engine = create_engine(trino_engine,
                           connect_args={
                               "auth": BasicAuthentication(f"{trino_conn.login}", f"{trino_conn.password}"),
                               "http_scheme": "https",
                           })

    print("engine=================> ", engine)
    # return conn, cur, engine


trino_task = get_trino_connector('mariadb', 'ms01')

trino_task
