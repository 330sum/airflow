from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.providers.trino.operators.trino import TrinoOperator
from pendulum.tz.timezone import Timezone
from sqlalchemy import create_engine
from trino.auth import BasicAuthentication

kst = Timezone("Asia/Seoul")

date_now = datetime.now(tz=kst)
start_date = date_now - timedelta(days=1)


@dag(
    dag_id="h01_trino_dag",
    schedule=None,
    start_date=start_date,
    description='에어플로우 - 트리노 연습 중',
    catchup=False,
    tags=["test"],
)
def main():
    @task
    def conn_test():
        trino_conn = BaseHook.get_connection(conn_id="trino_test")
        trino_engine = f'trino://{trino_conn.login}@{trino_conn.host}:{trino_conn.port}/postgresql/test_sm'
        engine = create_engine(trino_engine,
                               connect_args={
                                   "auth": BasicAuthentication(f"{trino_conn.login}", f"{trino_conn.password}"),
                                   "http_scheme": "https",
                               })
        print('trino_conn ===============> ', trino_conn)
        print('trino_engine ============>', trino_engine)
        print('engine=====================>', engine)
        print('trino_conn.login=====================>', trino_conn.login)
        print('trino_conn.host=====================>', trino_conn.host)
        print('trino_conn.port=====================>', trino_conn.port)
        print('trino_conn.login=====================>', trino_conn.login)
        print('trino_conn.password=====================>', trino_conn.password)

    @task
    def test(**context):
        print('context =======================> ', context)
        print('task_instance =======================> ', context['task_instance'])

    get_some = TrinoOperator(
        task_id="get_some",
        trino_conn_id="trino_test",
        sql="select count(1) from postgresql.test_sm.test_tb_1"
    )

    conn_test() >> test() >> get_some


main()
