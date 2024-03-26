from datetime import datetime, timedelta
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from airflow.decorators import dag, task
from airflow.providers.trino.operators.trino import TrinoOperator
from pendulum.tz.timezone import Timezone
from trino.auth import BasicAuthentication

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
    dag_id="trino_dag",
    schedule=None,
    start_date=start_date,
    description='에어플로우 - 트리노 연습 중',
    catchup=False,
    tags=["trino"],
    params={
        "source_table_name": source_table,
        "target_table_name": target_table,
        "partition_column": partition_column,
    }
)
def full_main():
    @task
    def conn_test():
        trino_conn = BaseHook.get_connection(conn_id="trinotrino")
        trino_engine = f'trino://{trino_conn.login}@{trino_conn.host}:{trino_conn.port}/{source_catalog}/{source_schema}'
        engine = create_engine(trino_engine,
                               connect_args={
                                   "auth": BasicAuthentication(f"{trino_conn.login}", f"{trino_conn.password}"),
                                   "http_scheme": "https",
                               })
        # print('trino_conn ===============> ', trino_conn)
        # print('trino_engine ============>', trino_engine)
        # print('engine=====================>', engine)

    @task
    def test(**context):
        print('context =======================> ', context)
        print('task_instance =======================> ', context['task_instance'])

    get_some = TrinoOperator(
        task_id="get_some",
        trino_conn_id="trinotrino",
        sql="select count(1) from mariadb.ms01.mt01"

    )

    # 1. Drop target table
    drop_target_table_task = TrinoOperator(
        task_id="drop_target_table_task",
        trino_conn_id="trinotrino",
        sql=f"""
                    DROP TABLE IF EXISTS {target_catalog}.{target_schema}.{{{{ params.target_table_name }}}}
            """
    )
    # 2. Create target table as select SOURCE table
    create_target_table_task = TrinoOperator(
        task_id="create_target_table_task",
        trino_conn_id="trinotrino",
        sql=f"""
                CREATE TABLE {target_catalog}.{target_schema}.{{{{ params.target_table_name }}}}
                WITH (
                    format = 'PARQUET',
                    partitioning = ARRAY['id'],
                    location = 's3a://bucket01/{target_catalog}/{target_schema}/{{{{ params.target_table_name }}}}'
                )
                AS SELECT *
                        , CAST(AT_TIMEZONE(NOW(), 'Asia/Seoul') AS TIMESTAMP(6)) AS ETL_DT  -- ETL 날짜
                  FROM {source_catalog}.{source_schema}.{{{{ params.source_table_name }}}}
        """
    )

    conn_test() >> test() >> get_some >> drop_target_table_task >> create_target_table_task


full_main()





# , SUBSTR(CAST({{{{ params.partition_column }}}} AS VARCHAR), LENGTH(CAST({{{{ params.partition_column }}}} AS VARCHAR))-1) AS STORE_ID  -- 상점 코드(3자리)
