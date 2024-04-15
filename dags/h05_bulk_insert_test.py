from airflow.decorators import dag, task
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from airflow.hooks.base import BaseHook

from datetime import datetime, timedelta
from pendulum.tz.timezone import Timezone

import sqlalchemy as sa
from sqlalchemy import create_engine
import pandas as pd
from trino.auth import BasicAuthentication
from trino.dbapi import connect

kst = Timezone("Asia/Seoul")

date_now = datetime.now(tz=kst)
start_date = date_now - timedelta(days=1)

table_catalog = 'ICEBERG'
table_schema = 'DW_TEST'
table_name = 'TEST_TB'


@dag(
    dag_id='h05_bulk_insert_test',
    schedule=None,
    start_date=start_date,
    description='oracle to trino : bulk insert test - 초기적재',
    catchup=False,
    tags=['test'],
    params={
        'rownum': 1000000,
        'chunk_size': 5000,
        'sql': False
    }
)
def full_main():
    # 1. select src data
    select_src_data_task = OracleOperator(
        task_id='select_src_data_task',
        oracle_conn_id='oracle_test',
        sql=f"""
                SELECT 
                    id
                    , name
                    , TO_CHAR(SYSDATE, 'YYYYMMDDHH24MISS') AS if_ldg_dtm 
                FROM
                    sumin.sumin_tb
                WHERE rownum <= {{{{ params.rownum }}}}
        """
    )

    # 2. insert into target table
    @task(task_id='insert_into_target')
    def insert_into_target(**context):
        task_instance = context['task_instance']
        datas = task_instance.xcom_pull(task_ids='select_src_data_task', key='return_value')
        cols = ['id', 'name', 'if_ldg_dtm']

        trino_conn_info = BaseHook.get_connection(conn_id='trino_test')

        sql_tf = context["params"]["sql"]
        print('sql_tf ===============>', sql_tf)

        if sql_tf:
            try:
                trino_conn = connect(host=trino_conn_info.host,
                                     port=trino_conn_info.port,
                                     user=trino_conn_info.login,
                                     catalog='iceberg',
                                     schema='dw_test',
                                     auth=BasicAuthentication(trino_conn_info.login, trino_conn_info.password),
                                     http_scheme='https'
                                     )
                cur = trino_conn.cursor()

                bulk_data = [tuple(x) for x in datas]
                print('bulk_data==========>', bulk_data)
                bulk_q = str(bulk_data).lstrip('[').rstrip(']').replace('None', 'NULL')
                print('bulk_q==========>', bulk_q)
                sql = f""" INSERT INTO test_tb VALUES {bulk_q} """
                print('sql==========>', sql)

                cur.execute(sql)

            finally:
                cur.close()
                trino_conn.close()

        else:
            trino_engine_url = f"trino://{trino_conn_info.login}@{trino_conn_info.host}:{trino_conn_info.port}/{table_catalog}/{table_schema}"

            try:
                trino_engine = create_engine(trino_engine_url, connect_args={
                    "auth": BasicAuthentication(trino_conn_info.login, trino_conn_info.password),
                    #"http_scheme": "https"
                })
                chunk_size = context["params"]["chunk_size"]
                print('chunk_size==========>', chunk_size)

                conn = trino_engine.connect()
                df = pd.DataFrame(datas, columns=cols)
                print('df==========>', df)
                df.to_sql(
                    name='test_tb',
                    con=conn,
                    if_exists='append',
                    index=False,
                    chunksize=chunk_size,
                    method='multi'
                )
            finally:
                conn.close()
                trino_engine.dispose()

    select_src_data_task >> insert_into_target()


full_main()
