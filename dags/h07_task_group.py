from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.utils.task_group import TaskGroup
from pendulum.tz.timezone import Timezone

kst = Timezone("Asia/Seoul")

date_now = datetime.now(tz=kst)
start_date = date_now - timedelta(days=1)

@dag(
    dag_id='h07_task_group',
    schedule=None,
    start_date=start_date,
    description='test',
    catchup=False,
    tags=['test'],
)
def full_main():
    with TaskGroup(group_id='group_01') as group_01:
        with TaskGroup(group_id='inner_01') as inner_01:
            drop_tbl = TrinoOperator(
                task_id='drop_tbl',
                trino_conn_id='trino_test',
                sql=f"""
                    DROP TABLE IF EXISTS iceberg.dm.test_tb
                """
            )
            create_tbl= TrinoOperator(
                task_id='create_tbl',
                trino_conn_id='trino_test',
                sql=f"""
                    CREATE TABLE IF NOT EXISTS iceberg.dm.test_tb (id int, name varchar(10))
                """
            )
            insert_tbl = TrinoOperator(
                task_id='insert_tbl',
                trino_conn_id='trino_test',
                sql=f"""
                    INSERT INTO
                        iceberg.dm.test_tb
                    VALUES
                        (1, 'park'), (2, 'kim'), (3, 'lee')
                """
            )
            drop_tbl >> create_tbl >> insert_tbl

        with TaskGroup(group_id='inner_02') as inner_02:
            drop_tbl = TrinoOperator(
                task_id='drop_tbl',
                trino_conn_id='trino_test',
                sql=f"""
                     DROP TABLE IF EXISTS iceberg.dm.test_tb
                 """
            )
            create_tbl = TrinoOperator(
                task_id='create_tbl',
                trino_conn_id='trino_test',
                sql=f"""
                     CREATE TABLE IF NOT EXISTS iceberg.dm.test_tb (id int, name varchar(10))
                 """
            )
            insert_tbl = TrinoOperator(
                task_id='insert_tbl',
                trino_conn_id='trino_test',
                sql=f"""
                     INSERT INTO
                         iceberg.dm.test_tb
                     VALUES
                         (1, 'park'), (2, 'kim'), (3, 'lee')
                 """
            )
            drop_tbl >> create_tbl >> insert_tbl

        inner_01 >> inner_02

    group_01

full_main()