from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
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

source_catalog = 'ICEBERG'
source_schema = 'DW_SM'
source_table = 'TEST_TB'

# target_catalog = ''
target_schema = 'SUMIN'
target_table = 'SUMIN_TB'


@dag(
    dag_id='h06_oracle_to_trino',
    schedule=None,
    start_date=start_date,
    description='trino to oracle',
    catchup=False,
    tags=["test"],
)
def full_main():
    # 1. select from trino
    select_src_data_task = TrinoOperator(
        task_id='select_src_data_task',
        trino_conn_id='trino_test',
        sql=f""" 
                SELECT 
                    * 
                FROM {source_catalog}.{source_schema}.{source_table}
        """
    )

    # 2. get oracle columns
    select_src_col_task = OracleOperator(
        task_id='select_src_col_task',
        oracle_conn_id='oracle_test',
        sql=f"""
                SELECT column_name
                FROM all_tab_columns
                WHERE 1=1
                    AND owner = '{target_schema}'
                    AND table_name = '{target_table}'
                ORDER BY column_id ASC
        """
    )

    # 3. make query
    @task(task_id='make_query')
    def make_query(**context):
        task_instance = context['task_instance']
        datas = task_instance.xcom_pull(task_ids='select_src_data_task', key='return value')
        print('datas =======> ', datas)  # datas = [['1', 'park'], ['2', 'kim'], ['3', 'lee']]

        col_data = task_instance.xcom_pull(task_ids='select_src_col_task', key='return value')
        print('col_data =======> ', col_data)  # col_data = [['id'], ['name']]

        cols = list(map(''.join, col_data))
        print('cols =======> ', cols)  # cols = ['id', 'name']

        sql_statements = []
        for data in datas:
            values = ', '.join(f"'{val}'" for val in data)
            sql_statement = f"SELECT {values} FROM DUAL"
            sql_statements.append(sql_statement)

        combined_selects = ' UNION ALL\n'.join(sql_statements)

        sql = f"INSERT INTO {target_schema}.{target_table} ({', '.join(cols)})\n{combined_selects}".format(
            target_schema=f'{target_schema}',
            target_table=f'{target_table}',
        )

        return sql

        # sql_1 = 'INSERT INTO {target_schema}.{target_table} {cols}'.format(
        #     target_schema = f'{target_schema}',
        #     target_table = f'{target_table}',
        #     cols ='({})'.format(', '.join(cols)) if cols else '',
        # )
        # def convert_to_sql(datas):
        #     sql_statements = []
        #     for data in datas:
        #         values = ', '.join(f"'{val}'" for val in data)
        #         sql_statement = f" SELECT {values} FROM DUAL"
        #         sql_statements.append(sql_statement)
        #     return ' UNION ALL '.join(sql_statements)
        #
        # sql_2 = convert_to_sql(datas)
        # full_sql = sql_1 + sql_2

        # return full_sql

    # 4. insert to oracle
    insert_into_target_task = OracleOperator(
        task_id='insert_into_target_task',
        oracle_conn_id='oracle_test',
        autocommit=True,
        sql=f"""
            {{{{ task_instance.xcom_pull(task_ids='make_query') }}}}
        """
    )

    @task(task_id='get_xcom')
    def get_xcom(**context):
        task_instance = context['task_instance']
        values = task_instance.xcom_pull(task_ids='insert_into_target_task', key='return_value')
        print('values =========> ', values)

    select_src_data_task >> select_src_col_task >> make_query() >> insert_into_target_task >> get_xcom()


full_main()
