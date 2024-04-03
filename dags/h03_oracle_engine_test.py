# 오라클 커넥션 없어도 오라클에 붙는지 확인 할 수 있는 에어플로우 데그
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from pendulum.tz.timezone import Timezone
from sqlalchemy import create_engine

#############
kst = Timezone("Asia/Seoul")
date_now = datetime.now(tz=kst)
today = date_now.strftime('%Y-%m-%d')

start_date = date_now - timedelta(days=1)


#############

@dag(
    dag_id='h03_oracle_engine_test',
    schedule=None,
    start_date=start_date,
    description='Oracle Connection Test',
    catchup=False,
    tags=["test"],
)
def test_main():
    @task(task_id='testtest')
    def print_oracle(**context):
        # oracledb.init_oracle_client()

        # oracle_engine = create_engine('oracle://test:test@localhost:1521/orcl')
        oracle_engine = create_engine('oracle://sumin:1234@localhost:1521/xe')
        conn = oracle_engine.connect()
        try:

            df = pd.read_sql("select * from sumin_tb", conn)
            # oracle에서 유저 sumin이 sumin_tb 생성한거로 연결한 거임!!!

            print("df =========> ", df)

        finally:
            conn.close()
            oracle_engine.dispose()

    print_oracle = print_oracle()
    print_oracle


test_main()

"""
에러 주의
    Traceback (most recent call last):
      File "/home/lotte/.local/lib/python3.10/site-packages/airflow/decorators/base.py", line 220, in execute
        return_value = super().execute(context)
      File "/home/lotte/.local/lib/python3.10/site-packages/airflow/operators/python.py", line 181, in execute
        return_value = self.execute_callable()
      File "/home/lotte/.local/lib/python3.10/site-packages/airflow/operators/python.py", line 198, in execute_callable
        return self.python_callable(*self.op_args, **self.op_kwargs)
      File "/home/lotte/airflow/dags/oracle_test.py", line 38, in print_oracle
        df = pd.read_sql("select * from test_tb", conn)
      File "/home/lotte/.local/lib/python3.10/site-packages/pandas/io/sql.py", line 706, in read_sql
        return pandas_sql.read_query(
      File "/home/lotte/.local/lib/python3.10/site-packages/pandas/io/sql.py", line 2736, in read_query
        cursor = self.execute(sql, params)
      File "/home/lotte/.local/lib/python3.10/site-packages/pandas/io/sql.py", line 2670, in execute
        cur = self.con.cursor()
    AttributeError: 'Connection' object has no attribute 'cursor'

    pandas 버전이 높아지면서 sqlalchemy랑 충돌 날 수 있음.
    pandas==2.0.3 / sqlalchemy=1.4.50
"""