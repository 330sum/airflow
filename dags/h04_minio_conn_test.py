from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.trino.operators.trino import TrinoOperator
from pendulum.tz.timezone import Timezone

kst = Timezone("Asia/Seoul")

date_now = datetime.now(tz=kst)
start_date = date_now - timedelta(days=1)


@dag(
    dag_id="h04_minio_conn_test",
    schedule=None,
    start_date=start_date,
    description='minio 테스트',
    catchup=False,
    tags=["test"],
)
def test_main():
    select_test = TrinoOperator(
        task_id='select_test',
        trino_conn_id='trino_test',
        sql=f"""
                CREATE TABLE iceberg.dw.ice_tb
                WITH (format='parquet', location='s3a://hello/schema/dw/ice_tb')
                AS SELECT * FROM postgresql.test_sm.test_tb_1
        """
    )

    # WITH (format='parquet', location='s3a://hello-bucket/schema/dw/test_sm')

    select_test


test_main()