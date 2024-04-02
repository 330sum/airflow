from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.oracle.operators.oracle import OracleOperator
from pendulum.tz.timezone import Timezone

kst = Timezone("Asia/Seoul")

date_now = datetime.now(tz=kst)
start_date = date_now - timedelta(days=1)


@dag(
    dag_id="h3_oracle_conn_test",
    schedule=None,
    start_date=start_date,
    description='오라클 테스트',
    catchup=False,
    tags=["test"],
)
def test_main():
    oracle_test = OracleOperator(
        task_id='oracle_test',
        oracle_conn_id='oracle_test',
        sql="select user from dual"
    )

    @task(task_id='print_select')
    def print_select(**context):
        task_instance = context['task_instance']
        datas = task_instance.xcom_pull(task_ids='oracle_test')

        print('context =======================> ', context)
        print('task_instance =======================> ', task_instance)
        print('datas =============> ', datas)

    oracle_test >> print_select()


test_main()