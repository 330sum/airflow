from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.trino.operators.trino import TrinoOperator
from pendulum.tz.timezone import Timezone

kst = Timezone("Asia/Seoul")

date_now = datetime.now(tz=kst)
start_date = date_now - timedelta(days=1)


@dag(
    dag_id="h02_trino_conn_test",
    schedule=None,
    start_date=start_date,
    description='트리노 테스트',
    catchup=False,
    tags=["test"],
)
def test_main():
    trino_test = TrinoOperator(
        task_id='trino_test',
        trino_conn_id='trino_test',
        sql=f"""
                SELECT count(1) FROM postgresql.test_sm.test_tb_1
        """
    )

    @task(task_id='print_select')
    def print_select(**context):
        task_instance = context['task_instance']
        datas = task_instance.xcom_pull(task_ids='trino_test')

        print('context =======================> ', context)
        print('task_instance =======================> ', task_instance)
        print('datas =============> ', datas)

    trino_test >> print_select()


test_main()
