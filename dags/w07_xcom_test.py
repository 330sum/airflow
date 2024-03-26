from datetime import datetime, timedelta
from pendulum.tz import Timezone

kst = Timezone('Asia/Seoul')
date_now = datetime.now(tz=kst)
today = date_now.strftime('%Y-%m-%d')
start_date = date_now - timedelta(days=1)


@dag(
    dag_id='xcom_test',
    start_date=None,
    schedule=None,
    tags=['xcom']
)
def xcom_func():
    @task(task_id='xcom_t1')
    def xcom_t1():
        value = {"첫번째": "첫번째값이다 임마", "두번째": "두번째값이다 임마", "세번째": "세번째값이다 임마"}
        return value

    @task(task_id='xcom_check')
    def xcom_check(**context):
        task_instance = context['task_instance']
        print('task_instance====================================>', task_instance)
        xcom_dict = task_instance.xcom_pull(task_ids='xcom_t1', key='return_value')

        print('xcom_rslt ============>', xcom_dict)
        print(xcom_dict.get('두번째'))

    xcom_t1 = xcom_t1()
    xcom_check = xcom_check()

    xcom_t1 >> xcom_check


xcom_func()
