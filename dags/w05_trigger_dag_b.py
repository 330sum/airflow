from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from pendulum.tz.timezone import Timezone

kst = Timezone("Asia/Seoul")  # 한국
date_now = datetime.now(tz=kst)  # 한국 시간
today = date_now.strftime('%Y-%m-%d')  # 한국 날짜
start_date = date_now - timedelta(days=1)


@dag(
    dag_id='trigger_dag_b',
    description='트리거 데그b 입니다',
    start_date=start_date,
    schedule=None,
    schedule_interval='',
    catchup=False,
    tags=['trigger'],

)
def main():
    @task
    def task_b1(**context):
        from pprint import pprint
        print('==============================context=============================')
        pprint(context)
        print('==================================================================')
        print('dag_run================================>', context['dag_run'])
        print('dag_run - conf - a1 =======================>', context['dag_run'].conf['a1'])
        print('dag_run - conf - b1 =======================>', context['dag_run'].conf['b1'])
        print("테스크 b1")

    @task
    def task_b2():
        print("테스크 b2")

    @task
    def task_b3():
        print("테스크 b3")

    store_ids = ['store_id_001', 'store_id_002']
    with TaskGroup(group_id='my_task_group') as tasssk_group:
        prev_task = None
        for store_id in store_ids:
            @task(task_id=f'tttask____{store_id}')
            def why_test(store_id, **context):
                tt = context['dag_run'].conf['a1']

                print('tt==============>', tt)
                print('store_id============>', store_id)

                return tt, store_id

            task_tt = why_test(store_id)
            if prev_task:
                prev_task >> task_tt
                prev_task = task_tt

    task_b1() >> task_b2() >> task_b3() >> tasssk_group


main()
